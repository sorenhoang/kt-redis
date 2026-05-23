package io.ktredis.persistence

import io.ktredis.storage.RedisObject
import io.ktredis.storage.RedisDatabase
import java.io.*

private object Op {            // opcode
    const val AUX = 0xFA
    const val RESIZEDB = 0xFB
    const val EXPIRE_MS = 0xFC
    const val EXPIRE_SEC = 0xFD
    const val SELECTDB = 0xFE
    const val EOF = 0xFF
}

private object Type {          // type byte
    const val STRING = 0
    const val LIST = 1         // legacy: len + N strings
    const val SET = 2          // len + N members
    const val HASH = 4         // len + N*(field,value)
    const val ZSET2 = 5        // len + N*(member, double 8 byte LE)
}

/** Writes RDB primitive types. Length encoding = big-endian; binary numbers (double/expire) = little-endian. */
class RdbWriter(private val out: OutputStream) {
    fun byte(b: Int) = out.write(b and 0xFF)

    fun writeLength(len: Long) {
        when {
            len < (1 shl 6)    -> byte(len.toInt() and 0x3F)                                  // 00xxxxxx
            len < (1 shl 14)   -> { byte(0x40 or (len.toInt() ushr 8)); byte(len.toInt()) }   // 01xxxxxx + 1 byte
            len <= 0xFFFFFFFFL -> { byte(0x80); writeBE(len, 4) }                             // 0x80 + 4 byte BE
            else               -> { byte(0x81); writeBE(len, 8) }                             // 0x81 + 8 byte BE
        }
    }

    private fun writeBE(v: Long, n: Int) { for (i in n - 1 downTo 0) byte((v ushr (i * 8)).toInt()) }

    /** 8-byte little-endian integer (used for expire timestamps in milliseconds). */
    fun writeLongLE(v: Long) { for (i in 0 until 8) byte((v ushr (i * 8)).toInt()) }

    fun writeString(b: ByteArray) { writeLength(b.size.toLong()); out.write(b) }      // plain, uncompressed
    fun writeString(s: String) = writeString(s.toByteArray(Charsets.UTF_8))

    fun writeDouble(d: Double) = writeLongLE(java.lang.Double.doubleToLongBits(d))     // 8 byte IEEE-754 LE
}

/** Reads RDB primitive types (symmetric with RdbWriter; supports Redis integer encoding). */
class RdbReader(private val ins: InputStream) {
    fun readByte(): Int { val b = ins.read(); if (b < 0) throw EOFException(); return b }

    /** Returns (value, isEncoded). isEncoded=true means this is a specially encoded value (int/LZF). */
    fun readLength(): Pair<Long, Boolean> {
        val b = readByte()
        return when ((b and 0xC0) ushr 6) {
            0 -> (b and 0x3F).toLong() to false
            1 -> (((b and 0x3F) shl 8) or readByte()).toLong() to false
            2 -> when (b) {
                0x80 -> readBE(4) to false
                0x81 -> readBE(8) to false
                else -> throw IOException("invalid length byte: $b")
            }
            else -> (b and 0x3F).toLong() to true          // 11xxxxxx -> special encoding
        }
    }

    private fun readBE(n: Int): Long { var v = 0L; repeat(n) { v = (v shl 8) or readByte().toLong() }; return v }
    private fun readLE(n: Int): Long { var v = 0L; for (i in 0 until n) v = v or (readByte().toLong() shl (i * 8)); return v }

    fun readLongLE(): Long = readLE(8)
    fun readDouble(): Double = java.lang.Double.longBitsToDouble(readLE(8))

    fun readString(): ByteArray {
        val (len, encoded) = readLength()
        if (encoded) return when (len.toInt()) {
            0 -> readByte().toByte().toString().toByteArray()                 // int8 -> decimal string
            1 -> readLE(2).toShort().toString().toByteArray()                 // int16 LE
            2 -> readLE(4).toInt().toString().toByteArray()                   // int32 LE
            else -> throw IOException("LZF-compressed string not supported")    // 3 = LZF (future)
        }
        val data = ByteArray(len.toInt())
        var off = 0
        while (off < data.size) {
            val r = ins.read(data, off, data.size - off)
            if (r < 0) throw EOFException()
            off += r
        }
        return data
    }

    fun readKey(): String = String(readString(), Charsets.UTF_8)
}

object Rdb {
    /** Snapshots the entire keyspace to dump.rdb. */
    fun save(db: RedisDatabase, file: File) {
        BufferedOutputStream(FileOutputStream(file)).use { os -> write(db, os) }
    }

    /** Serializes the keyspace to a byte array (used for PSYNC: master sends RDB over the socket). */
    fun dumpToBytes(db: RedisDatabase): ByteArray =
        ByteArrayOutputStream().also { write(db, it) }.toByteArray()

    private fun write(db: RedisDatabase, os: OutputStream) {
        val w = RdbWriter(os)
        os.write("REDIS0011".toByteArray(Charsets.US_ASCII))
        w.byte(Op.SELECTDB); w.writeLength(0)

        for ((key, v) in db.allEntries()) {
            db.expireAt(key)?.let { w.byte(Op.EXPIRE_MS); w.writeLongLE(it) }
            when (v) {
                is RedisObject.StringValue -> {
                    w.byte(Type.STRING); w.writeString(key); w.writeString(v.data)
                }
                is RedisObject.ListValue -> {
                    w.byte(Type.LIST); w.writeString(key)
                    w.writeLength(v.items.size.toLong()); v.items.forEach { w.writeString(it) }
                }
                is RedisObject.SetValue -> {
                    w.byte(Type.SET); w.writeString(key)
                    w.writeLength(v.members.size.toLong()); v.members.forEach { w.writeString(it) }
                }
                is RedisObject.HashValue -> {
                    w.byte(Type.HASH); w.writeString(key)
                    w.writeLength(v.fields.size.toLong())
                    v.fields.forEach { (f, x) -> w.writeString(f); w.writeString(x) }
                }
                is RedisObject.SortedSetValue -> {
                    w.byte(Type.ZSET2); w.writeString(key)
                    val all = v.ascending()
                    w.writeLength(all.size.toLong())
                    all.forEach { (score, member) -> w.writeString(member); w.writeDouble(score) }
                }
            }
        }
        w.byte(Op.EOF)
        w.writeLongLE(0)                 // CRC64 (check ignored on load -> writing 0 is fine)
    }

    /** Reads dump.rdb from a file -> list of (key, value, absolute-expireAt). expireAt is null if no TTL. */
    fun load(file: File): List<Triple<String, RedisObject, Long?>> {
        if (!file.exists() || file.length() == 0L) return emptyList()
        return BufferedInputStream(FileInputStream(file)).use { load(it) }
    }

    /** Reads RDB from any InputStream (used by replica to receive RDB over the socket). */
    fun load(input: InputStream): List<Triple<String, RedisObject, Long?>> {
        val r = RdbReader(input)
        readHeader(input)
        val result = ArrayList<Triple<String, RedisObject, Long?>>()
        var pendingExpire: Long? = null
        loop@ while (true) {
            when (val op = r.readByte()) {
                Op.EOF -> break@loop
                Op.SELECTDB -> r.readLength()
                Op.RESIZEDB -> { r.readLength(); r.readLength() }
                Op.AUX -> { r.readString(); r.readString() }
                Op.EXPIRE_MS -> pendingExpire = r.readLongLE()
                Op.EXPIRE_SEC -> {
                    var v = 0L; for (i in 0 until 4) v = v or (r.readByte().toLong() shl (i * 8))
                    pendingExpire = v * 1000
                }
                else -> {                                  // op is the type byte
                    val key = r.readKey()
                    val value = readValue(op, r)
                    result.add(Triple(key, value, pendingExpire)); pendingExpire = null
                }
            }
        }
        return result
    }

    private fun readHeader(ins: InputStream) {
        val header = ByteArray(9)
        var off = 0
        while (off < 9) {
            val n = ins.read(header, off, 9 - off)
            if (n < 0) throw EOFException("RDB file too short")
            off += n
        }
        require(String(header, Charsets.US_ASCII).startsWith("REDIS")) { "not a valid RDB file" }
    }

    private fun readValue(type: Int, r: RdbReader): RedisObject = when (type) {
        Type.STRING -> RedisObject.StringValue(r.readString())
        Type.LIST -> RedisObject.ListValue(ArrayDeque<ByteArray>().apply {
            repeat(r.readLength().first.toInt()) { add(r.readString()) }
        })
        Type.SET -> RedisObject.SetValue(LinkedHashSet<String>().apply {
            repeat(r.readLength().first.toInt()) { add(r.readKey()) }
        })
        Type.HASH -> RedisObject.HashValue(LinkedHashMap<String, ByteArray>().apply {
            repeat(r.readLength().first.toInt()) { put(r.readKey(), r.readString()) }
        })
        Type.ZSET2 -> RedisObject.SortedSetValue().apply {
            repeat(r.readLength().first.toInt()) { val m = r.readKey(); add(m, r.readDouble()) }
        }
        else -> throw IOException("type $type not supported (possibly listpack/intset/quicklist from real Redis)")
    }
}
