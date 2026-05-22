package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import io.ktredis.storage.RedisObject

class CommandDispatcher(private val db: RedisDatabase) {

    fun dispatch(args: List<ByteArray>): RespValue {
        if (args.isEmpty()) return RespValue.error("ERR empty command")
        return when (keyOf(args[0]).uppercase()) {
            "PING"    -> ping(args)
            "ECHO"    -> echo(args)
            "SET"     -> set(args)
            "GET"     -> get(args)
            "DEL"     -> del(args)
            "EXISTS"  -> exists(args)
            "EXPIRE"  -> expire(args, 1000L)
            "PEXPIRE" -> expire(args, 1L)
            "TTL"     -> ttl(args, seconds = true)
            "PTTL"    -> ttl(args, seconds = false)
            "PERSIST" -> persist(args)
            "KEYS"     -> keys(args)
            "TYPE"     -> type(args)
            "DBSIZE"   -> dbsize(args)
            "FLUSHDB"  -> flushdb(args)
            "RENAME"   -> rename(args)
            "SCAN"     -> scan(args)
            "INCR"     -> incr(args, 1)
            "DECR"     -> incr(args, -1)
            "INCRBY"   -> incrByArg(args, 1)
            "DECRBY"   -> incrByArg(args, -1)
            "APPEND"   -> append(args)
            "STRLEN"   -> strlen(args)
            "GETRANGE" -> getrange(args)
            "SETRANGE" -> setrange(args)
            "MSET"     -> mset(args)
            "MGET"     -> mget(args)
            "RPUSH"  -> push(args, atHead = false, requireExists = false)
            "LPUSH"  -> push(args, atHead = true,  requireExists = false)
            "RPUSHX" -> push(args, atHead = false, requireExists = true)
            "LPUSHX" -> push(args, atHead = true,  requireExists = true)
            "LLEN"   -> llen(args)
            "LRANGE" -> lrange(args)
            "LINDEX" -> lindex(args)
            "LSET"   -> lset(args)
            "LPOP"   -> pop(args, fromHead = true)
            "RPOP"   -> pop(args, fromHead = false)
            else      -> RespValue.error("ERR unknown command '${keyOf(args[0])}'")
        }
    }

    private fun keyOf(a: ByteArray) = a.toString(Charsets.UTF_8)
    private fun syntaxError() = RespValue.error("ERR syntax error")
    private fun notInteger() = RespValue.error("ERR value is not an integer or out of range")

    private fun ping(args: List<ByteArray>) =
        if (args.size >= 2) RespValue.bulk(args[1]) else RespValue.PONG

    private fun echo(args: List<ByteArray>) =
        if (args.size == 2) RespValue.bulk(args[1])
        else RespValue.error("ERR wrong number of arguments for 'echo' command")

    private fun set(args: List<ByteArray>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments for 'set' command")
        val key = keyOf(args[1])
        val value = args[2]

        var ttlMillis: Long? = null
        var i = 3
        while (i < args.size) {
            when (keyOf(args[i]).uppercase()) {
                "EX" -> {
                    if (i + 1 >= args.size) return syntaxError()
                    ttlMillis = (keyOf(args[i + 1]).toLongOrNull() ?: return notInteger()) * 1000
                    i += 2
                }
                "PX" -> {
                    if (i + 1 >= args.size) return syntaxError()
                    ttlMillis = keyOf(args[i + 1]).toLongOrNull() ?: return notInteger()
                    i += 2
                }
                else -> return syntaxError()
            }
        }

        db.set(key, RedisObject.StringValue(value))
        if (ttlMillis != null) db.setExpire(key, ttlMillis)
        return RespValue.OK
    }

    private fun get(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments for 'get' command")
        val obj = db.get(keyOf(args[1])) ?: return RespValue.NIL
        if (obj !is RedisObject.StringValue)
            return RespValue.error("WRONGTYPE Operation against a key holding the wrong kind of value")
        return RespValue.bulk(obj.data)
    }

    private fun del(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments for 'del' command")
        var count = 0L
        for (i in 1 until args.size) if (db.delete(keyOf(args[i]))) count++
        return RespValue.int(count)
    }

    private fun exists(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments for 'exists' command")
        var count = 0L
        for (i in 1 until args.size) if (db.exists(keyOf(args[i]))) count++
        return RespValue.int(count)
    }

    private fun expire(args: List<ByteArray>, multiplierToMillis: Long): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val n = keyOf(args[2]).toLongOrNull() ?: return notInteger()
        return RespValue.int(if (db.setExpire(keyOf(args[1]), n * multiplierToMillis)) 1 else 0)
    }

    private fun ttl(args: List<ByteArray>, seconds: Boolean): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val ms = db.pttl(keyOf(args[1]))
        if (ms < 0) return RespValue.int(ms)               // -1 hoặc -2
        return RespValue.int(if (seconds) (ms + 500) / 1000 else ms)
    }

    private fun persist(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        return RespValue.int(if (db.persist(keyOf(args[1]))) 1 else 0)
    }

    // ---------- keyspace ----------
    private fun wrongType() =
        RespValue.error("WRONGTYPE Operation against a key holding the wrong kind of value")

    private fun keys(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments for 'keys' command")
        val pattern = keyOf(args[1])
        return RespValue.Array(db.keys().filter { globMatch(pattern, it) }.map { RespValue.bulk(it) })
    }

    private fun type(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments for 'type' command")
        val obj = db.get(keyOf(args[1])) ?: return RespValue.SimpleString("none")
        return RespValue.SimpleString(
            when (obj) {
                is RedisObject.StringValue -> "string"
                is RedisObject.ListValue   -> "list"
            }
        )
    }

    private fun dbsize(args: List<ByteArray>): RespValue =
        RespValue.int(db.keys().size.toLong())

    private fun flushdb(args: List<ByteArray>): RespValue {
        db.clear()
        return RespValue.OK
    }

    private fun rename(args: List<ByteArray>): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments for 'rename' command")
        return if (db.rename(keyOf(args[1]), keyOf(args[2]))) RespValue.OK
        else RespValue.error("ERR no such key")
    }

    private fun scan(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments for 'scan' command")
        var pattern = "*"
        var i = 2
        while (i < args.size) {
            when (keyOf(args[i]).uppercase()) {
                "MATCH" -> { if (i + 1 >= args.size) return syntaxError(); pattern = keyOf(args[i + 1]); i += 2 }
                "COUNT" -> { if (i + 1 >= args.size) return syntaxError(); i += 2 }   // bỏ qua (đơn giản hoá)
                else -> return syntaxError()
            }
        }
        val matched = db.keys().filter { globMatch(pattern, it) }
        return RespValue.Array(
            listOf(
                RespValue.bulk("0"),                                  // cursor luôn 0 — quét hết trong 1 lần
                RespValue.Array(matched.map { RespValue.bulk(it) })
            )
        )
    }

    // ---------- string / số ----------
    /** Trả (lỗi, data). Không tồn tại -> (null, null). Sai kiểu -> (wrongType, null). */
    private fun currentString(key: String): Pair<RespValue?, ByteArray?> {
        val obj = db.get(key) ?: return null to null
        if (obj !is RedisObject.StringValue) return wrongType() to null
        return null to obj.data
    }

    private fun incr(args: List<ByteArray>, by: Long): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        return doIncr(keyOf(args[1]), by)
    }

    private fun incrByArg(args: List<ByteArray>, sign: Long): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val delta = keyOf(args[2]).toLongOrNull() ?: return notInteger()
        return doIncr(keyOf(args[1]), sign * delta)
    }

    private fun doIncr(key: String, delta: Long): RespValue {
        val (err, data) = currentString(key)
        if (err != null) return err
        val current = if (data == null) 0L
            else data.toString(Charsets.UTF_8).toLongOrNull() ?: return notInteger()
        val next = current + delta
        db.setKeepTtl(key, RedisObject.StringValue(next.toString().toByteArray()))
        return RespValue.int(next)
    }

    private fun append(args: List<ByteArray>): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments for 'append' command")
        val key = keyOf(args[1])
        val (err, data) = currentString(key)
        if (err != null) return err
        val combined = (data ?: ByteArray(0)) + args[2]
        db.setKeepTtl(key, RedisObject.StringValue(combined))
        return RespValue.int(combined.size.toLong())
    }

    private fun strlen(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments for 'strlen' command")
        val (err, data) = currentString(keyOf(args[1]))
        if (err != null) return err
        return RespValue.int((data?.size ?: 0).toLong())
    }

    private fun getrange(args: List<ByteArray>): RespValue {
        if (args.size != 4) return RespValue.error("ERR wrong number of arguments for 'getrange' command")
        val (err, data) = currentString(keyOf(args[1]))
        if (err != null) return err
        val bytes = data ?: ByteArray(0)
        val len = bytes.size
        if (len == 0) return RespValue.bulk("")
        var start = keyOf(args[2]).toIntOrNull() ?: return notInteger()
        var end = keyOf(args[3]).toIntOrNull() ?: return notInteger()
        if (start < 0) start += len
        if (end < 0) end += len
        if (start < 0) start = 0
        if (end >= len) end = len - 1
        if (start > end) return RespValue.bulk("")
        return RespValue.bulk(bytes.copyOfRange(start, end + 1))
    }

    private fun setrange(args: List<ByteArray>): RespValue {
        if (args.size != 4) return RespValue.error("ERR wrong number of arguments for 'setrange' command")
        val key = keyOf(args[1])
        val offset = keyOf(args[2]).toIntOrNull() ?: return notInteger()
        if (offset < 0) return RespValue.error("ERR offset is out of range")
        val value = args[3]
        val (err, data) = currentString(key)
        if (err != null) return err
        val base = data ?: ByteArray(0)
        if (value.isEmpty()) return RespValue.int(base.size.toLong())
        val newLen = maxOf(base.size, offset + value.size)
        val result = ByteArray(newLen)                       // tự pad \x00
        System.arraycopy(base, 0, result, 0, base.size)
        System.arraycopy(value, 0, result, offset, value.size)
        db.setKeepTtl(key, RedisObject.StringValue(result))
        return RespValue.int(newLen.toLong())
    }

    private fun mset(args: List<ByteArray>): RespValue {
        if (args.size < 3 || args.size % 2 == 0)
            return RespValue.error("ERR wrong number of arguments for 'mset' command")
        var i = 1
        while (i + 1 < args.size) {
            db.set(keyOf(args[i]), RedisObject.StringValue(args[i + 1]))   // MSET xoá TTL như SET
            i += 2
        }
        return RespValue.OK
    }

    private fun mget(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments for 'mget' command")
        val items = (1 until args.size).map { idx ->
            val obj = db.get(keyOf(args[idx]))
            if (obj is RedisObject.StringValue) RespValue.bulk(obj.data) else RespValue.NIL
        }
        return RespValue.Array(items)
    }

    private fun listForRead(key: String): Pair<RespValue?, ArrayDeque<ByteArray>?> {
        val obj = db.get(key) ?: return null to null
        if (obj !is RedisObject.ListValue) return wrongType() to null
        return null to obj.items
    }

    private fun push(args: List<ByteArray>, atHead: Boolean, requireExists: Boolean): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val obj = db.get(key)
        if (obj != null && obj !is RedisObject.ListValue) return wrongType()
        if (obj == null && requireExists) return RespValue.int(0)        // *PUSHX khi key chưa có
        val list = if (obj is RedisObject.ListValue) obj.items
        else ArrayDeque<ByteArray>().also { db.setKeepTtl(key, RedisObject.ListValue(it)) }
        for (i in 2 until args.size) {
            if (atHead) list.addFirst(args[i]) else list.addLast(args[i])
        }
        return RespValue.int(list.size.toLong())
    }

    private fun llen(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, list) = listForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.int((list?.size ?: 0).toLong())
    }

    private fun lrange(args: List<ByteArray>): RespValue {
        if (args.size != 4) return RespValue.error("ERR wrong number of arguments")
        val (err, list) = listForRead(keyOf(args[1]))
        if (err != null) return err
        if (list == null) return RespValue.Array(emptyList())
        val len = list.size
        var start = keyOf(args[2]).toIntOrNull() ?: return notInteger()
        var stop  = keyOf(args[3]).toIntOrNull() ?: return notInteger()
        if (start < 0) start += len
        if (stop  < 0) stop  += len
        if (start < 0) start = 0
        if (stop >= len) stop = len - 1
        if (start > stop) return RespValue.Array(emptyList())
        return RespValue.Array((start..stop).map { RespValue.bulk(list[it]) })
    }

    private fun lindex(args: List<ByteArray>): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val (err, list) = listForRead(keyOf(args[1]))
        if (err != null) return err
        if (list == null) return RespValue.NIL
        var idx = keyOf(args[2]).toIntOrNull() ?: return notInteger()
        if (idx < 0) idx += list.size
        if (idx < 0 || idx >= list.size) return RespValue.NIL
        return RespValue.bulk(list[idx])
    }

    private fun lset(args: List<ByteArray>): RespValue {
        if (args.size != 4) return RespValue.error("ERR wrong number of arguments")
        val (err, list) = listForRead(keyOf(args[1]))
        if (err != null) return err
        if (list == null) return RespValue.error("ERR no such key")
        var idx = keyOf(args[2]).toIntOrNull() ?: return notInteger()
        if (idx < 0) idx += list.size
        if (idx < 0 || idx >= list.size) return RespValue.error("ERR index out of range")
        list[idx] = args[3]
        return RespValue.OK
    }

    private fun pop(args: List<ByteArray>, fromHead: Boolean): RespValue {
        if (args.size !in 2..3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val (err, list) = listForRead(key)
        if (err != null) return err
        val count = if (args.size == 3) (keyOf(args[2]).toIntOrNull() ?: return notInteger()) else null
        if (count != null && count < 0) return RespValue.error("ERR value is out of range, must be positive")
        if (list == null || list.isEmpty())
            return if (count == null) RespValue.NIL else RespValue.Array(emptyList())

        if (count == null) {
            val v = if (fromHead) list.removeFirst() else list.removeLast()
            if (list.isEmpty()) db.delete(key)        // Redis không giữ list rỗng
            return RespValue.bulk(v)
        }
        val n = minOf(count, list.size)
        val out = (1..n).map { RespValue.bulk(if (fromHead) list.removeFirst() else list.removeLast()) }
        if (list.isEmpty()) db.delete(key)
        return RespValue.Array(out)
    }
}