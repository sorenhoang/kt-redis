package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import io.ktredis.storage.RedisObject

class CommandDispatcher(private val db: RedisDatabase) {

    fun dispatch(args: List<ByteArray>): RespValue {
        if (args.isEmpty()) return RespValue.error("ERR empty command")
        return when (keyOf(args[0]).uppercase()) {
            "PING" -> ping(args)
            "ECHO" -> echo(args)
            "SET" -> set(args)
            "GET" -> get(args)
            "DEL" -> del(args)
            "EXISTS" -> exists(args)
            "EXPIRE" -> expire(args, 1000L)
            "PEXPIRE" -> expire(args, 1L)
            "TTL" -> ttl(args, seconds = true)
            "PTTL" -> ttl(args, seconds = false)
            "PERSIST" -> persist(args)
            "KEYS" -> keys(args)
            "TYPE" -> type(args)
            "DBSIZE" -> dbsize(args)
            "FLUSHDB" -> flushdb(args)
            "RENAME" -> rename(args)
            "SCAN" -> scan(args)
            "INCR" -> incr(args, 1)
            "DECR" -> incr(args, -1)
            "INCRBY" -> incrByArg(args, 1)
            "DECRBY" -> incrByArg(args, -1)
            "APPEND" -> append(args)
            "STRLEN" -> strlen(args)
            "GETRANGE" -> getrange(args)
            "SETRANGE" -> setrange(args)
            "MSET" -> mset(args)
            "MGET" -> mget(args)
            "RPUSH" -> push(args, atHead = false, requireExists = false)
            "LPUSH" -> push(args, atHead = true, requireExists = false)
            "RPUSHX" -> push(args, atHead = false, requireExists = true)
            "LPUSHX" -> push(args, atHead = true, requireExists = true)
            "LLEN" -> llen(args)
            "LRANGE" -> lrange(args)
            "LINDEX" -> lindex(args)
            "LSET" -> lset(args)
            "LPOP" -> pop(args, fromHead = true)
            "RPOP" -> pop(args, fromHead = false)
            "HSET" -> hset(args)
            "HGET" -> hget(args)
            "HMGET" -> hmget(args)
            "HGETALL" -> hgetall(args)
            "HDEL" -> hdel(args)
            "HEXISTS" -> hexists(args)
            "HKEYS" -> hkeys(args)
            "HVALS" -> hvals(args)
            "HLEN" -> hlen(args)
            "HINCRBY" -> hincrby(args)
            "SADD" -> sadd(args)
            "SREM" -> srem(args)
            "SMEMBERS" -> smembers(args)
            "SISMEMBER" -> sismember(args)
            "SCARD" -> scard(args)
            "SPOP" -> spop(args)
            "SRANDMEMBER" -> srandmember(args)
            "SINTER" -> sinter(args)
            "SUNION" -> sunion(args)
            "SDIFF" -> sdiff(args)
            "SINTERSTORE" -> storeOp(args, ::intersectAll)
            "SUNIONSTORE" -> storeOp(args, ::unionAll)
            "SDIFFSTORE" -> storeOp(args, ::diffAll)
            "ZADD" -> zadd(args)
            "ZREM" -> zrem(args)
            "ZSCORE" -> zscore(args)
            "ZCARD" -> zcard(args)
            "ZRANK" -> zrank(args, reverse = false)
            "ZREVRANK" -> zrank(args, reverse = true)
            "ZINCRBY" -> zincrby(args)
            "ZRANGE" -> zrange(args, reverse = false)
            "ZREVRANGE" -> zrange(args, reverse = true)
            "ZRANGEBYSCORE" -> zrangebyscore(args)
            "ZCOUNT" -> zcount(args)
            else -> RespValue.error("ERR unknown command '${keyOf(args[0])}'")
        }
    }

    private fun type(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments for 'type' command")
        val obj = db.get(keyOf(args[1])) ?: return RespValue.SimpleString("none")
        return RespValue.SimpleString(
            when (obj) {
                is RedisObject.StringValue -> "string"
                is RedisObject.ListValue -> "list"
                is RedisObject.HashValue -> "hash"
                is RedisObject.SetValue -> "set"
                is RedisObject.SortedSetValue -> "zset"
            }
        )
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
        if (ms < 0) return RespValue.int(ms)               // -1 or -2
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
                "MATCH" -> {
                    if (i + 1 >= args.size) return syntaxError(); pattern = keyOf(args[i + 1]); i += 2
                }

                "COUNT" -> {
                    if (i + 1 >= args.size) return syntaxError(); i += 2
                }   // ignored (simplified)
                else -> return syntaxError()
            }
        }
        val matched = db.keys().filter { globMatch(pattern, it) }
        return RespValue.Array(
            listOf(
                RespValue.bulk("0"),                                  // cursor always 0 — full scan in one call
                RespValue.Array(matched.map { RespValue.bulk(it) })
            )
        )
    }

    // ---------- string / số ----------
    /** Returns (error, data). Key absent -> (null, null). Wrong type -> (wrongType, null). */
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
        val result = ByteArray(newLen)                       // zero-padded automatically
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
            db.set(keyOf(args[i]), RedisObject.StringValue(args[i + 1]))   // MSET clears TTL like SET
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

    fun hashForRead(key: String): Pair<RespValue?, LinkedHashMap<String, ByteArray>?> {
        val obj = db.get(key) ?: return null to null
        if (obj !is RedisObject.HashValue) return wrongType() to null
        return null to obj.fields
    }

    private fun push(args: List<ByteArray>, atHead: Boolean, requireExists: Boolean): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val obj = db.get(key)
        if (obj != null && obj !is RedisObject.ListValue) return wrongType()
        if (obj == null && requireExists) return RespValue.int(0)        // *PUSHX when key does not exist
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
        var stop = keyOf(args[3]).toIntOrNull() ?: return notInteger()
        if (start < 0) start += len
        if (stop < 0) stop += len
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
            if (list.isEmpty()) db.delete(key)        // Redis does not retain empty lists
            return RespValue.bulk(v)
        }
        val n = minOf(count, list.size)
        val out = (1..n).map { RespValue.bulk(if (fromHead) list.removeFirst() else list.removeLast()) }
        if (list.isEmpty()) db.delete(key)
        return RespValue.Array(out)
    }

    private fun hset(args: List<ByteArray>): RespValue {
        if (args.size < 4 || args.size % 2 != 0)
            return RespValue.error("ERR wrong number of arguments for 'hset' command")
        val key = keyOf(args[1])
        val obj = db.get(key)
        if (obj != null && obj !is RedisObject.HashValue) return wrongType()
        val fields = if (obj is RedisObject.HashValue) obj.fields
        else LinkedHashMap<String, ByteArray>().also { db.setKeepTtl(key, RedisObject.HashValue(it)) }
        var added = 0L
        var i = 2
        while (i + 1 < args.size) {
            val f = keyOf(args[i])
            if (!fields.containsKey(f)) added++          // only count NEW fields
            fields[f] = args[i + 1]
            i += 2
        }
        return RespValue.int(added)
    }

    private fun hget(args: List<ByteArray>): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val (err, fields) = hashForRead(keyOf(args[1]))
        if (err != null) return err
        val v = fields?.get(keyOf(args[2])) ?: return RespValue.NIL
        return RespValue.bulk(v)
    }

    private fun hmget(args: List<ByteArray>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val (err, fields) = hashForRead(keyOf(args[1]))
        if (err != null) return err
        val items = (2 until args.size).map { idx ->
            val v = fields?.get(keyOf(args[idx]))
            if (v != null) RespValue.bulk(v) else RespValue.NIL
        }
        return RespValue.Array(items)
    }

    private fun hgetall(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, fields) = hashForRead(keyOf(args[1]))
        if (err != null) return err
        if (fields == null) return RespValue.Array(emptyList())
        val out = ArrayList<RespValue>(fields.size * 2)
        for ((f, v) in fields) {
            out.add(RespValue.bulk(f)); out.add(RespValue.bulk(v))
        }
        return RespValue.Array(out)
    }

    private fun hdel(args: List<ByteArray>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val (err, fields) = hashForRead(key)
        if (err != null) return err
        if (fields == null) return RespValue.int(0)
        var removed = 0L
        for (i in 2 until args.size) if (fields.remove(keyOf(args[i])) != null) removed++
        if (fields.isEmpty()) db.delete(key)             // empty hash -> delete key
        return RespValue.int(removed)
    }

    private fun hexists(args: List<ByteArray>): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val (err, fields) = hashForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.int(if (fields?.containsKey(keyOf(args[2])) == true) 1 else 0)
    }

    private fun hkeys(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, fields) = hashForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.Array(fields?.keys?.map { RespValue.bulk(it) } ?: emptyList())
    }

    private fun hvals(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, fields) = hashForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.Array(fields?.values?.map { RespValue.bulk(it) } ?: emptyList())
    }

    private fun hlen(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, fields) = hashForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.int((fields?.size ?: 0).toLong())
    }

    private fun hincrby(args: List<ByteArray>): RespValue {
        if (args.size != 4) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val delta = keyOf(args[3]).toLongOrNull() ?: return notInteger()
        val obj = db.get(key)
        if (obj != null && obj !is RedisObject.HashValue) return wrongType()
        val fields = if (obj is RedisObject.HashValue) obj.fields
        else LinkedHashMap<String, ByteArray>().also { db.setKeepTtl(key, RedisObject.HashValue(it)) }
        val f = keyOf(args[2])
        val cur = fields[f]?.toString(Charsets.UTF_8)?.toLongOrNull()
        if (fields[f] != null && cur == null) return RespValue.error("ERR hash value is not an integer")
        val next = (cur ?: 0L) + delta
        fields[f] = next.toString().toByteArray()
        return RespValue.int(next)
    }

    private fun setForRead(key: String): Pair<RespValue?, LinkedHashSet<String>?> {
        val obj = db.get(key) ?: return null to null
        if (obj !is RedisObject.SetValue) return wrongType() to null
        return null to obj.members
    }

    /** Collects sets from args[from..]. Missing key = empty set. Wrong type -> error. */
    private fun gatherSets(args: List<ByteArray>, from: Int): Pair<RespValue?, List<Set<String>>?> {
        val sets = ArrayList<Set<String>>()
        for (i in from until args.size) {
            when (val obj = db.get(keyOf(args[i]))) {
                null -> sets.add(emptySet())
                is RedisObject.SetValue -> sets.add(obj.members)
                else -> return wrongType() to null
            }
        }
        return null to sets
    }

    private fun intersectAll(sets: List<Set<String>>) =
        sets.reduceOrNull { a, b -> a.intersect(b) } ?: emptySet()

    private fun unionAll(sets: List<Set<String>>) =
        LinkedHashSet<String>().apply { sets.forEach { addAll(it) } }

    private fun diffAll(sets: List<Set<String>>): Set<String> {
        if (sets.isEmpty()) return emptySet()
        val r = LinkedHashSet(sets.first())
        for (i in 1 until sets.size) r.removeAll(sets[i])
        return r
    }

    private fun sadd(args: List<ByteArray>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val obj = db.get(key)
        if (obj != null && obj !is RedisObject.SetValue) return wrongType()
        val set = if (obj is RedisObject.SetValue) obj.members
        else LinkedHashSet<String>().also { db.setKeepTtl(key, RedisObject.SetValue(it)) }
        var added = 0L
        for (i in 2 until args.size) if (set.add(keyOf(args[i]))) added++
        return RespValue.int(added)
    }

    private fun srem(args: List<ByteArray>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val (err, set) = setForRead(key)
        if (err != null) return err
        if (set == null) return RespValue.int(0)
        var removed = 0L
        for (i in 2 until args.size) if (set.remove(keyOf(args[i]))) removed++
        if (set.isEmpty()) db.delete(key)
        return RespValue.int(removed)
    }

    private fun smembers(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, set) = setForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.Array(set?.map { RespValue.bulk(it) } ?: emptyList())
    }

    private fun sismember(args: List<ByteArray>): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val (err, set) = setForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.int(if (set?.contains(keyOf(args[2])) == true) 1 else 0)
    }

    private fun scard(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, set) = setForRead(keyOf(args[1]))
        if (err != null) return err
        return RespValue.int((set?.size ?: 0).toLong())
    }

    private fun spop(args: List<ByteArray>): RespValue {
        if (args.size !in 2..3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val (err, set) = setForRead(key)
        if (err != null) return err
        val count = if (args.size == 3) (keyOf(args[2]).toIntOrNull() ?: return notInteger()) else null
        if (count != null && count < 0) return RespValue.error("ERR value is out of range, must be positive")
        if (set == null || set.isEmpty())
            return if (count == null) RespValue.NIL else RespValue.Array(emptyList())
        if (count == null) {
            val m = set.random()
            set.remove(m)
            if (set.isEmpty()) db.delete(key)
            return RespValue.bulk(m)
        }
        val picked = set.shuffled().take(minOf(count, set.size))
        picked.forEach { set.remove(it) }
        if (set.isEmpty()) db.delete(key)
        return RespValue.Array(picked.map { RespValue.bulk(it) })
    }

    private fun srandmember(args: List<ByteArray>): RespValue {
        if (args.size !in 2..3) return RespValue.error("ERR wrong number of arguments")
        val (err, set) = setForRead(keyOf(args[1]))
        if (err != null) return err
        val count = if (args.size == 3) (keyOf(args[2]).toIntOrNull() ?: return notInteger()) else null
        if (set == null || set.isEmpty())
            return if (count == null) RespValue.NIL else RespValue.Array(emptyList())
        if (count == null) return RespValue.bulk(set.random())
        return if (count >= 0) {
            RespValue.Array(set.shuffled().take(count).map { RespValue.bulk(it) })   // no duplicates
        } else {
            val list = set.toList()
            RespValue.Array((1..(-count)).map { RespValue.bulk(list.random()) })       // duplicates allowed
        }
    }

    // --- set operations ---
    private fun sinter(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments")
        val (err, sets) = gatherSets(args, 1); if (err != null) return err
        return RespValue.Array(intersectAll(sets!!).map { RespValue.bulk(it) })
    }

    private fun sunion(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments")
        val (err, sets) = gatherSets(args, 1); if (err != null) return err
        return RespValue.Array(unionAll(sets!!).map { RespValue.bulk(it) })
    }

    private fun sdiff(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments")
        val (err, sets) = gatherSets(args, 1); if (err != null) return err
        return RespValue.Array(diffAll(sets!!).map { RespValue.bulk(it) })
    }

    // SINTERSTORE/SUNIONSTORE/SDIFFSTORE dest key [key...]
    private fun storeOp(args: List<ByteArray>, op: (List<Set<String>>) -> Set<String>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val (err, sets) = gatherSets(args, 2); if (err != null) return err   // skip dest at index 1
        val result = op(sets!!)
        val dest = keyOf(args[1])
        if (result.isEmpty()) db.delete(dest)
        else db.set(dest, RedisObject.SetValue(LinkedHashSet(result)))        // STORE overwrites -> use db.set
        return RespValue.int(result.size.toLong())
    }

    private fun formatScore(d: Double): String = when {
        d.isInfinite() -> if (d > 0) "inf" else "-inf"
        d == d.toLong().toDouble() -> d.toLong().toString()    // integer value -> strip ".0"
        else -> d.toString()
    }

    private class ScoreBound(val value: Double, val exclusive: Boolean) {
        fun acceptsMin(score: Double) = if (exclusive) score > value else score >= value
        fun acceptsMax(score: Double) = if (exclusive) score < value else score <= value
    }

    private fun parseBound(s: String): ScoreBound? {
        val exclusive = s.startsWith("(")
        val body = if (exclusive) s.substring(1) else s
        val v = when (body.lowercase()) {
            "-inf" -> Double.NEGATIVE_INFINITY
            "+inf", "inf" -> Double.POSITIVE_INFINITY
            else -> body.toDoubleOrNull() ?: return null
        }
        return ScoreBound(v, exclusive)
    }

    private fun zsetForRead(key: String): Pair<RespValue?, RedisObject.SortedSetValue?> {
        val obj = db.get(key) ?: return null to null
        if (obj !is RedisObject.SortedSetValue) return wrongType() to null
        return null to obj
    }

    private fun zsetForWrite(key: String): Pair<RespValue?, RedisObject.SortedSetValue?> {
        val obj = db.get(key)
        if (obj != null && obj !is RedisObject.SortedSetValue) return wrongType() to null
        val z = if (obj is RedisObject.SortedSetValue) obj
        else RedisObject.SortedSetValue().also { db.setKeepTtl(key, it) }
        return null to z
    }

    private fun zadd(args: List<ByteArray>): RespValue {
        if (args.size < 4 || args.size % 2 != 0)
            return RespValue.error("ERR wrong number of arguments for 'zadd' command")
        val (err, z) = zsetForWrite(keyOf(args[1])); if (err != null) return err
        var added = 0L
        var i = 2
        while (i + 1 < args.size) {
            val score = keyOf(args[i]).toDoubleOrNull() ?: return RespValue.error("ERR value is not a valid float")
            if (z!!.add(keyOf(args[i + 1]), score)) added++
            i += 2
        }
        return RespValue.int(added)
    }

    private fun zrem(args: List<ByteArray>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments")
        val key = keyOf(args[1])
        val (err, z) = zsetForRead(key); if (err != null) return err
        if (z == null) return RespValue.int(0)
        var removed = 0L
        for (i in 2 until args.size) if (z.remove(keyOf(args[i]))) removed++
        if (z.size == 0) db.delete(key)
        return RespValue.int(removed)
    }

    private fun zscore(args: List<ByteArray>): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val (err, z) = zsetForRead(keyOf(args[1])); if (err != null) return err
        val s = z?.score(keyOf(args[2])) ?: return RespValue.NIL
        return RespValue.bulk(formatScore(s))
    }

    private fun zcard(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments")
        val (err, z) = zsetForRead(keyOf(args[1])); if (err != null) return err
        return RespValue.int((z?.size ?: 0).toLong())
    }

    private fun zrank(args: List<ByteArray>, reverse: Boolean): RespValue {
        if (args.size != 3) return RespValue.error("ERR wrong number of arguments")
        val (err, z) = zsetForRead(keyOf(args[1])); if (err != null) return err
        if (z == null) return RespValue.NIL
        val r = z.rank(keyOf(args[2])) ?: return RespValue.NIL
        return RespValue.int((if (reverse) z.size - 1 - r else r).toLong())
    }

    private fun zincrby(args: List<ByteArray>): RespValue {
        if (args.size != 4) return RespValue.error("ERR wrong number of arguments")
        val (err, z) = zsetForWrite(keyOf(args[1])); if (err != null) return err
        val delta = keyOf(args[2]).toDoubleOrNull() ?: return RespValue.error("ERR value is not a valid float")
        return RespValue.bulk(formatScore(z!!.incrBy(keyOf(args[3]), delta)))
    }

    private fun zrange(args: List<ByteArray>, reverse: Boolean): RespValue {
        if (args.size < 4) return RespValue.error("ERR wrong number of arguments")
        val (err, z) = zsetForRead(keyOf(args[1])); if (err != null) return err
        val withScores = args.size >= 5 && keyOf(args[4]).equals("WITHSCORES", true)
        if (z == null) return RespValue.Array(emptyList())
        val all = if (reverse) z.ascending().reversed() else z.ascending()
        val len = all.size
        var start = keyOf(args[2]).toIntOrNull() ?: return notInteger()
        var stop = keyOf(args[3]).toIntOrNull() ?: return notInteger()
        if (start < 0) start += len
        if (stop < 0) stop += len
        if (start < 0) start = 0
        if (stop >= len) stop = len - 1
        if (start > stop) return RespValue.Array(emptyList())
        val out = ArrayList<RespValue>()
        for ((score, member) in all.subList(start, stop + 1)) {
            out.add(RespValue.bulk(member))
            if (withScores) out.add(RespValue.bulk(formatScore(score)))
        }
        return RespValue.Array(out)
    }

    private fun zrangebyscore(args: List<ByteArray>): RespValue {
        if (args.size < 4) return RespValue.error("ERR wrong number of arguments")
        val (err, z) = zsetForRead(keyOf(args[1])); if (err != null) return err
        val min = parseBound(keyOf(args[2])) ?: return RespValue.error("ERR min or max is not a float")
        val max = parseBound(keyOf(args[3])) ?: return RespValue.error("ERR min or max is not a float")
        val withScores = args.size >= 5 && keyOf(args[4]).equals("WITHSCORES", true)
        if (z == null) return RespValue.Array(emptyList())
        val out = ArrayList<RespValue>()
        for ((score, member) in z.ascending()) {
            if (min.acceptsMin(score) && max.acceptsMax(score)) {
                out.add(RespValue.bulk(member))
                if (withScores) out.add(RespValue.bulk(formatScore(score)))
            }
        }
        return RespValue.Array(out)
    }

    private fun zcount(args: List<ByteArray>): RespValue {
        if (args.size != 4) return RespValue.error("ERR wrong number of arguments")
        val (err, z) = zsetForRead(keyOf(args[1])); if (err != null) return err
        val min = parseBound(keyOf(args[2])) ?: return RespValue.error("ERR min or max is not a float")
        val max = parseBound(keyOf(args[3])) ?: return RespValue.error("ERR min or max is not a float")
        if (z == null) return RespValue.int(0)
        var c = 0L
        for ((score, _) in z.ascending()) if (min.acceptsMin(score) && max.acceptsMax(score)) c++
        return RespValue.int(c)
    }
}