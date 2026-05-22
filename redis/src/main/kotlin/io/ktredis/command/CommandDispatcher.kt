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
}