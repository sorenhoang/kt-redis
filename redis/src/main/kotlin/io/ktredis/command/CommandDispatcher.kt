package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.*

class CommandDispatcher(private val db: RedisDatabase) {

    fun dispatch(args: List<ByteArray>): RespValue {
        if (args.isEmpty()) return RespValue.error("ERR empty command")
        val name = args[0].toString(Charsets.UTF_8).uppercase()
        return when (name) {
            "PING"   -> ping(args)
            "ECHO"   -> echo(args)
            "SET"    -> set(args)
            "GET"    -> get(args)
            "DEL"    -> del(args)
            "EXISTS" -> exists(args)
            else     -> RespValue.error("ERR unknown command '${args[0].toString(Charsets.UTF_8)}'")
        }
    }

    private fun ping(args: List<ByteArray>): RespValue =
        if (args.size >= 2) RespValue.bulk(args[1]) else RespValue.PONG

    private fun echo(args: List<ByteArray>): RespValue =
        if (args.size == 2) RespValue.bulk(args[1])
        else RespValue.error("ERR wrong number of arguments for 'echo' command")

    private fun set(args: List<ByteArray>): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments for 'set' command")
        db.set(args[1].toString(Charsets.UTF_8), RedisObject.StringValue(args[2]))
        return RespValue.OK
    }

    private fun get(args: List<ByteArray>): RespValue {
        if (args.size != 2) return RespValue.error("ERR wrong number of arguments for 'get' command")
        val obj = db.get(args[1].toString(Charsets.UTF_8)) ?: return RespValue.NIL
        if (obj !is RedisObject.StringValue)
            return RespValue.error("WRONGTYPE Operation against a key holding the wrong kind of value")
        return RespValue.bulk(obj.data)
    }

    private fun del(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments for 'del' command")
        var count = 0L
        for (i in 1 until args.size)
            if (db.delete(args[i].toString(Charsets.UTF_8))) count++
        return RespValue.int(count)
    }

    private fun exists(args: List<ByteArray>): RespValue {
        if (args.size < 2) return RespValue.error("ERR wrong number of arguments for 'exists' command")
        var count = 0L
        for (i in 1 until args.size)
            if (db.exists(args[i].toString(Charsets.UTF_8))) count++
        return RespValue.int(count)
    }
}