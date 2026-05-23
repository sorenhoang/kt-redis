package io.ktredis.protocol

import io.ktor.utils.io.*
import java.io.ByteArrayOutputStream

sealed interface RespValue{
    data class SimpleString(val value: String) : RespValue
    data class Error(val message: String) : RespValue
    data class Integer(val value: Long) : RespValue
    data class BulkString(val data: ByteArray?) : RespValue   // null = $-1
    data class Array(val items: List<RespValue>?) : RespValue // null = *-1
    data class Raw(val bytes: ByteArray) : RespValue          // bytes thô, ghi nguyên xi (dùng cho khung RDB của PSYNC)

    companion object {
        val OK = SimpleString("OK")
        val PONG = SimpleString("PONG")
        val NIL = BulkString(null)
        fun bulk(s: String) = BulkString(s.toByteArray())
        fun bulk(b: ByteArray?) = BulkString(b)
        fun error(msg: String) = Error(msg)
        fun int(n: Long) = Integer(n)
    }
}

object Resp{
    fun encode(value: RespValue): ByteArray = when (value) {
        is RespValue.SimpleString -> "+${value.value}\r\n".toByteArray()
        is RespValue.Error        -> "-${value.message}\r\n".toByteArray()
        is RespValue.Integer      -> ":${value.value}\r\n".toByteArray()
        is RespValue.BulkString   -> encodeBulk(value.data)
        is RespValue.Array        -> encodeArray(value.items)
        is RespValue.Raw          -> value.bytes
    }

    private fun encodeBulk(data: ByteArray?): ByteArray {
        if(data == null) return "\$-1\r\n".toByteArray()
        val out = ByteArrayOutputStream()
        out.write("\$${data.size}\r\n".toByteArray())
        out.write(data)
        out.write("\r\n".toByteArray())
        return out.toByteArray()
    }

    private fun encodeArray(items: List<RespValue>?): ByteArray {
        if (items == null) return "*-1\r\n".toByteArray()
        val out = ByteArrayOutputStream()
        out.write("*${items.size}\r\n".toByteArray())
        for(item in items) {
            out.write(encode(item))
        }
        return out.toByteArray()
    }
}

suspend fun ByteWriteChannel.writeResp(value: RespValue) {
    writeByteArray(Resp.encode(value))
    flush()
}