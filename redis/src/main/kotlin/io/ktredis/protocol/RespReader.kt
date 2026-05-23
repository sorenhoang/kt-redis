package io.ktredis.protocol

import io.ktor.utils.io.*

class RespException(message: String) : Exception(message)

class RespReader(private val channel: ByteReadChannel) {

    suspend fun readCommand(): List<ByteArray>? {
        val line = channel.readUTF8Line() ?: return null

        if (line.isEmpty()) return emptyList()

        return when (line[0]) {
            '*' -> readArray(line)
            else -> line.trim().split(" ").filter { it.isNotEmpty() }.map { it.toByteArray() } // inline command (temporary)
        }
    }

    private suspend fun readArray(header: String): List<ByteArray> {
        val count = header.substring(1).toIntOrNull() ?: throw RespException("invalid multibulk length: $header")

        if (count <= 0) return emptyList()
        var args = ArrayList<ByteArray>()

        repeat(count) {
            val bulkHeader = channel.readUTF8Line() ?: throw RespException("unexpected EOF")

            if (bulkHeader.isEmpty() || bulkHeader[0] != '$')
                throw RespException("expected '$', got: $bulkHeader")

            val len = bulkHeader.substring(1).toIntOrNull() ?: throw RespException("expected '$', got: $bulkHeader")
            val data = channel.readByteArray(len)

            channel.readByte()
            channel.readByte()

            args.add(data)
        }

        return args
    }
}