package io.ktredis.server

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.readUTF8Line
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.*

class RedisServer(
    private val host: String = "127.0.0.1",
    private val port: Int = 6379
) {
    suspend fun start() = coroutineScope{
        val selector = SelectorManager(Dispatchers.IO)
        var serverSocket = aSocket(selector).tcp().bind(host, port)
        println("kt-redis is listening on $host:$port")

        while (true) {
            val socket = serverSocket.accept()
            launch { handleClient(socket) }
        }
    }

    private suspend fun handleClient(socket: Socket) {
        val remote = socket.remoteAddress
        println("client connected $remote")

        val reader = socket.openReadChannel()
        val writer = socket.openWriteChannel(autoFlush = true)
        try {
            while (true) {
                val line = reader.readUTF8Line() ?: break   // null = client close
                writer.writeStringUtf8("Response: $line\r\n")
            }
        } catch (e: Throwable) {

        } finally {
            socket.close()
            println("client disconnected: \$remote")
        }
    }
}