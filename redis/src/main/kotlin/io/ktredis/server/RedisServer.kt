package io.ktredis.server

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktredis.protocol.*
import kotlinx.coroutines.*

class RedisServer(
    private val host: String = "127.0.0.1",
    private val port: Int = 6379
) {
    suspend fun start() = coroutineScope{
        val executor = CommandExecutor(this)
        val selector = SelectorManager(Dispatchers.IO)
        var serverSocket = aSocket(selector).tcp().bind(host, port)
        println("kt-redis is listening on $host:$port")

        while (true) {
            val socket = serverSocket.accept()
            launch { handleClient(socket, executor) }
        }
    }

    private suspend fun handleClient(socket: Socket, executor: CommandExecutor) {
        val remote = socket.remoteAddress
        println("client connected: $remote")
        val reader = RespReader(socket.openReadChannel())
        val writer = socket.openWriteChannel(autoFlush = true)

        try {
            while (true) {
                val args = reader.readCommand() ?: break
                if (args.isEmpty()) continue
                writer.writeResp(executor.execute(args))
            }
        } catch (e: Throwable) {

        } finally {
            socket.close()
            println("client disconnected: $remote")
        }
    }
}