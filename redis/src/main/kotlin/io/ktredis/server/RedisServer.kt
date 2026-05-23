package io.ktredis.server

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktredis.persistence.Aof
import io.ktredis.persistence.FsyncPolicy
import io.ktredis.protocol.*
import kotlinx.coroutines.*
import java.io.File

class RedisServer(
    private val host: String = "127.0.0.1",
    private val port: Int = 6379
) {
    suspend fun start() = coroutineScope{
        val selector = SelectorManager(Dispatchers.IO)
        var serverSocket = aSocket(selector).tcp().bind(host, port)

        val aof = Aof(File("appendonly.aof"), FsyncPolicy.EVERYSEC)
        val executor = CommandExecutor(this, aof)
        executor.loadAof()                          //

        println("kt-redis is listening on $host:$port")

        while (true) {
            val socket = serverSocket.accept()
            launch { handleClient(socket, executor) }
        }
    }

    private suspend fun handleClient(socket: Socket, executor: CommandExecutor) = coroutineScope {
        val remote = socket.remoteAddress
        println("client connected: $remote")
        val reader = RespReader(socket.openReadChannel())
        val writeChannel = socket.openWriteChannel(autoFlush = true)
        val client = ClientHandle()

        // coroutine writer: rút outgoing -> socket (mọi lần ghi đều qua đây)
        val writer = launch {
            try {
                for (reply in client.outgoing) writeChannel.writeResp(reply)
            } catch (_: Throwable) { /* socket đóng */ }
        }
        try {
            while (true) {
                val args = reader.readCommand() ?: break
                if (args.isEmpty()) continue
                executor.execute(args, client)        // reply sẽ vào client.outgoing
            }
        } catch (e: Throwable) {
            println("connection error: ${e.message}")
        } finally {
            executor.disconnect(client)               // gỡ subscribe
            client.outgoing.close()                   // writer drain xong rồi kết thúc
            socket.close()
            println("client disconnected: $remote")
        }
    }
}