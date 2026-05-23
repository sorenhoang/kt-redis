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
    private val port: Int = 6379,
    private val replicaOf: Pair<String, Int>? = null,   // (masterHost, masterPort) when running as replica
    private val clusterEnabled: Boolean = false
) {
    suspend fun start() = coroutineScope{
        val selector = SelectorManager(Dispatchers.IO)
        var serverSocket = aSocket(selector).tcp().bind(host, port)

        val aofFile = File("appendonly.aof")
        val hadAof = aofFile.exists() && aofFile.length() > 0     // decide BEFORE Aof creates an empty file
        val aof = Aof(aofFile, FsyncPolicy.EVERYSEC)
        val executor = CommandExecutor(this, aof, myPort = port, clusterEnabled = clusterEnabled, myHost = host)

        if (hadAof) executor.loadAof()              // prefer AOF if data exists
        else executor.loadRdb()                     // otherwise load dump.rdb (if it exists)

        // if started with --replicaof: become a replica and connect to master
        if (replicaOf != null) {
            executor.becomeReplica(replicaOf.first, replicaOf.second)
            ReplicaLink(replicaOf.first, replicaOf.second, port, executor, this).start()
            println("kt-redis running in REPLICA mode of ${replicaOf.first}:${replicaOf.second}")
        }

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

        // coroutine writer: drains outgoing -> socket (all writes go through here)
        val writer = launch {
            try {
                for (reply in client.outgoing) writeChannel.writeResp(reply)
            } catch (_: Throwable) { /* socket closed */ }
        }
        try {
            while (true) {
                val args = reader.readCommand() ?: break
                if (args.isEmpty()) continue
                executor.execute(args, client)        // reply goes into client.outgoing
            }
        } catch (e: Throwable) {
            println("connection error: ${e.message}")
        } finally {
            executor.disconnect(client)               // unsubscribe
            client.outgoing.close()                   // writer finishes draining then terminates
            socket.close()
            println("client disconnected: $remote")
        }
    }
}