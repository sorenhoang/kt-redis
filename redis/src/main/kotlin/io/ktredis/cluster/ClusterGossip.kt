package io.ktredis.cluster

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import io.ktredis.protocol.RespValue
import io.ktredis.protocol.writeResp
import kotlinx.coroutines.*

/**
 * Simplified gossip: exchanges the node table with peers via the `CLUSTER GOSSIP <table>` command over the client port.
 * (Real Redis uses a separate binary cluster-bus; this implementation uses its own protocol between kt-redis nodes.)
 */
class ClusterGossip(
    private val state: ClusterState,
    private val scope: CoroutineScope
) {
    /** Periodically gossips with all known peers -> propagates topology and slot ownership. */
    fun startPeriodic(): Job = scope.launch {
        while (isActive) {
            delay(1000)
            for ((ip, port) in state.peers()) {
                try { gossipTo(ip, port) } catch (_: Throwable) { /* peer temporarily down -> skip */ }
            }
        }
    }

    /** CLUSTER MEET: initial handshake with an unknown node. */
    fun meetAsync(ip: String, port: Int): Job = scope.launch {
        try { gossipTo(ip, port) } catch (e: Throwable) { println("CLUSTER MEET error: ${e.message}") }
    }

    private suspend fun gossipTo(ip: String, port: Int) {
        val selector = SelectorManager(Dispatchers.IO)
        val socket = aSocket(selector).tcp().connect(ip, port)
        try {
            val read = socket.openReadChannel()
            val write = socket.openWriteChannel(autoFlush = true)
            write.writeResp(
                RespValue.Array(listOf(RespValue.bulk("CLUSTER"), RespValue.bulk("GOSSIP"), RespValue.bulk(state.serialize())))
            )
            // read 1 bulk string reply = peer's node table
            val line = read.readUTF8Line() ?: return
            if (!line.startsWith("$")) return
            val len = line.substring(1).toIntOrNull() ?: return
            if (len < 0) return
            val data = read.readByteArray(len)
            read.readByte(); read.readByte()                  // CRLF
            state.mergeFrom(String(data, Charsets.UTF_8))
        } finally {
            socket.close(); selector.close()
        }
    }
}
