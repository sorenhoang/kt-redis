package io.ktredis.cluster

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import io.ktredis.protocol.RespValue
import io.ktredis.protocol.writeResp
import kotlinx.coroutines.*

/**
 * Gossip đơn giản hoá: trao đổi node table với peer bằng lệnh `CLUSTER GOSSIP <table>` qua cổng client.
 * (Redis thật dùng cluster-bus nhị phân riêng; bản học dùng giao thức của chính mình giữa các kt-redis node.)
 */
class ClusterGossip(
    private val state: ClusterState,
    private val scope: CoroutineScope
) {
    /** Định kỳ gossip với mọi peer đã biết -> lan truyền topology + quyền sở hữu slot. */
    fun startPeriodic(): Job = scope.launch {
        while (isActive) {
            delay(1000)
            for ((ip, port) in state.peers()) {
                try { gossipTo(ip, port) } catch (_: Throwable) { /* peer tạm thời down -> bỏ qua */ }
            }
        }
    }

    /** CLUSTER MEET: bắt tay lần đầu với một node chưa biết. */
    fun meetAsync(ip: String, port: Int): Job = scope.launch {
        try { gossipTo(ip, port) } catch (e: Throwable) { println("CLUSTER MEET lỗi: ${e.message}") }
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
            // đọc 1 reply bulk string = node table của peer
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
