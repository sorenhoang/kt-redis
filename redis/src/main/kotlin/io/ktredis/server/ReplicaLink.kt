package io.ktredis.server

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import io.ktredis.persistence.Rdb
import io.ktredis.protocol.RespReader
import io.ktredis.protocol.RespValue
import io.ktredis.protocol.writeResp
import kotlinx.coroutines.*
import java.io.ByteArrayInputStream

/**
 * Phía replica: kết nối master, bắt tay (PING -> REPLCONF -> PSYNC), nhận RDB full resync,
 * rồi áp dụng mọi lệnh ghi master truyền xuống. Tự thử kết nối lại khi rớt.
 */
class ReplicaLink(
    private val masterHost: String,
    private val masterPort: Int,
    private val myPort: Int,
    private val executor: CommandExecutor,
    private val scope: CoroutineScope
) {
    fun start(): Job = scope.launch {
        while (isActive) {
            try {
                sync()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                println("replica link lỗi: ${e.message} — thử lại sau 1s")
                executor.setLinkUp(false)
                delay(1000)
            }
        }
    }

    private suspend fun sync() {
        val selector = SelectorManager(Dispatchers.IO)
        val socket = aSocket(selector).tcp().connect(masterHost, masterPort)
        try {
            val read = socket.openReadChannel()
            val write = socket.openWriteChannel(autoFlush = true)

            suspend fun cmd(vararg parts: String) =
                write.writeResp(RespValue.Array(parts.map { RespValue.bulk(it) }))
            suspend fun line(): String = read.readUTF8Line() ?: throw RuntimeException("master đóng kết nối")

            // 1) handshake
            cmd("PING"); line()                                          // +PONG
            cmd("REPLCONF", "listening-port", myPort.toString()); line() // +OK
            cmd("REPLCONF", "capa", "eof", "capa", "psync2"); line()     // +OK
            cmd("PSYNC", "?", "-1")
            println("replica: ${line()}")                                // +FULLRESYNC <id> <off>

            // 2) nhận RDB: $<len>\r\n<bytes> (KHÔNG có CRLF cuối)
            val lenLine = line()
            require(lenLine.startsWith("$")) { "mong đợi \$<len> nhưng nhận: $lenLine" }
            val len = lenLine.substring(1).toInt()
            val rdb = read.readByteArray(len)
            executor.installSnapshot(Rdb.load(ByteArrayInputStream(rdb)))
            executor.setLinkUp(true)
            println("replica: đã nạp RDB ($len bytes), bắt đầu nhận stream từ master")

            // 3) stream: áp dụng mọi lệnh ghi master gửi xuống
            val reader = RespReader(read)
            while (true) {
                val command = reader.readCommand() ?: break
                if (command.isNotEmpty()) executor.applyReplicated(command)
            }
            throw RuntimeException("master ngắt stream")                 // -> vòng ngoài thử kết nối lại
        } finally {
            socket.close()
            selector.close()
        }
    }
}
