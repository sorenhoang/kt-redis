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
 * Replica side: connects to master, performs handshake (PING -> REPLCONF -> PSYNC),
 * receives full RDB resync, then applies all write commands propagated by master.
 * Automatically reconnects on failure.
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
                println("replica link error: ${e.message} — retrying in 1s")
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
            suspend fun line(): String = read.readUTF8Line() ?: throw RuntimeException("master closed connection")

            // 1) handshake
            cmd("PING"); line()                                          // +PONG
            cmd("REPLCONF", "listening-port", myPort.toString()); line() // +OK
            cmd("REPLCONF", "capa", "eof", "capa", "psync2"); line()     // +OK
            cmd("PSYNC", "?", "-1")
            println("replica: ${line()}")                                // +FULLRESYNC <id> <off>

            // 2) receive RDB: $<len>\r\n<bytes> (no trailing CRLF)
            val lenLine = line()
            require(lenLine.startsWith("$")) { "expected \$<len> but got: $lenLine" }
            val len = lenLine.substring(1).toInt()
            val rdb = read.readByteArray(len)
            executor.installSnapshot(Rdb.load(ByteArrayInputStream(rdb)))
            executor.setLinkUp(true)
            println("replica: RDB loaded ($len bytes), starting command stream from master")

            // 3) command stream: apply all write commands from master
            val reader = RespReader(read)
            while (true) {
                val command = reader.readCommand() ?: break
                if (command.isNotEmpty()) executor.applyReplicated(command)
            }
            throw RuntimeException("master closed stream")                // -> outer loop retries
        } finally {
            socket.close()
            selector.close()
        }
    }
}
