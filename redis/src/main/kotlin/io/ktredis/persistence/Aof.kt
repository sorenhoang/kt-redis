package io.ktredis.persistence

import io.ktor.util.cio.*
import io.ktredis.protocol.Resp
import io.ktredis.protocol.RespReader
import io.ktredis.protocol.RespValue
import java.io.File
import java.io.FileOutputStream

enum class FsyncPolicy { ALWAYS, EVERYSEC, NO }

class Aof(
    private val file: File,
    val policy: FsyncPolicy = FsyncPolicy.EVERYSEC,
) {
    private val out = FileOutputStream(file, true)

    fun append(args: List<ByteArray>) {
        val resp = RespValue.Array(args.map { RespValue.bulk(it) })
        out.write(Resp.encode(resp))
        if (policy == FsyncPolicy.ALWAYS) {
            fsync()
        }
    }

    fun fsync() {
        out.flush()
        out.fd.sync()
    }

    fun close(){
        fsync()
        out.close()
    }

    suspend fun loadCommands(): List<List<ByteArray>>{
        if (!file.exists() || file.length() == 0L) return emptyList()
        val reader = RespReader(file.readChannel())
        val cmds = ArrayList<List<ByteArray>>()
        while (true) {
            val cmd = reader.readCommand() ?: break
            if (cmd.isNotEmpty()) cmds.add(cmd)
        }
        return cmds
    }
}