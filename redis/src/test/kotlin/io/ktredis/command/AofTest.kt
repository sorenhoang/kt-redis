package io.ktredis.command

import io.ktredis.persistence.Aof
import io.ktredis.persistence.FsyncPolicy
import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AofTest {

    private val clock = FakeClock()
    private val db = RedisDatabase(clock)
    private val d = CommandDispatcher(db)
    private fun cmd(vararg p: String) = p.map { it.toByteArray() }
    private fun str(r: RespValue) = (r as RespValue.BulkString).data!!.toString(Charsets.UTF_8)

    @Test fun `append and load correct command`() = runBlocking {
        val f = File.createTempFile("test", ".aof").apply { deleteOnExit() }
        val aof = Aof(f, FsyncPolicy.ALWAYS)
        aof.append(listOf("SET".toByteArray(), "foo".toByteArray(), "bar".toByteArray()))
        aof.close()

        val cmds = Aof(f, FsyncPolicy.NO).loadCommands()
        assertEquals(1, cmds.size)
        assertEquals("SET", cmds[0][0].toString(Charsets.UTF_8))
        assertEquals("bar", cmds[0][2].toString(Charsets.UTF_8))
    }
}