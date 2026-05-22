package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SetCommandsTest {

    private val clock = FakeClock()
    private val db = RedisDatabase(clock)
    private val d = CommandDispatcher(db)
    private fun cmd(vararg p: String) = p.map { it.toByteArray() }
    private fun str(r: RespValue) = (r as RespValue.BulkString).data!!.toString(Charsets.UTF_8)

    @Test fun `sadd dedupes and scard`() {
        assertEquals(RespValue.int(3), d.dispatch(cmd("SADD", "s", "a", "b", "c")))
        assertEquals(RespValue.int(0), d.dispatch(cmd("SADD", "s", "a")))   // trùng, không thêm
        assertEquals(RespValue.int(3), d.dispatch(cmd("SCARD", "s")))
    }

    @Test fun `sismember`() {
        d.dispatch(cmd("SADD", "s", "a"))
        assertEquals(RespValue.int(1), d.dispatch(cmd("SISMEMBER", "s", "a")))
        assertEquals(RespValue.int(0), d.dispatch(cmd("SISMEMBER", "s", "z")))
    }

    @Test fun `sinter`() {
        d.dispatch(cmd("SADD", "s1", "a", "b", "c"))
        d.dispatch(cmd("SADD", "s2", "b", "c", "d"))
        val r = (d.dispatch(cmd("SINTER", "s1", "s2")) as RespValue.Array).items!!.map { str(it) }.toSet()
        assertEquals(setOf("b", "c"), r)
    }

    @Test fun `sdiffstore`() {
        d.dispatch(cmd("SADD", "s1", "a", "b", "c"))
        d.dispatch(cmd("SADD", "s2", "b"))
        assertEquals(RespValue.int(2), d.dispatch(cmd("SDIFFSTORE", "dst", "s1", "s2")))
        val r = (d.dispatch(cmd("SMEMBERS", "dst")) as RespValue.Array).items!!.map { str(it) }.toSet()
        assertEquals(setOf("a", "c"), r)
    }

    @Test fun `wrongtype`() {
        d.dispatch(cmd("SET", "k", "v"))
        assertTrue(d.dispatch(cmd("SADD", "k", "a")) is RespValue.Error)
    }

}