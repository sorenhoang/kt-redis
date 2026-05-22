package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.Clock
import io.ktredis.storage.RedisDatabase
import kotlin.test.Test
import kotlin.test.assertEquals

class FakeClock(var time: Long = 0) : Clock {
    override fun now(): Long = time
}

class ExpiryTest {
    private val clock = FakeClock()
    private val db = RedisDatabase(clock)
    private val d = CommandDispatcher(db)
    private fun cmd(vararg p: String) = p.map { it.toByteArray() }
    private fun str(r: RespValue) = (r as RespValue.BulkString).data!!.toString(Charsets.UTF_8)

    @Test fun `px expires after time advances`() {
        d.dispatch(cmd("SET", "k", "v", "PX", "100"))
        assertEquals("v", str(d.dispatch(cmd("GET", "k"))))   // còn sống
        clock.time += 150                                      // "tua" 150ms
        assertEquals(RespValue.NIL, d.dispatch(cmd("GET", "k")))
        assertEquals(RespValue.int(-2), d.dispatch(cmd("TTL", "k")))
    }

    @Test fun `ttl of persistent key is -1`() {
        d.dispatch(cmd("SET", "k", "v"))
        assertEquals(RespValue.int(-1), d.dispatch(cmd("TTL", "k")))
    }

    @Test fun `expire then persist`() {
        d.dispatch(cmd("SET", "k", "v"))
        assertEquals(RespValue.int(1), d.dispatch(cmd("EXPIRE", "k", "100")))
        assertEquals(RespValue.int(1), d.dispatch(cmd("PERSIST", "k")))
        assertEquals(RespValue.int(-1), d.dispatch(cmd("TTL", "k")))
    }

    @Test fun `set clears old ttl`() {
        d.dispatch(cmd("SET", "k", "v", "EX", "100"))
        d.dispatch(cmd("SET", "k", "v2"))                      // SET lại → mất TTL
        assertEquals(RespValue.int(-1), d.dispatch(cmd("TTL", "k")))
    }
}