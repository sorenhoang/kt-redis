package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SortedSetCommandsTest {

    private val clock = FakeClock()
    private val db = RedisDatabase(clock)
    private val d = CommandDispatcher(db)
    private fun cmd(vararg p: String) = p.map { it.toByteArray() }
    private fun str(r: RespValue) = (r as RespValue.BulkString).data!!.toString(Charsets.UTF_8)

    @Test fun `zadd zrange withscores`() {
        assertEquals(RespValue.int(2), d.dispatch(cmd("ZADD", "lb", "100", "an", "90", "bo")))
        val arr = (d.dispatch(cmd("ZRANGE", "lb", "0", "-1", "WITHSCORES")) as RespValue.Array)
            .items!!.map { str(it) }
        assertEquals(listOf("bo", "90", "an", "100"), arr)   // ascending by score
    }

    @Test fun `zrank`() {
        d.dispatch(cmd("ZADD", "lb", "100", "an", "90", "bo"))
        assertEquals(RespValue.int(1), d.dispatch(cmd("ZRANK", "lb", "an")))
        assertEquals(RespValue.int(0), d.dispatch(cmd("ZREVRANK", "lb", "an")))
    }

    @Test fun `zincrby and zscore`() {
        d.dispatch(cmd("ZADD", "lb", "5", "x"))
        assertEquals("8", str(d.dispatch(cmd("ZINCRBY", "lb", "3", "x"))))
        assertEquals("8", str(d.dispatch(cmd("ZSCORE", "lb", "x"))))
    }

    @Test fun `zrangebyscore with exclusive and inf`() {
        d.dispatch(cmd("ZADD", "lb", "1", "a", "2", "b", "3", "c"))
        val r = (d.dispatch(cmd("ZRANGEBYSCORE", "lb", "(1", "+inf")) as RespValue.Array)
            .items!!.map { str(it) }
        assertEquals(listOf("b", "c"), r)
    }

    @Test fun `wrongtype`() {
        d.dispatch(cmd("SET", "k", "v"))
        assertTrue(d.dispatch(cmd("ZADD", "k", "1", "a")) is RespValue.Error)
    }
    
}