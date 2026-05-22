package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ListCommandsTest {

    private val clock = FakeClock()
    private val db = RedisDatabase(clock)
    private val d = CommandDispatcher(db)
    private fun cmd(vararg p: String) = p.map { it.toByteArray() }
    private fun str(r: RespValue) = (r as RespValue.BulkString).data!!.toString(Charsets.UTF_8)

    @Test fun `rpush and lrange`() {
        assertEquals(RespValue.int(3), d.dispatch(cmd("RPUSH", "l", "a", "b", "c")))
        val arr = d.dispatch(cmd("LRANGE", "l", "0", "-1")) as RespValue.Array
        assertEquals(listOf("a", "b", "c"), arr.items!!.map { str(it) })
    }

    @Test fun `lpush reverses order`() {
        d.dispatch(cmd("LPUSH", "l", "a", "b", "c"))
        val arr = d.dispatch(cmd("LRANGE", "l", "0", "-1")) as RespValue.Array
        assertEquals(listOf("c", "b", "a"), arr.items!!.map { str(it) })
    }

    @Test fun `pop empties then key disappears`() {
        d.dispatch(cmd("RPUSH", "l", "x"))
        assertEquals("x", str(d.dispatch(cmd("LPOP", "l"))))
        assertEquals(RespValue.int(0), d.dispatch(cmd("EXISTS", "l")))
    }

    @Test
    fun `wrongtype`() {
        d.dispatch(cmd("SET", "s", "v"))
        assertTrue(d.dispatch(cmd("LPUSH", "s", "x")) is RespValue.Error)
    }
}