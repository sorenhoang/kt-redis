package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class HashCommandsTest {

    private val clock = FakeClock()
    private val db = RedisDatabase(clock)
    private val d = CommandDispatcher(db)
    private fun cmd(vararg p: String) = p.map { it.toByteArray() }
    private fun str(r: RespValue) = (r as RespValue.BulkString).data!!.toString(Charsets.UTF_8)

    @Test fun `hset counts new fields`() {
        assertEquals(RespValue.int(2), d.dispatch(cmd("HSET", "h", "a", "1", "b", "2")))
        assertEquals(RespValue.int(0), d.dispatch(cmd("HSET", "h", "a", "9")))   // ghi đè, không tính
        assertEquals("9", str(d.dispatch(cmd("HGET", "h", "a"))))
    }

    @Test fun `hgetall returns pairs`() {
        d.dispatch(cmd("HSET", "h", "a", "1", "b", "2"))
        val arr = (d.dispatch(cmd("HGETALL", "h")) as RespValue.Array).items!!.map { str(it) }
        assertEquals(listOf("a", "1", "b", "2"), arr)
    }

    @Test fun `hincrby`() {
        assertEquals(RespValue.int(5), d.dispatch(cmd("HINCRBY", "h", "n", "5")))
        assertEquals(RespValue.int(7), d.dispatch(cmd("HINCRBY", "h", "n", "2")))
    }

    @Test fun `hdel empties then key gone`() {
        d.dispatch(cmd("HSET", "h", "a", "1"))
        d.dispatch(cmd("HDEL", "h", "a"))
        assertEquals(RespValue.int(0), d.dispatch(cmd("EXISTS", "h")))
    }

    @Test fun `wrongtype`() {
        d.dispatch(cmd("SET", "s", "v"))
        assertTrue(d.dispatch(cmd("HSET", "s", "a", "1")) is RespValue.Error)
    }
}