package io.ktredis.command

import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StringCommandsTest {
    private val clock = FakeClock()
    private val db = RedisDatabase(clock)
    private val d = CommandDispatcher(db)
    private fun cmd(vararg p: String) = p.map { it.toByteArray() }
    private fun str(r: RespValue) = (r as RespValue.BulkString).data!!.toString(Charsets.UTF_8)

    @Test fun `incr from missing key starts at 0`() {
        assertEquals(RespValue.int(1), d.dispatch(cmd("INCR", "n")))
        assertEquals(RespValue.int(2), d.dispatch(cmd("INCR", "n")))
        assertEquals(RespValue.int(12), d.dispatch(cmd("INCRBY", "n", "10")))
        assertEquals(RespValue.int(11), d.dispatch(cmd("DECR", "n")))
    }

    @Test fun `incr on non-integer errors`() {
        d.dispatch(cmd("SET", "n", "abc"))
        assertTrue(d.dispatch(cmd("INCR", "n")) is RespValue.Error)
    }

    @Test fun `incr keeps ttl`() {
        d.dispatch(cmd("SET", "n", "5", "EX", "100"))
        d.dispatch(cmd("INCR", "n"))
        assertEquals(RespValue.int(100), d.dispatch(cmd("TTL", "n")))   // TTL not cleared
    }

    @Test fun `append returns new length`() {
        assertEquals(RespValue.int(5), d.dispatch(cmd("APPEND", "s", "hello")))
        assertEquals(RespValue.int(10), d.dispatch(cmd("APPEND", "s", "world")))
        assertEquals("helloworld", str(d.dispatch(cmd("GET", "s"))))
    }

    @Test fun `strlen`() {
        d.dispatch(cmd("SET", "s", "hello"))
        assertEquals(RespValue.int(5), d.dispatch(cmd("STRLEN", "s")))
        assertEquals(RespValue.int(0), d.dispatch(cmd("STRLEN", "missing")))
    }

    @Test fun `getrange with negative indices`() {
        d.dispatch(cmd("SET", "s", "Hello World"))
        assertEquals("Hello", str(d.dispatch(cmd("GETRANGE", "s", "0", "4"))))
        assertEquals("World", str(d.dispatch(cmd("GETRANGE", "s", "-5", "-1"))))
    }

    @Test fun `mset and mget`() {
        assertEquals(RespValue.OK, d.dispatch(cmd("MSET", "a", "1", "b", "2")))
        val arr = d.dispatch(cmd("MGET", "a", "b", "missing")) as RespValue.Array
        assertEquals("1", str(arr.items!![0]))
        assertEquals("2", str(arr.items!![1]))
        assertEquals(RespValue.NIL, arr.items!![2])
    }

    @Test fun `type`() {
        d.dispatch(cmd("SET", "s", "v"))
        assertEquals(RespValue.SimpleString("string"), d.dispatch(cmd("TYPE", "s")))
        assertEquals(RespValue.SimpleString("none"), d.dispatch(cmd("TYPE", "missing")))
    }

    @Test fun `keys with glob`() {
        d.dispatch(cmd("MSET", "user:1", "a", "user:2", "b", "post:1", "c"))
        val arr = d.dispatch(cmd("KEYS", "user:*")) as RespValue.Array
        assertEquals(setOf("user:1", "user:2"), arr.items!!.map { str(it) }.toSet())
    }

    @Test fun `dbsize and flushdb`() {
        d.dispatch(cmd("MSET", "a", "1", "b", "2"))
        assertEquals(RespValue.int(2), d.dispatch(cmd("DBSIZE")))
        d.dispatch(cmd("FLUSHDB"))
        assertEquals(RespValue.int(0), d.dispatch(cmd("DBSIZE")))
    }

    @Test fun `rename moves value and ttl`() {
        d.dispatch(cmd("SET", "a", "v", "EX", "100"))
        assertEquals(RespValue.OK, d.dispatch(cmd("RENAME", "a", "b")))
        assertEquals("v", str(d.dispatch(cmd("GET", "b"))))
        assertEquals(RespValue.int(0), d.dispatch(cmd("EXISTS", "a")))
        assertEquals(RespValue.int(100), d.dispatch(cmd("TTL", "b")))
        assertTrue(d.dispatch(cmd("RENAME", "nope", "x")) is RespValue.Error)
    }
}
