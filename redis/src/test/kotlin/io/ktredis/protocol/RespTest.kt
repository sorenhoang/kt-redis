package io.ktredis.protocol

import io.ktor.utils.io.*
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class RespTest {

    @Test
    fun `read array command`() = runBlocking {
        val ch = ByteReadChannel("*2\r\n\$4\r\nECHO\r\n\$3\r\nhey\r\n".toByteArray())
        val cmd = RespReader(ch).readCommand()!!.map { it.toString(Charsets.UTF_8) }
        assertEquals(listOf("ECHO", "hey"), cmd)
    }

    @Test
    fun `read returns null at EOF`() = runBlocking {
        val ch = ByteReadChannel(ByteArray(0))
        assertEquals(null, RespReader(ch).readCommand())
    }

    @Test fun `encode simple string`() =
        assertEquals("+OK\r\n", Resp.encode(RespValue.OK).toString(Charsets.UTF_8))

    @Test fun `encode bulk`() =
        assertEquals("\$5\r\nhello\r\n", Resp.encode(RespValue.bulk("hello")).toString(Charsets.UTF_8))

    @Test fun `encode null bulk`() =
        assertEquals("\$-1\r\n", Resp.encode(RespValue.NIL).toString(Charsets.UTF_8))

    @Test fun `encode array`() {
        val arr = RespValue.Array(listOf(RespValue.bulk("a"), RespValue.Integer(1)))
        assertEquals("*2\r\n\$1\r\na\r\n:1\r\n", Resp.encode(arr).toString(Charsets.UTF_8))
    }
}