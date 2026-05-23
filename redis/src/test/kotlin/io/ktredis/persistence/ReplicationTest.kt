package io.ktredis.persistence

import io.ktredis.protocol.Resp
import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import io.ktredis.storage.RedisObject
import java.io.ByteArrayInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReplicationTest {
    private fun b(s: String) = s.toByteArray()

    @Test fun `dumpToBytes and load from stream round-trip (PSYNC frame)`() {
        val db = RedisDatabase()
        db.set("a", RedisObject.StringValue(b("1")))
        db.set("l", RedisObject.ListValue(ArrayDeque(listOf(b("x"), b("y")))))

        val bytes = Rdb.dumpToBytes(db)
        assertTrue(String(bytes, 0, 5) == "REDIS")   // has RDB header

        val loaded = Rdb.load(ByteArrayInputStream(bytes)).associate { it.first to it.second }
        assertEquals("1", String((loaded["a"] as RedisObject.StringValue).data))
        assertEquals(listOf("x", "y"), (loaded["l"] as RedisObject.ListValue).items.map { String(it) })
    }

    @Test fun `RespValue Raw is written as-is`() {
        val raw = byteArrayOf(1, 2, 3, 0, 127, -1)
        assertEquals(raw.toList(), Resp.encode(RespValue.Raw(raw)).toList())
    }
}
