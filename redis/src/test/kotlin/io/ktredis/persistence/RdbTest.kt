package io.ktredis.persistence

import io.ktredis.storage.RedisDatabase
import io.ktredis.storage.RedisObject
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class RdbTest {
    private fun b(s: String) = s.toByteArray()

    @Test fun `round-trip moi kieu du lieu`() {
        val db = RedisDatabase()
        db.set("str", RedisObject.StringValue(b("hello")))
        db.set("lst", RedisObject.ListValue(ArrayDeque(listOf(b("a"), b("b"), b("c")))))
        db.set("st", RedisObject.SetValue(LinkedHashSet(listOf("x", "y"))))
        db.set("hsh", RedisObject.HashValue(LinkedHashMap<String, ByteArray>().apply {
            put("f1", b("v1")); put("f2", b("v2"))
        }))
        db.set("zs", RedisObject.SortedSetValue().apply { add("an", 100.0); add("bo", 90.0) })

        val f = File.createTempFile("dump", ".rdb").apply { deleteOnExit() }
        Rdb.save(db, f)
        val loaded = Rdb.load(f).associate { it.first to it.second }

        assertEquals(5, loaded.size)
        assertEquals("hello", String((loaded["str"] as RedisObject.StringValue).data))
        assertEquals(listOf("a", "b", "c"), (loaded["lst"] as RedisObject.ListValue).items.map { String(it) })
        assertEquals(setOf("x", "y"), (loaded["st"] as RedisObject.SetValue).members)

        val h = (loaded["hsh"] as RedisObject.HashValue).fields
        assertEquals("v1", String(h["f1"]!!))
        assertEquals("v2", String(h["f2"]!!))

        // zset phải đúng thứ tự theo score: bo(90) trước an(100)
        assertEquals(
            listOf(90.0 to "bo", 100.0 to "an"),
            (loaded["zs"] as RedisObject.SortedSetValue).ascending()
        )
    }

    @Test fun `giu TTL tuyet doi qua save-load`() {
        val db = RedisDatabase()
        val future = System.currentTimeMillis() + 100_000
        db.restore("k", RedisObject.StringValue(b("v")), future)

        val f = File.createTempFile("dump", ".rdb").apply { deleteOnExit() }
        Rdb.save(db, f)
        val triple = Rdb.load(f).first { it.first == "k" }
        assertEquals(future, triple.third)
    }
}
