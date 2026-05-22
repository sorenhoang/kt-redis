package io.ktredis.storage

class RedisDatabase{
    private val map = HashMap<String, RedisObject>()

    fun get(key: String): RedisObject? = map[key]
    fun set(key: String, value: RedisObject) { map[key] = value }
    fun delete(key: String): Boolean = map.remove(key) != null
    fun exists(key: String): Boolean = map.containsKey(key)
    val size: Int get() = map.size
}