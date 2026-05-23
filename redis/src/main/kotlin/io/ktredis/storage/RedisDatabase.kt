package io.ktredis.storage

class RedisDatabase(private val clock: Clock = SystemClock){
    private val map = HashMap<String, RedisObject>()
    private val expires = HashMap<String, Long>()

    // ---------- lazy expiration ----------
    private fun expireIfNeeded(key: String) {
        val at = expires[key] ?: return
        if (clock.now() >= at) {
            map.remove(key)
            expires.remove(key)
        }
    }

    fun get(key: String): RedisObject? {
        expireIfNeeded(key)
        return map[key]
    }

    fun set(key: String, value: RedisObject) {
        map[key] = value
        expires.remove(key)
    }

    fun delete(key: String): Boolean {
        expireIfNeeded(key)
        expires.remove(key)
        return map.remove(key) != null
    }

    fun exists(key: String): Boolean {
        expireIfNeeded(key)
        return map.containsKey(key)
    }

    val size: Int get() = map.size

    // ---------- TTL ----------
    fun setExpire(key: String, ttlMillis: Long): Boolean {
        expireIfNeeded(key)
        if (!map.containsKey(key)) return false
        expires[key] = clock.now() + ttlMillis
        return true
    }

    fun pttl(key: String): Long {
        expireIfNeeded(key)
        if (!map.containsKey(key)) return -2
        val at = expires[key] ?: return -1
        return (at - clock.now()).coerceAtLeast(0)
    }

    fun persist(key: String): Boolean {
        expireIfNeeded(key)
        if (!map.containsKey(key)) return false
        return expires.remove(key) != null
    }

    // ---------- keyspace ----------
    /** Danh sách key còn sống (đã dọn key hết hạn trước khi liệt kê). */
    fun keys(): List<String> {
        map.keys.toList().forEach { expireIfNeeded(it) }
        return map.keys.toList()
    }

    fun clear() {
        map.clear()
        expires.clear()
    }

    /** Đổi tên key, mang theo TTL. Trả false nếu src không tồn tại. */
    fun rename(src: String, dst: String): Boolean {
        expireIfNeeded(src)
        val value = map[src] ?: return false
        val ttl = expires[src]
        map.remove(src); expires.remove(src)
        map[dst] = value
        if (ttl != null) expires[dst] = ttl else expires.remove(dst)
        return true
    }

    /** Ghi value nhưng GIỮ TTL hiện có (cho INCR/APPEND/SETRANGE...). */
    fun setKeepTtl(key: String, value: RedisObject) {
        map[key] = value
    }

    // ---------- active expiration ----------
    fun activeExpireCycle(sampleSize: Int = 20) {
        if (expires.isEmpty()) return
        val now = clock.now()
        val sample = expires.keys.take(sampleSize)
        for (key in sample) {
            val at = expires[key] ?: continue
            if (now >= at) {
                map.remove(key)
                expires.remove(key)
            }
        }
    }

    fun expireAt(key: String): Long? = expires[key]

    fun restore(key: String, value: RedisObject, expireAtMillis: Long?) {
        map[key] = value
        if (expireAtMillis != null) expires[key] = expireAtMillis else expires.remove(key)
    }

    fun allEntries(): Map<String, RedisObject> {
        map.keys.toList().forEach { expireIfNeeded(it) }
        return map.toMap()
    }
}