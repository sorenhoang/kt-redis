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
}