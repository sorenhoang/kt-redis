package io.ktredis.server

import io.ktredis.storage.RedisDatabase
import io.ktredis.storage.RedisObject
import kotlinx.coroutines.CompletableDeferred

/**
 * Quản lý các client đang chờ (BLPOP/BRPOP).
 * TẤT CẢ method ở đây chỉ được gọi từ coroutine executor (single-writer),
 * nên không cần khoá và không có tranh chấp với việc push/pop thường.
 */
class BlockingManager(private val db: RedisDatabase) {

    class Waiter(
        val keys: List<String>,
        val fromHead: Boolean,                                   // true = BLPOP, false = BRPOP
        val deferred: CompletableDeferred<Pair<String, ByteArray>?>,
    )

    private val waiters = ArrayList<Waiter>()                    // theo thứ tự FIFO

    /** Thử pop ngay từ key đầu tiên còn phần tử; nếu rỗng hết thì đăng ký chờ. */
    fun tryPopOrRegister(waiter: Waiter) {
        for (key in waiter.keys) {
            val v = popFrom(key, waiter.fromHead)
            if (v != null) {
                waiter.deferred.complete(key to v)
                return
            }
        }
        waiters.add(waiter)
    }

    /** Sau khi có push vào `key`, giao phần tử cho (các) waiter cũ nhất đang chờ key đó. */
    fun notifyKey(key: String) {
        while (true) {
            val obj = db.get(key)
            if (obj !is RedisObject.ListValue || obj.items.isEmpty()) return
            val w = nextActiveWaiterFor(key) ?: return
            val v = if (w.fromHead) obj.items.removeFirst() else obj.items.removeLast()
            if (obj.items.isEmpty()) db.delete(key)
            w.deferred.complete(key to v)
            waiters.remove(w)
        }
    }

    /** Hết giờ: hoàn tất waiter bằng null nếu chưa ai phục vụ nó. */
    fun timeout(waiter: Waiter) {
        if (waiter.deferred.complete(null)) waiters.remove(waiter)
    }

    private fun popFrom(key: String, fromHead: Boolean): ByteArray? {
        val obj = db.get(key)
        if (obj !is RedisObject.ListValue || obj.items.isEmpty()) return null
        val v = if (fromHead) obj.items.removeFirst() else obj.items.removeLast()
        if (obj.items.isEmpty()) db.delete(key)
        return v
    }

    /** Waiter cũ nhất còn "sống" (chưa hoàn tất) quan tâm tới `key`; dọn luôn waiter đã hết giờ. */
    private fun nextActiveWaiterFor(key: String): Waiter? {
        val it = waiters.iterator()
        while (it.hasNext()) {
            val w = it.next()
            if (w.deferred.isCompleted) { it.remove(); continue }
            if (key in w.keys) return w
        }
        return null
    }
}
