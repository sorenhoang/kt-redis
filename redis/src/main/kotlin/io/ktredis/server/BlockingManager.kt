package io.ktredis.server

import io.ktredis.storage.RedisDatabase
import io.ktredis.storage.RedisObject
import kotlinx.coroutines.CompletableDeferred

/**
 * Manages waiting clients (BLPOP/BRPOP).
 * ALL methods here are only called from the executor coroutine (single-writer),
 * so no locking is needed and there is no contention with regular push/pop.
 */
class BlockingManager(private val db: RedisDatabase) {

    class Waiter(
        val keys: List<String>,
        val fromHead: Boolean,                                   // true = BLPOP, false = BRPOP
        val deferred: CompletableDeferred<Pair<String, ByteArray>?>,
    )

    private val waiters = ArrayList<Waiter>()                    // in FIFO order

    /** Tries to pop immediately from the first key that has elements; registers as a waiter if all are empty. */
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

    /** After a push to `key`, delivers the element to the oldest waiter(s) waiting on that key. */
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

    /** Timeout: completes the waiter with null if it has not been served yet. */
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

    /** Oldest still-active (not yet completed) waiter interested in `key`; also removes timed-out waiters. */
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
