package io.ktredis.server

import io.ktredis.command.CommandDispatcher
import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

class CommandExecutor(private val scope: CoroutineScope) {
    private val db = RedisDatabase()
    private val dispatcher = CommandDispatcher(db)
    private val blocking = BlockingManager(db)
    private val mailbox = Channel<() -> Unit>(Channel.UNLIMITED)
    private val pubsub = PubSub()
    private val watchers = HashMap<String, MutableSet<ClientHandle>>()   // key -> client đang WATCH

    init {
        // single-writer: chạy mọi task tuần tự
        scope.launch {
            for (task in mailbox) task()
        }
        // active expiration
        scope.launch {
            while (isActive) {
                delay(100)
                mailbox.send { db.activeExpireCycle() }
            }
        }
    }

    suspend fun execute(args: List<ByteArray>, client: ClientHandle) {
        if (args.isEmpty()) { client.outgoing.trySend(RespValue.error("ERR empty command")); return }
        val name = args[0].toString(Charsets.UTF_8).uppercase()

        // BLPOP/BRPOP: chờ (suspend) ngay tại connection coroutine, rồi đẩy kết quả ra outgoing
        if (name == "BLPOP" || name == "BRPOP") {
            client.outgoing.trySend(executeBlocking(args, fromHead = name == "BLPOP"))
            return
        }

        mailbox.send { runOnExecutor(name, args, client) }
    }

    private fun runOnExecutor(name: String, args: List<ByteArray>, client: ClientHandle) {
        when (name) {
            "MULTI"   -> { multi(client); return }
            "EXEC"    -> { exec(client); return }
            "DISCARD" -> { discard(client); return }
            "WATCH"   -> { watch(args, client); return }
            "UNWATCH" -> { unwatch(client); return }
        }
        // đang trong MULTI -> xếp hàng (các lệnh điều khiển ở trên đã được xử lý)
        if (client.inMulti) {
            client.queued.add(args)
            client.outgoing.trySend(RespValue.SimpleString("QUEUED"))
            return
        }
        // lệnh thường
        if (name in PUBSUB_COMMANDS) {
            handlePubSub(name, args, client)                  // tự đẩy ack/message/count vào outgoing
            return
        }
        client.outgoing.trySend(dispatchOnly(name, args))     // mọi reply đều đi qua outgoing
    }

    /** Chạy một lệnh (không pub/sub) và TRẢ về kết quả; dùng chung cho lệnh thường lẫn EXEC. */
    private fun dispatchOnly(name: String, args: List<ByteArray>): RespValue {
        val result = try {
            dispatcher.dispatch(args)
        } catch (e: Throwable) {
            RespValue.error("ERR ${e.message}")
        }
        if (name in PUSH_COMMANDS && result is RespValue.Integer) {
            blocking.notifyKey(args[1].toString(Charsets.UTF_8))
        }
        if (name in WRITE_COMMANDS) touchKey(args)            // báo cho client đang WATCH
        return result
    }

    /**
     * BLPOP/BRPOP key [key...] timeout
     * - Nếu có phần tử: pop ngay, trả [key, value].
     * - Nếu rỗng hết: chờ tới khi có push vào một trong các key, hoặc hết timeout (-> nil).
     * Executor KHÔNG bị treo: việc chờ nằm ở connection coroutine (deferred.await), còn task
     * trong mailbox chỉ đăng ký waiter rồi trả về ngay.
     */
    private suspend fun executeBlocking(args: List<ByteArray>, fromHead: Boolean): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments for 'blpop' command")
        val timeoutSec = args.last().toString(Charsets.UTF_8).toDoubleOrNull()
            ?: return RespValue.error("ERR timeout is not a float or out of range")
        if (timeoutSec < 0) return RespValue.error("ERR timeout is negative")
        val keys = (1 until args.size - 1).map { args[it].toString(Charsets.UTF_8) }

        val deferred = CompletableDeferred<Pair<String, ByteArray>?>()
        val waiter = BlockingManager.Waiter(keys, fromHead, deferred)
        mailbox.send { blocking.tryPopOrRegister(waiter) }

        // hẹn giờ timeout (0 = chờ vô hạn). Task timeout cũng đi qua mailbox -> không tranh chấp.
        val timeoutJob = if (timeoutSec > 0) scope.launch {
            delay((timeoutSec * 1000).toLong())
            mailbox.send { blocking.timeout(waiter) }
        } else null

        val result = deferred.await()
        timeoutJob?.cancel()

        return if (result == null) RespValue.Array(null)        // hết giờ -> nil array (*-1)
        else RespValue.Array(listOf(RespValue.bulk(result.first), RespValue.bulk(result.second)))
    }

    private fun handlePubSub(name: String, args: List<ByteArray>, client: ClientHandle) {
        fun s(i: Int) = args[i].toString(Charsets.UTF_8)
        fun ack(kind: String, name: RespValue) = client.outgoing.trySend(
            RespValue.Array(listOf(RespValue.bulk(kind), name, RespValue.int(client.subscriptionCount.toLong())))
        )

        when (name) {
            "SUBSCRIBE" -> for (i in 1 until args.size) {
                pubsub.subscribe(client, s(i)); ack("subscribe", RespValue.bulk(s(i)))
            }

            "PSUBSCRIBE" -> for (i in 1 until args.size) {
                pubsub.psubscribe(client, s(i)); ack("psubscribe", RespValue.bulk(s(i)))
            }

            "UNSUBSCRIBE" -> {
                val cs = if (args.size > 1) (1 until args.size).map { s(it) } else client.channels.toList()
                if (cs.isEmpty()) ack("unsubscribe", RespValue.NIL)
                else cs.forEach { pubsub.unsubscribe(client, it); ack("unsubscribe", RespValue.bulk(it)) }
            }

            "PUNSUBSCRIBE" -> {
                val ps = if (args.size > 1) (1 until args.size).map { s(it) } else client.patterns.toList()
                if (ps.isEmpty()) ack("punsubscribe", RespValue.NIL)
                else ps.forEach { pubsub.punsubscribe(client, it); ack("punsubscribe", RespValue.bulk(it)) }
            }

            "PUBLISH" -> client.outgoing.trySend(RespValue.int(pubsub.publish(s(1), args[2]).toLong()))
        }
    }

    // ---------- transaction ----------
    private fun multi(client: ClientHandle) {
        if (client.inMulti) { client.outgoing.trySend(RespValue.error("ERR MULTI calls can not be nested")); return }
        client.inMulti = true; client.queued.clear(); client.txError = false
        client.outgoing.trySend(RespValue.OK)
    }

    private fun discard(client: ClientHandle) {
        if (!client.inMulti) { client.outgoing.trySend(RespValue.error("ERR DISCARD without MULTI")); return }
        resetTx(client)
        client.outgoing.trySend(RespValue.OK)
    }

    private fun exec(client: ClientHandle) {
        if (!client.inMulti) { client.outgoing.trySend(RespValue.error("ERR EXEC without MULTI")); return }
        if (client.txError) {
            resetTx(client)
            client.outgoing.trySend(RespValue.error("EXECABORT Transaction discarded because of previous errors."))
            return
        }
        if (client.dirty) {                                   // watched key đã đổi -> huỷ
            resetTx(client)
            client.outgoing.trySend(RespValue.Array(null))    // nil array
            return
        }
        val results = client.queued.map { qargs ->
            dispatchOnly(qargs[0].toString(Charsets.UTF_8).uppercase(), qargs)
        }
        resetTx(client)
        client.outgoing.trySend(RespValue.Array(results))     // chạy nguyên khối -> atomic
    }

    private fun watch(args: List<ByteArray>, client: ClientHandle) {
        if (client.inMulti) { client.outgoing.trySend(RespValue.error("ERR WATCH inside MULTI is not allowed")); return }
        for (i in 1 until args.size) {
            val key = args[i].toString(Charsets.UTF_8)
            watchers.getOrPut(key) { HashSet() }.add(client)
            client.watchedKeys.add(key)
        }
        client.outgoing.trySend(RespValue.OK)
    }

    private fun unwatch(client: ClientHandle) {
        clearWatches(client)
        client.outgoing.trySend(RespValue.OK)
    }

    /** Sau mỗi lệnh ghi: đánh dấu dirty cho client đang WATCH key này. */
    private fun touchKey(args: List<ByteArray>) {
        if (args.size < 2) return
        val key = args[1].toString(Charsets.UTF_8)
        watchers[key]?.forEach { it.dirty = true }
    }

    private fun resetTx(client: ClientHandle) {
        client.inMulti = false; client.queued.clear(); client.txError = false
        clearWatches(client)
    }

    private fun clearWatches(client: ClientHandle) {
        client.watchedKeys.forEach { watchers[it]?.remove(client) }
        client.watchedKeys.clear(); client.dirty = false
    }

    suspend fun disconnect(client: ClientHandle) {
        mailbox.send { pubsub.removeClient(client); clearWatches(client) }
    }

    companion object {
        private val PUSH_COMMANDS = setOf("RPUSH", "LPUSH", "RPUSHX", "LPUSHX")
        private val PUBSUB_COMMANDS = setOf("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH")
        private val WRITE_COMMANDS = setOf(
            "SET", "DEL", "EXPIRE", "PEXPIRE", "PERSIST", "RENAME", "INCR", "DECR", "INCRBY", "DECRBY",
            "APPEND", "SETRANGE", "MSET",
            "RPUSH", "LPUSH", "RPUSHX", "LPUSHX", "LPOP", "RPOP", "LSET",
            "HSET", "HDEL", "HINCRBY",
            "SADD", "SREM", "SPOP", "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
            "ZADD", "ZREM", "ZINCRBY"
        )
    }
}
