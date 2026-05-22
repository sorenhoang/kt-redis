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

        mailbox.send {
            if (name in PUBSUB_COMMANDS) {
                handlePubSub(name, args, client)              // tự đẩy ack/message/count vào outgoing
                return@send
            }
            val result = try {
                dispatcher.dispatch(args)
            } catch (e: Throwable) {
                RespValue.error("ERR ${e.message}")
            }
            if (name in PUSH_COMMANDS && result is RespValue.Integer) {
                blocking.notifyKey(args[1].toString(Charsets.UTF_8))
            }
            client.outgoing.trySend(result)                   // mọi reply đều đi qua outgoing
        }
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

    suspend fun disconnect(client: ClientHandle) {
        mailbox.send { pubsub.removeClient(client) }
    }

    companion object {
        private val PUSH_COMMANDS = setOf("RPUSH", "LPUSH", "RPUSHX", "LPUSHX")
        private val PUBSUB_COMMANDS = setOf("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH")
    }
}
