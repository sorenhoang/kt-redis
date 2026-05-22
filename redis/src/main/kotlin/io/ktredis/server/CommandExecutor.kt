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

    suspend fun execute(args: List<ByteArray>): RespValue {
        if (args.isEmpty()) return RespValue.error("ERR empty command")
        val name = args[0].toString(Charsets.UTF_8).uppercase()
        if (name == "BLPOP" || name == "BRPOP") return executeBlocking(args, fromHead = name == "BLPOP")

        val reply = CompletableDeferred<RespValue>()
        mailbox.send {
            val result = try {
                dispatcher.dispatch(args)
            } catch (e: Throwable) {
                RespValue.error("ERR ${e.message}")
            }
            // sau khi push thành công, đánh thức waiter đang chờ key đó
            if (name in PUSH_COMMANDS && result is RespValue.Integer) {
                blocking.notifyKey(args[1].toString(Charsets.UTF_8))
            }
            reply.complete(result)
        }
        return reply.await()
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

    companion object {
        private val PUSH_COMMANDS = setOf("RPUSH", "LPUSH", "RPUSHX", "LPUSHX")
    }
}
