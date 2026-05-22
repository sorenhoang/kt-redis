package io.ktredis.server

import io.ktredis.command.CommandDispatcher
import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

class CommandExecutor(scope: CoroutineScope) {
    private val db = RedisDatabase()
    private val dispatcher = CommandDispatcher(db)
    private val mailbox = Channel<() -> Unit>(Channel.UNLIMITED)

    init {

        scope.launch {
            for (task in mailbox) task()
        }

        scope.launch {
            while (isActive) {
                delay(100)
                mailbox.send { db.activeExpireCycle() }
            }
        }
    }

    suspend fun execute(args: List<ByteArray>): RespValue {
        val reply = CompletableDeferred<RespValue>()
        mailbox.send {
            val result = try {
                dispatcher.dispatch(args)
            } catch (e: Throwable) {
                RespValue.error("ERR ${e.message}")
            }
            reply.complete(result)
        }
        return reply.await()
    }
}