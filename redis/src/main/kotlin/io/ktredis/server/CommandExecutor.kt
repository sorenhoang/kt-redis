package io.ktredis.server

import io.ktredis.command.CommandDispatcher
import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

class CommandExecutor(scope: CoroutineScope) {
    private class Request(val args: List<ByteArray>, val reply: CompletableDeferred<RespValue>)

    private val dispatcher = CommandDispatcher(RedisDatabase())
    private val mailbox = Channel<Request>(Channel.UNLIMITED)

    init {
        scope.launch {
            for (req in mailbox) {
                val result = try {
                    dispatcher.dispatch(req.args)
                } catch (e: Throwable) {
                    RespValue.error("ERR ${e.message}")
                }

                req.reply.complete(result)

            }
        }
    }

    suspend fun execute(args: List<ByteArray>): RespValue {
        val reply = CompletableDeferred<RespValue>()
        mailbox.send(Request(args, reply))
        return reply.await()                                  // chờ executor xử lý xong
    }
}