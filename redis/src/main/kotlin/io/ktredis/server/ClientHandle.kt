package io.ktredis.server

import io.ktredis.protocol.RespValue
import kotlinx.coroutines.channels.Channel

class ClientHandle{
    val outgoing = Channel<RespValue>(Channel.UNLIMITED)
    val channels = HashSet<String>()
    val patterns = HashSet<String>()
    val subscriptionCount: Int get() = channels.size + patterns.size
}