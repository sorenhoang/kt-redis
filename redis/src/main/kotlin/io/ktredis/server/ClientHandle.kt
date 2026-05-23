package io.ktredis.server

import io.ktredis.protocol.RespValue
import kotlinx.coroutines.channels.Channel

class ClientHandle{
    val outgoing = Channel<RespValue>(Channel.UNLIMITED)
    val channels = HashSet<String>()
    val patterns = HashSet<String>()
    val subscriptionCount: Int get() = channels.size + patterns.size

    // --- transaction ---
    var inMulti = false
    val queued = ArrayList<List<ByteArray>>()
    var txError = false
    val watchedKeys = HashSet<String>()
    var dirty = false

    // --- replication ---
    var isReplica = false                 // connection này đã PSYNC -> là một replica
    var replListeningPort: Int? = null    // cổng replica báo qua REPLCONF listening-port
}