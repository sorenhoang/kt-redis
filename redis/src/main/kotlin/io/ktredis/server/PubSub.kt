package io.ktredis.server

import io.ktredis.command.globMatch
import io.ktredis.protocol.RespValue

class PubSub {
    private val channels = HashMap<String, MutableSet<ClientHandle>>()
    private val patterns = HashMap<String, MutableSet<ClientHandle>>()

    fun subscribe(client: ClientHandle, channel: String) {
        channels.getOrPut(channel) { HashSet() }.add(client)
        client.channels.add(channel)
    }

    fun unsubscribe(client: ClientHandle, channel: String) {
        channels[channel]?.remove(client)
        if (channels[channel]?.isEmpty() == true) channels.remove(channel)
        client.channels.remove(channel)
    }

    fun psubscribe(client: ClientHandle, pattern: String) {
        patterns.getOrPut(pattern) { HashSet() }.add(client)
        client.patterns.add(pattern)
    }

    fun punsubscribe(client: ClientHandle, pattern: String) {
        patterns[pattern]?.remove(client)
        if (patterns[pattern]?.isEmpty() == true) patterns.remove(pattern)
        client.patterns.remove(pattern)
    }

    fun publish(channel: String, message: ByteArray): Int {
        var n = 0

        channels[channel]?.toList()?.forEach { c ->
            c.outgoing.trySend(RespValue.Array(listOf(
                RespValue.bulk("message"), RespValue.bulk(channel), RespValue.bulk(message)
            )))
            n++
        }

        patterns.forEach { (pat, subs) ->
            if (globMatch(pat, channel)) subs.toList().forEach { c ->
                c.outgoing.trySend(RespValue.Array(listOf(
                    RespValue.bulk("pmessage"), RespValue.bulk(pat),
                    RespValue.bulk(channel), RespValue.bulk(message)
                )))
                n++
            }
        }
        return n
    }

    fun removeClient(client: ClientHandle) {
        client.channels.toList().forEach { unsubscribe(client, it) }
        client.patterns.toList().forEach { punsubscribe(client, it) }
    }
}