package io.ktredis

import io.ktredis.server.RedisServer
import kotlinx.coroutines.*

fun main(args: Array<String>) = runBlocking {
    var port = 6379
    var replicaOf: Pair<String, Int>? = null

    var i = 0
    while (i < args.size) {
        when (args[i].lowercase()) {
            "--port" -> { port = args[i + 1].toInt(); i += 2 }
            "--replicaof", "--slaveof" -> { replicaOf = args[i + 1] to args[i + 2].toInt(); i += 3 }
            else -> i++
        }
    }

    RedisServer(port = port, replicaOf = replicaOf).start()
}