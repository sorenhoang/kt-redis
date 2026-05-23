package io.ktredis

import io.ktredis.server.RedisServer
import kotlinx.coroutines.*

fun main(args: Array<String>) = runBlocking {
    var port = 6379
    var replicaOf: Pair<String, Int>? = null
    var clusterEnabled = false

    var i = 0
    while (i < args.size) {
        when (args[i].lowercase()) {
            "--port" -> { port = args[i + 1].toInt(); i += 2 }
            "--replicaof", "--slaveof" -> { replicaOf = args[i + 1] to args[i + 2].toInt(); i += 3 }
            "--cluster-enabled" -> { clusterEnabled = args[i + 1].equals("yes", true); i += 2 }
            else -> i++
        }
    }

    RedisServer(port = port, replicaOf = replicaOf, clusterEnabled = clusterEnabled).start()
}