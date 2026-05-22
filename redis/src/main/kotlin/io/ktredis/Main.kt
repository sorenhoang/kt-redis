package io.ktredis

import io.ktredis.server.RedisServer
import kotlinx.coroutines.*

fun main()= runBlocking{
    RedisServer().start()
}