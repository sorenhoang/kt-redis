package io.ktredis

import io.ktredis.server.*
import kotlinx.coroutines.*

fun main()= runBlocking{
    RedisServer().start()
}