package io.ktredis.storage

sealed interface RedisObject {
    class StringValue(var data: ByteArray) : RedisObject
}