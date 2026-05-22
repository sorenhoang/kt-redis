package io.ktredis.storage

sealed interface RedisObject {
    class StringValue(var data: ByteArray) : RedisObject
    class ListValue(val items: ArrayDeque<ByteArray> = ArrayDeque()) : RedisObject
}