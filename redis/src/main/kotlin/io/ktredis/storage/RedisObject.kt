package io.ktredis.storage

sealed interface RedisObject {
    class StringValue(var data: ByteArray) : RedisObject
    class ListValue(val items: ArrayDeque<ByteArray> = ArrayDeque()) : RedisObject
    class HashValue(val fields: LinkedHashMap<String, ByteArray> = LinkedHashMap()) : RedisObject
}