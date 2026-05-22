package io.ktredis.storage

interface Clock {
    fun now(): Long          // epoch millis
}

object SystemClock : Clock {
    override fun now(): Long = System.currentTimeMillis()
}