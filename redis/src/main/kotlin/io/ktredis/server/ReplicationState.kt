package io.ktredis.server

enum class Role { MASTER, REPLICA }

/** Trạng thái replication cấp server (sống trong CommandExecutor). */
class ReplicationState {
    var role: Role = Role.MASTER
    val replId: String = randomHexId()
    var masterReplOffset: Long = 0L

    // master: các connection đã PSYNC
    val replicas: MutableSet<ClientHandle> = LinkedHashSet()

    // replica: thông tin master đang theo
    var masterHost: String? = null
    var masterPort: Int = 0
    var linkUp: Boolean = false

    companion object {
        private fun randomHexId(): String {
            val chars = "0123456789abcdef"
            return buildString { repeat(40) { append(chars.random()) } }
        }
    }
}
