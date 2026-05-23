package io.ktredis.server

enum class Role { MASTER, REPLICA }

/** Server-level replication state (lives inside CommandExecutor). */
class ReplicationState {
    var role: Role = Role.MASTER
    val replId: String = randomHexId()
    var masterReplOffset: Long = 0L

    // master: connections that have completed PSYNC
    val replicas: MutableSet<ClientHandle> = LinkedHashSet()

    // replica: info about the master being followed
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
