package io.ktredis.cluster

import io.ktredis.protocol.RespValue

class ClusterNode(
    val id: String,
    var ip: String,
    var port: Int,
    var cport: Int,                                  // cluster-bus port (convention: port + 10000)
    var epoch: Long,
    val slots: java.util.TreeSet<Int> = java.util.TreeSet(),
    val myself: Boolean = false
)

/**
 * Cluster state for a node: known node table + slot-to-owner map.
 * All operations run on the writer thread (single-threaded cooperative) so no locking is needed.
 */
class ClusterState(myHost: String, myPort: Int) {
    val myId: String = randomId()
    val nodes = LinkedHashMap<String, ClusterNode>()         // id -> node (includes self)
    private val slotOwner = arrayOfNulls<String>(Crc16.SLOTS) // slot -> nodeId
    private val me = ClusterNode(myId, myHost, myPort, myPort + 10000, 0, myself = true)

    init { nodes[myId] = me }

    // ---------- slot ----------
    fun addSlots(slots: List<Int>) {
        me.epoch++                                           // new claim -> higher epoch wins during gossip
        for (s in slots) {
            if (s !in 0 until Crc16.SLOTS) continue
            slotOwner[s]?.let { old -> if (old != myId) nodes[old]?.slots?.remove(s) }
            slotOwner[s] = myId
            me.slots.add(s)
        }
    }

    fun ownerOf(slot: Int): ClusterNode? = slotOwner[slot]?.let { nodes[it] }
    fun isMine(slot: Int): Boolean = slotOwner[slot] == myId
    private fun assignedCount(): Int = slotOwner.count { it != null }
    private fun servingNodes(): Int = nodes.values.count { it.slots.isNotEmpty() }

    fun peers(): List<Pair<String, Int>> = nodes.values.filter { !it.myself }.map { it.ip to it.port }

    fun reset() {
        nodes.clear(); nodes[myId] = me
        me.slots.clear(); me.epoch = 0
        for (i in slotOwner.indices) slotOwner[i] = null
    }

    // ---------- gossip: serialize / merge ----------
    /** Each line: id ip port cport epoch slotRanges (comma-separated or '-' for ranges). */
    fun serialize(): String = buildString {
        for (n in nodes.values) {
            append(n.id).append(' ').append(n.ip).append(' ').append(n.port).append(' ')
            append(n.cport).append(' ').append(n.epoch).append(' ')
            append(if (n.slots.isEmpty()) "-" else rangesString(n.slots))
            append('\n')
        }
    }

    fun mergeFrom(payload: String) {
        for (line in payload.split('\n')) {
            if (line.isBlank()) continue
            val f = line.trim().split(' ')
            if (f.size < 6) continue
            val (id, ip, portS, cportS, epochS) = f
            if (id == myId) continue                          // do not overwrite our own node info
            val port = portS.toIntOrNull() ?: continue
            val epoch = epochS.toLongOrNull() ?: 0L
            val node = nodes.getOrPut(id) { ClusterNode(id, ip, port, cportS.toIntOrNull() ?: port + 10000, epoch) }
            node.ip = ip; node.port = port; node.cport = cportS.toIntOrNull() ?: node.cport
            if (epoch >= node.epoch) {                        // newer (or equal) epoch -> accept slot ownership from this node
                node.epoch = epoch
                node.slots.toList().forEach { if (slotOwner[it] == id) slotOwner[it] = null }
                node.slots.clear()
                for (slot in parseRanges(f[5])) {
                    node.slots.add(slot); slotOwner[slot] = id
                }
            }
        }
    }

    // ---------- replies for CLUSTER commands ----------
    fun info(): String = buildString {
        val ok = assignedCount() == Crc16.SLOTS
        append("cluster_enabled:1\r\n")
        append("cluster_state:${if (ok) "ok" else "fail"}\r\n")
        append("cluster_slots_assigned:${assignedCount()}\r\n")
        append("cluster_known_nodes:${nodes.size}\r\n")
        append("cluster_size:${servingNodes()}\r\n")
    }

    /** Redis CLUSTER NODES format: <id> <ip:port@cport> <flags> <master> <ping> <pong> <epoch> connected <slots> */
    fun nodesText(): String = buildString {
        for (n in nodes.values) {
            val flags = if (n.myself) "myself,master" else "master"
            append("${n.id} ${n.ip}:${n.port}@${n.cport} $flags - 0 0 ${n.epoch} connected")
            if (n.slots.isNotEmpty()) append(' ').append(rangesString(n.slots).replace(',', ' '))
            append('\n')
        }
    }

    fun slotsReply(): RespValue {
        val entries = ArrayList<RespValue>()
        for (n in nodes.values) {
            for ((start, end) in ranges(n.slots)) {
                entries.add(
                    RespValue.Array(
                        listOf(
                            RespValue.int(start.toLong()), RespValue.int(end.toLong()),
                            RespValue.Array(listOf(RespValue.bulk(n.ip), RespValue.int(n.port.toLong()), RespValue.bulk(n.id)))
                        )
                    )
                )
            }
        }
        return RespValue.Array(entries)
    }

    companion object {
        private fun randomId(): String {
            val c = "0123456789abcdef"
            return buildString { repeat(40) { append(c.random()) } }
        }

        /** Collapses discrete slot numbers into contiguous ranges [start,end]. */
        private fun ranges(slots: java.util.TreeSet<Int>): List<Pair<Int, Int>> {
            val out = ArrayList<Pair<Int, Int>>()
            var start = -1; var prev = -2
            for (s in slots) {
                if (s != prev + 1) { if (start >= 0) out.add(start to prev); start = s }
                prev = s
            }
            if (start >= 0) out.add(start to prev)
            return out
        }

        private fun rangesString(slots: java.util.TreeSet<Int>): String =
            ranges(slots).joinToString(",") { (a, b) -> if (a == b) "$a" else "$a-$b" }

        private fun parseRanges(spec: String): List<Int> {
            if (spec == "-" || spec.isBlank()) return emptyList()
            val out = ArrayList<Int>()
            for (part in spec.split(',')) {
                val dash = part.indexOf('-')
                if (dash > 0) {
                    val a = part.substring(0, dash).toIntOrNull() ?: continue
                    val b = part.substring(dash + 1).toIntOrNull() ?: continue
                    for (s in a..b) out.add(s)
                } else part.toIntOrNull()?.let { out.add(it) }
            }
            return out
        }
    }
}
