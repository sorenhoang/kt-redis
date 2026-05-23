package io.ktredis.server

import io.ktredis.command.CommandDispatcher
import io.ktredis.persistence.Aof
import io.ktredis.persistence.FsyncPolicy
import io.ktredis.persistence.Rdb
import io.ktredis.protocol.Resp
import io.ktredis.protocol.RespValue
import io.ktredis.storage.RedisDatabase
import io.ktredis.storage.RedisObject
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.io.File

class CommandExecutor(
    private val scope: CoroutineScope,
    private val aof: Aof? = null,
    private val rdbPath: String = "dump.rdb",
    private val myPort: Int = 6379
) {
    private var loading = false
    private val db = RedisDatabase()
    private val dispatcher = CommandDispatcher(db)
    private val blocking = BlockingManager(db)
    private val mailbox = Channel<() -> Unit>(Channel.UNLIMITED)
    private val pubsub = PubSub()
    private val watchers = HashMap<String, MutableSet<ClientHandle>>()   // key -> client đang WATCH
    private val repl = ReplicationState()                                // trạng thái replication

    init {
        // single-writer: chạy mọi task tuần tự
        scope.launch {
            for (task in mailbox) task()
        }
        // fsync nền cho chính sách everysec (đẩy qua mailbox -> luôn chạy trên writer thread)
        if (aof != null && aof.policy == FsyncPolicy.EVERYSEC) {
            scope.launch {
                while (isActive) {
                    delay(1000); mailbox.send { aof.fsync() }
                }
            }
        }
        // active expiration
        scope.launch {
            while (isActive) {
                delay(100)
                mailbox.send { db.activeExpireCycle() }
            }
        }
    }

    suspend fun execute(args: List<ByteArray>, client: ClientHandle) {
        if (args.isEmpty()) {
            client.outgoing.trySend(RespValue.error("ERR empty command")); return
        }
        val name = args[0].toString(Charsets.UTF_8).uppercase()

        // BLPOP/BRPOP: chờ (suspend) ngay tại connection coroutine, rồi đẩy kết quả ra outgoing
        if (name == "BLPOP" || name == "BRPOP") {
            client.outgoing.trySend(executeBlocking(args, fromHead = name == "BLPOP"))
            return
        }

        mailbox.send { runOnExecutor(name, args, client) }
    }

    private fun runOnExecutor(name: String, args: List<ByteArray>, client: ClientHandle) {
        when (name) {
            "MULTI" -> {
                multi(client); return
            }

            "EXEC" -> {
                exec(client); return
            }

            "DISCARD" -> {
                discard(client); return
            }

            "WATCH" -> {
                watch(args, client); return
            }

            "UNWATCH" -> {
                unwatch(client); return
            }

            "SAVE" -> {
                saveRdb(); client.outgoing.trySend(RespValue.OK); return
            }

            "BGSAVE" -> {
                saveRdb(); client.outgoing.trySend(RespValue.SimpleString("Background saving started")); return
            }

            "REPLCONF" -> {
                handleReplconf(args, client); return
            }

            "PSYNC" -> {
                handlePsync(client); return
            }

            "INFO" -> {
                client.outgoing.trySend(RespValue.bulk(infoReplication())); return
            }

            "WAIT" -> {
                client.outgoing.trySend(RespValue.int(repl.replicas.size.toLong())); return
            }

            "REPLICAOF", "SLAVEOF" -> {
                handleReplicaOf(args, client); return
            }
        }
        // đang trong MULTI -> xếp hàng (các lệnh điều khiển ở trên đã được xử lý)
        if (client.inMulti) {
            client.queued.add(args)
            client.outgoing.trySend(RespValue.SimpleString("QUEUED"))
            return
        }
        // lệnh thường
        if (name in PUBSUB_COMMANDS) {
            handlePubSub(name, args, client)                  // tự đẩy ack/message/count vào outgoing
            return
        }
        // replica chỉ đọc: từ chối lệnh ghi đến từ client thường (lệnh từ master đi đường applyReplicated)
        if (repl.role == Role.REPLICA && name in WRITE_COMMANDS) {
            client.outgoing.trySend(RespValue.error("READONLY You can't write against a read only replica."))
            return
        }
        client.outgoing.trySend(dispatchOnly(name, args))     // mọi reply đều đi qua outgoing
    }

    private fun dispatchOnly(name: String, args: List<ByteArray>): RespValue {
        val result = try {
            dispatcher.dispatch(args)
        } catch (e: Throwable) {
            RespValue.error("ERR ${e.message}")
        }
        if (name in PUSH_COMMANDS && result is RespValue.Integer) {
            blocking.notifyKey(args[1].toString(Charsets.UTF_8))
        }

        if (name in WRITE_COMMANDS) {
            touchKey(args)
            if (!loading && result !is RespValue.Error) {
                aof?.append(args)                              // bền vững cục bộ
                if (repl.role == Role.MASTER) propagate(args)  // truyền xuống các replica
            }
        }
        return result
    }

    private suspend fun executeBlocking(args: List<ByteArray>, fromHead: Boolean): RespValue {
        if (args.size < 3) return RespValue.error("ERR wrong number of arguments for 'blpop' command")
        val timeoutSec = args.last().toString(Charsets.UTF_8).toDoubleOrNull()
            ?: return RespValue.error("ERR timeout is not a float or out of range")
        if (timeoutSec < 0) return RespValue.error("ERR timeout is negative")
        val keys = (1 until args.size - 1).map { args[it].toString(Charsets.UTF_8) }

        val deferred = CompletableDeferred<Pair<String, ByteArray>?>()
        val waiter = BlockingManager.Waiter(keys, fromHead, deferred)
        mailbox.send { blocking.tryPopOrRegister(waiter) }

        // hẹn giờ timeout (0 = chờ vô hạn). Task timeout cũng đi qua mailbox -> không tranh chấp.
        val timeoutJob = if (timeoutSec > 0) scope.launch {
            delay((timeoutSec * 1000).toLong())
            mailbox.send { blocking.timeout(waiter) }
        } else null

        val result = deferred.await()
        timeoutJob?.cancel()

        return if (result == null) RespValue.Array(null)        // hết giờ -> nil array (*-1)
        else RespValue.Array(listOf(RespValue.bulk(result.first), RespValue.bulk(result.second)))
    }

    private fun handlePubSub(name: String, args: List<ByteArray>, client: ClientHandle) {
        fun s(i: Int) = args[i].toString(Charsets.UTF_8)
        fun ack(kind: String, name: RespValue) = client.outgoing.trySend(
            RespValue.Array(listOf(RespValue.bulk(kind), name, RespValue.int(client.subscriptionCount.toLong())))
        )

        when (name) {
            "SUBSCRIBE" -> for (i in 1 until args.size) {
                pubsub.subscribe(client, s(i)); ack("subscribe", RespValue.bulk(s(i)))
            }

            "PSUBSCRIBE" -> for (i in 1 until args.size) {
                pubsub.psubscribe(client, s(i)); ack("psubscribe", RespValue.bulk(s(i)))
            }

            "UNSUBSCRIBE" -> {
                val cs = if (args.size > 1) (1 until args.size).map { s(it) } else client.channels.toList()
                if (cs.isEmpty()) ack("unsubscribe", RespValue.NIL)
                else cs.forEach { pubsub.unsubscribe(client, it); ack("unsubscribe", RespValue.bulk(it)) }
            }

            "PUNSUBSCRIBE" -> {
                val ps = if (args.size > 1) (1 until args.size).map { s(it) } else client.patterns.toList()
                if (ps.isEmpty()) ack("punsubscribe", RespValue.NIL)
                else ps.forEach { pubsub.punsubscribe(client, it); ack("punsubscribe", RespValue.bulk(it)) }
            }

            "PUBLISH" -> client.outgoing.trySend(RespValue.int(pubsub.publish(s(1), args[2]).toLong()))
        }
    }

    // ---------- transaction ----------
    private fun multi(client: ClientHandle) {
        if (client.inMulti) {
            client.outgoing.trySend(RespValue.error("ERR MULTI calls can not be nested")); return
        }
        client.inMulti = true; client.queued.clear(); client.txError = false
        client.outgoing.trySend(RespValue.OK)
    }

    private fun discard(client: ClientHandle) {
        if (!client.inMulti) {
            client.outgoing.trySend(RespValue.error("ERR DISCARD without MULTI")); return
        }
        resetTx(client)
        client.outgoing.trySend(RespValue.OK)
    }

    private fun exec(client: ClientHandle) {
        if (!client.inMulti) {
            client.outgoing.trySend(RespValue.error("ERR EXEC without MULTI")); return
        }
        if (client.txError) {
            resetTx(client)
            client.outgoing.trySend(RespValue.error("EXECABORT Transaction discarded because of previous errors."))
            return
        }
        if (client.dirty) {                                   // watched key đã đổi -> huỷ
            resetTx(client)
            client.outgoing.trySend(RespValue.Array(null))    // nil array
            return
        }
        val results = client.queued.map { qargs ->
            dispatchOnly(qargs[0].toString(Charsets.UTF_8).uppercase(), qargs)
        }
        resetTx(client)
        client.outgoing.trySend(RespValue.Array(results))     // chạy nguyên khối -> atomic
    }

    private fun watch(args: List<ByteArray>, client: ClientHandle) {
        if (client.inMulti) {
            client.outgoing.trySend(RespValue.error("ERR WATCH inside MULTI is not allowed")); return
        }
        for (i in 1 until args.size) {
            val key = args[i].toString(Charsets.UTF_8)
            watchers.getOrPut(key) { HashSet() }.add(client)
            client.watchedKeys.add(key)
        }
        client.outgoing.trySend(RespValue.OK)
    }

    private fun unwatch(client: ClientHandle) {
        clearWatches(client)
        client.outgoing.trySend(RespValue.OK)
    }

    /** Sau mỗi lệnh ghi: đánh dấu dirty cho client đang WATCH key này. */
    private fun touchKey(args: List<ByteArray>) {
        if (args.size < 2) return
        val key = args[1].toString(Charsets.UTF_8)
        watchers[key]?.forEach { it.dirty = true }
    }

    private fun resetTx(client: ClientHandle) {
        client.inMulti = false; client.queued.clear(); client.txError = false
        clearWatches(client)
    }

    private fun clearWatches(client: ClientHandle) {
        client.watchedKeys.forEach { watchers[it]?.remove(client) }
        client.watchedKeys.clear(); client.dirty = false
    }

    suspend fun disconnect(client: ClientHandle) {
        mailbox.send { pubsub.removeClient(client); clearWatches(client); repl.replicas.remove(client) }
    }

    // ---------- replication: master ----------
    /** Đẩy một lệnh ghi xuống mọi replica + tăng offset. Chạy trên writer thread nên thứ tự đảm bảo. */
    private fun propagate(args: List<ByteArray>) {
        val frame = RespValue.Array(args.map { RespValue.bulk(it) })
        repl.masterReplOffset += Resp.encode(frame).size
        for (r in repl.replicas) r.outgoing.trySend(frame)
    }

    private fun handleReplconf(args: List<ByteArray>, client: ClientHandle) {
        val sub = if (args.size > 1) args[1].toString(Charsets.UTF_8).uppercase() else ""
        if (sub == "LISTENING-PORT" && args.size > 2)
            client.replListeningPort = args[2].toString(Charsets.UTF_8).toIntOrNull()
        // GETACK/capa... -> chỉ cần ack
        client.outgoing.trySend(RespValue.OK)
    }

    /** PSYNC ? -1: gửi FULLRESYNC + RDB snapshot, rồi đăng ký connection này làm replica. */
    private fun handlePsync(client: ClientHandle) {
        client.outgoing.trySend(RespValue.SimpleString("FULLRESYNC ${repl.replId} ${repl.masterReplOffset}"))
        val rdb = Rdb.dumpToBytes(db)
        val header = "\$${rdb.size}\r\n".toByteArray()
        client.outgoing.trySend(RespValue.Raw(header + rdb))   // $<len>\r\n<bytes> (không CRLF cuối)
        client.isReplica = true
        repl.replicas.add(client)
        println("replica synced (listening-port=${client.replListeningPort}), sent RDB ${rdb.size} bytes")
    }

    private fun infoReplication(): String = buildString {
        append("# Replication\r\n")
        if (repl.role == Role.MASTER) {
            append("role:master\r\n")
            append("connected_slaves:${repl.replicas.size}\r\n")
            repl.replicas.forEachIndexed { i, c ->
                append("slave$i:ip=127.0.0.1,port=${c.replListeningPort ?: 0},state=online,offset=${repl.masterReplOffset},lag=0\r\n")
            }
        } else {
            append("role:slave\r\n")
            append("master_host:${repl.masterHost}\r\n")
            append("master_port:${repl.masterPort}\r\n")
            append("master_link_status:${if (repl.linkUp) "up" else "down"}\r\n")
        }
        append("master_replid:${repl.replId}\r\n")
        append("master_repl_offset:${repl.masterReplOffset}\r\n")
    }

    /** REPLICAOF NO ONE -> thành master; REPLICAOF host port -> thành replica và kết nối master. */
    private fun handleReplicaOf(args: List<ByteArray>, client: ClientHandle) {
        if (args.size < 3) { client.outgoing.trySend(RespValue.error("ERR wrong number of arguments for 'replicaof'")); return }
        val host = args[1].toString(Charsets.UTF_8)
        val port = args[2].toString(Charsets.UTF_8)
        if (host.equals("no", true) && port.equals("one", true)) {
            repl.role = Role.MASTER; repl.masterHost = null; repl.linkUp = false
            client.outgoing.trySend(RespValue.OK)
            return
        }
        val p = port.toIntOrNull()
        if (p == null) { client.outgoing.trySend(RespValue.error("ERR Invalid master port")); return }
        becomeReplica(host, p)
        ReplicaLink(host, p, myPort, this, scope).start()
        client.outgoing.trySend(RespValue.OK)
    }

    // ---------- replication: replica ----------
    fun becomeReplica(host: String, port: Int) {
        repl.role = Role.REPLICA; repl.masterHost = host; repl.masterPort = port; repl.linkUp = false
    }

    fun setLinkUp(up: Boolean) { repl.linkUp = up }

    /** Thay toàn bộ keyspace bằng snapshot RDB nhận từ master (full resync). */
    suspend fun installSnapshot(entries: List<Triple<String, RedisObject, Long?>>) {
        mailbox.send {
            db.clear()
            val now = System.currentTimeMillis()
            for ((k, v, exp) in entries) {
                if (exp != null && exp <= now) continue
                db.restore(k, v, exp)
            }
        }
    }

    /** Áp dụng một lệnh do master truyền xuống (bỏ qua read-only guard, không re-propagate vì không có sub-replica). */
    suspend fun applyReplicated(args: List<ByteArray>) {
        val name = args[0].toString(Charsets.UTF_8).uppercase()
        mailbox.send {
            dispatchOnly(name, args)
            repl.masterReplOffset += Resp.encode(RespValue.Array(args.map { RespValue.bulk(it) })).size
        }
    }

    suspend fun loadAof() {
        if (aof == null) return
        loading = true
        try {
            for (cmd in aof.loadCommands()) {
                val name = cmd[0].toString(Charsets.UTF_8).uppercase()
                dispatchOnly(name, cmd)
            }
            println("AOF loaded: replayed commands")
        } finally {
            loading = false
        }
    }

    /** Snapshot toàn bộ keyspace ra dump.rdb (chạy trên writer thread -> nhất quán). */
    private fun saveRdb() = Rdb.save(db, File(rdbPath))

    /** Nạp dump.rdb lúc khởi động; bỏ qua key đã hết hạn. Không kích hoạt AOF append (nạp thẳng vào db). */
    fun loadRdb() {
        val now = System.currentTimeMillis()
        var count = 0
        for ((key, value, expireAt) in Rdb.load(File(rdbPath))) {
            if (expireAt != null && expireAt <= now) continue
            db.restore(key, value, expireAt)
            count++
        }
        if (count > 0) println("RDB loaded: $count keys")
    }

    companion object {
        private val PUSH_COMMANDS = setOf("RPUSH", "LPUSH", "RPUSHX", "LPUSHX")
        private val PUBSUB_COMMANDS = setOf("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH")
        private val WRITE_COMMANDS = setOf(
            "SET", "DEL", "EXPIRE", "PEXPIRE", "PERSIST", "RENAME", "INCR", "DECR", "INCRBY", "DECRBY",
            "APPEND", "SETRANGE", "MSET",
            "RPUSH", "LPUSH", "RPUSHX", "LPUSHX", "LPOP", "RPOP", "LSET",
            "HSET", "HDEL", "HINCRBY",
            "SADD", "SREM", "SPOP", "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
            "ZADD", "ZREM", "ZINCRBY"
        )
    }
}
