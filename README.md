# kt-redis

A single-node **Redis clone written in Kotlin**, built step by step from a raw TCP
accept loop up to persistence, replication, and clustering. It speaks the **RESP2**
wire protocol, so you can talk to it with the standard `redis-cli`.

> Educational project. The goal is to understand how Redis works internally by
> re-implementing it — not to be a drop-in production server.

## Highlights

- **RESP2 protocol** — binary-safe parser/serializer; works with `redis-cli`.
- **Single-writer concurrency** — many coroutines handle connection I/O, but every
  command is applied to the keyspace on **one** writer coroutine (an actor reading
  from a `Channel`). This makes each command atomic without locks, mirroring Redis's
  single-threaded execution model.
- **All core data types** — strings, lists, hashes, sets, and sorted sets.
- **Key expiration** — lazy + active expiration, testable via an injectable `Clock`.
- **Blocking commands** — `BLPOP` / `BRPOP` suspend the connection coroutine.
- **Pub/Sub** — channels and pattern subscriptions.
- **Transactions** — `MULTI` / `EXEC` / `DISCARD` with optimistic locking via `WATCH`.
- **Persistence** — append-only file (AOF) and binary RDB snapshots.
- **Replication** — master/replica with full resync + live command propagation.
- **Cluster** — 16384 hash slots, gossip-based topology discovery, and `MOVED` redirects.

## Architecture

```
client ──TCP──> RespReader ──args──> CommandExecutor (mailbox / single writer)
                                          │
                                          ├── CommandDispatcher ──> RedisDatabase (keyspace)
                                          ├── AOF append          (persistence)
                                          └── replica propagation (replication)
```

Connection coroutines only read/write sockets. They hand decoded commands to the
`CommandExecutor`, which serializes all keyspace mutations through a single coroutine.
Replies travel back out through a per-connection outgoing `Channel`. AOF appends and
replica propagation happen at the same single-writer point, so ordering is guaranteed.

## Project structure

```
redis/src/main/kotlin/io/ktredis/
├── Main.kt                 # entry point + CLI arg parsing
├── protocol/               # RESP2 reader / writer
├── storage/                # RedisDatabase, RedisObject, Clock (keyspace + TTL)
├── command/                # CommandDispatcher (all command logic) + glob matching
├── server/                 # TCP server, executor, pub/sub, blocking, replication
├── persistence/            # AOF + RDB
└── cluster/                # CRC16 slot hashing, cluster state, gossip
```

## Requirements

- JDK 17+
- A `redis-cli` binary for manual testing (`brew install redis` / `apt install redis-tools`)

## Build & test

All Gradle commands run from the `redis/` directory:

```bash
cd redis
./gradlew test            # run the unit test suite
./gradlew installDist     # produce a runnable distribution
```

The launcher script is then at `build/install/redis/bin/redis`.

## Running

```bash
# default: listen on 127.0.0.1:6379
./gradlew run

# or from the installed distribution, with options
./build/install/redis/bin/redis --port 6380
./build/install/redis/bin/redis --replicaof 127.0.0.1 6379
./build/install/redis/bin/redis --port 7001 --cluster-enabled yes
```

Talk to it with `redis-cli`:

```bash
redis-cli -p 6379 ping            # PONG
redis-cli -p 6379 set foo bar     # OK
redis-cli -p 6379 get foo         # "bar"
```

## Supported commands

| Group         | Commands |
|---------------|----------|
| Connection    | `PING`, `ECHO` |
| Keyspace      | `DEL`, `EXISTS`, `KEYS`, `SCAN`, `TYPE`, `DBSIZE`, `FLUSHDB`, `RENAME` |
| Expiration    | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` (plus `SET ... EX/PX`) |
| Strings       | `SET`, `GET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `MSET`, `MGET` |
| Lists         | `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LPOP`, `RPOP`, `LREM`, `LTRIM`, `BLPOP`, `BRPOP` |
| Hashes        | `HSET`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT` |
| Sets          | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SUNION`, `SDIFF` (+ `*STORE`) |
| Sorted sets   | `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZINCRBY`, `ZCARD`, `ZCOUNT` |
| Pub/Sub       | `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH` |
| Transactions  | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| Persistence   | `SAVE`, `BGSAVE` |
| Replication   | `REPLICAOF` / `SLAVEOF`, `REPLCONF`, `PSYNC`, `WAIT`, `INFO replication` |
| Cluster       | `CLUSTER MEET / ADDSLOTS / NODES / SLOTS / INFO / KEYSLOT / MYID` |

## Persistence

Two mechanisms; AOF takes priority on startup if present:

- **AOF** (`appendonly.aof`) — every write command is appended in RESP format and
  replayed on startup. The fsync policy is configurable (`always` / `everysec` / `no`).
- **RDB** (`dump.rdb`) — a compact binary snapshot written by `SAVE` / `BGSAVE` and
  loaded at startup. The reader also understands the integer-encoded strings used by
  real Redis dumps for string keys.

## Replication

Start a replica with `--replicaof <host> <port>`. The replica performs the standard
handshake (`PING` → `REPLCONF` → `PSYNC ? -1`), receives a full RDB snapshot from the
master, then applies the live stream of write commands. Replicas reject direct client
writes (`-READONLY`). Inspect status with `INFO replication`.

```bash
# terminal 1 — master
( cd /tmp/m && redis --port 6379 )
# terminal 2 — replica (separate working dir for its own data files)
( cd /tmp/r && redis --port 6380 --replicaof 127.0.0.1 6379 )

redis-cli -p 6379 set x 1     # write on master
redis-cli -p 6380 get x       # "1" — propagated to the replica
```

## Cluster

Enable with `--cluster-enabled yes`. Each node owns a subset of the 16384 hash slots
(`slot = CRC16(key) % 16384`, with `{hash tag}` support). Nodes discover each other via
`CLUSTER MEET` and a periodic gossip exchange; a key that doesn't belong to the queried
node returns a `MOVED` redirect, which `redis-cli -c` follows automatically.

```bash
redis-cli -p 7001 cluster meet 127.0.0.1 7002
redis-cli -p 7001 cluster addslots $(seq 0 8191)
redis-cli -p 7002 cluster addslots $(seq 8192 16383)
redis-cli -c -p 7001 set foo bar     # redirected to the owning node, then OK
```

## Limitations

This is a learning implementation, so several real-Redis features are intentionally
simplified or omitted:

- Gossip uses a simplified protocol over the client port rather than Redis's binary
  cluster bus; no failover, replica-of-cluster nodes, or live slot migration (`ASK`).
- AOF logs write commands verbatim — non-deterministic commands (`SPOP`, relative
  `EXPIRE`) are not rewritten to deterministic forms.
- Single logical database (no `SELECT` across multiple DBs), RESP2 only, no ACL/auth.
- Multi-key commands are not validated for cross-slot access in cluster mode.

## License

See [LICENSE](LICENSE).
