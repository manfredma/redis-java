# Redis Java 实现 — 设计规范

**日期：** 2026-05-01  
**目标：** 用 Maven + Java 8 实现完整 Redis，与原始 Redis 在协议兼容性、内部数据结构和算法上保持一致。

---

## 1. 项目范围

| 维度 | 要求 |
|------|------|
| 协议兼容 | redis-cli 及任意 Redis 客户端可直接连接使用，命令行为与官方完全一致 |
| 数据结构 | 对照 Redis C 源码实现等价数据结构（SDS、Dict、ListPack、QuickList、IntSet、ZSkipList） |
| 持久化 | RDB + AOF 字节级兼容官方格式，可互相读取文件 |
| 集群高可用 | 主从复制（PSYNC2）+ Sentinel + Redis Cluster |
| 技术栈 | Java 8，纯 NIO 事件循环，主流工具库（Lombok、Logback、JUnit 5、Mockito） |

---

## 2. 实现阶段

```
Phase 1: 单机核心       — 事件循环 + RESP2/3 + 5大数据结构 + 基础命令
Phase 2: 持久化         — RDB（字节级兼容）+ AOF（字节级兼容）
Phase 3: 高级特性       — Stream、Pub/Sub、Lua 脚本、事务、慢查询日志
Phase 4: 主从复制       — PSYNC2 协议、增量/全量重同步、复制缓冲区
Phase 5: Sentinel       — 哨兵协议、SDOWN/ODOWN、Raft 领头选举、故障转移
Phase 6: Cluster        — Gossip 协议、16384 槽、MOVED/ASK 重定向、槽迁移
```

---

## 3. Maven 多模块结构

```
redis-java/
├── pom.xml                    # 父 pom，Java 8，统一依赖管理
├── redis-core/                # 数据结构：SDS、Dict、ListPack、QuickList、IntSet、ZSkipList、RedisObject
├── redis-server/              # 事件循环、命令表、网络层、RedisServer 主入口
├── redis-persistence/         # RDB + AOF 实现
├── redis-replication/         # 主从复制（PSYNC2）
├── redis-sentinel/            # Sentinel 独立进程
├── redis-cluster/             # Redis Cluster
└── redis-test/                # 集成测试（对比官方 Redis 行为）
```

---

## 4. 核心数据结构

对照 Redis 源码，每个 C 结构体对应一个 Java 类：

| Redis C 结构 | Java 实现类 | 源文件 | 说明 |
|---|---|---|---|
| `sds` | `Sds.java` | `sds.c` | 动态字符串，含 len/alloc/flags |
| `dict` | `Dict.java` | `dict.c` | 哈希表，渐进式 rehash，两个 dictht |
| `listpack` | `ListPack.java` | `listpack.c` | 紧凑列表（替代 ziplist） |
| `quicklist` | `QuickList.java` | `quicklist.c` | 双向链表 + listpack 节点 |
| `intset` | `IntSet.java` | `intset.c` | 整数集合，升级机制 |
| `zskiplist` | `ZSkipList.java` | `t_zset.c` | 跳跃表，最多 64 层 |
| `redisObject` | `RedisObject.java` | `object.c` | 统一对象头：type/encoding/lru/refcount/ptr |

### 编码转换规则（与 Redis 完全一致）

| 类型 | 编码转换路径 | 阈值 |
|------|------------|------|
| String | INT → EMBSTR（≤44字节）→ RAW | — |
| List | LISTPACK → QUICKLIST | 元素>128 或单元素>64字节 |
| Hash | LISTPACK → HT | 元素>128 或 key/value>64字节 |
| Set | INTSET / LISTPACK → HT | 元素>128 或含非整数 |
| ZSet | LISTPACK → SKIPLIST | 元素>128 或 member>64字节 |

### RedisObject 内存布局

```
4bit type | 4bit encoding | 24bit lru | 4byte refcount | 8byte ptr
```

---

## 5. 网络层与事件循环

### 事件循环（`AeEventLoop.java`，对照 `ae.c`）

- 用 Java NIO `Selector` 模拟 `aeApiPoll`（epoll/kqueue）
- 单线程处理所有 IO 事件，与 Redis 完全一致
- `FileEvent`（读/写）+ `TimeEvent`（定时任务）
- `serverCron` 每 100ms 触发：过期键清理、RDB/AOF 检查、统计更新、客户端超时

### RESP 协议（`RespDecoder.java` / `RespEncoder.java`）

- 完整实现 RESP2 + RESP3（`HELLO` 命令切换协议版本）
- 内联命令支持（telnet 直连）
- 流式解析，正确处理半包/粘包

### 连接管理（`RedisClient.java`，对照 `client` 结构体）

- querybuf、argc/argv、cmd、flags、db、authenticated
- 输出缓冲区：固定 buf（16KB）+ 动态 reply list（对照 `client.buf` + `client.reply`）
- 支持 `CLIENT LIST`、`CLIENT KILL`、`CLIENT SETNAME`、`CLIENT ID` 等命令

### 线程模型

| 线程 | 职责 | 对照 Redis |
|------|------|------------|
| 主线程 | 事件循环（单线程，零锁） | `aeMain` |
| BIO 线程 1 | close fd | `bio.c` BIO_CLOSE_FILE |
| BIO 线程 2 | AOF fsync | `bio.c` BIO_AOF_FSYNC |
| BIO 线程 3 | lazy free | `bio.c` BIO_LAZY_FREE |

---

## 6. 命令系统

### 命令注册（注解驱动，对照 `redisCommandTable`）

```java
@RedisCommand(name="set", arity=-3, flags="write deny-oom",
              firstKey=1, lastKey=1, step=1)
public class SetCommand implements Command {
    RedisObject execute(RedisClient client, RedisObject[] argv);
}
```

### 命令执行流程（对照 `processCommand`）

1. 查找命令表 → 检查 arity → 检查 requirepass
2. 检查 maxmemory → 检查持久化错误（`MISCONF`）
3. 事务队列检查（MULTI/EXEC）
4. 执行命令 → 传播到 AOF + replicas
5. 慢查询日志记录（slowlog）

### 命令覆盖范围

| 类别 | 主要命令 |
|------|---------|
| String | GET SET MGET MSET INCR DECR INCRBY DECRBY APPEND GETRANGE SETRANGE SETNX GETSET GETDEL GETEX STRLEN |
| List | LPUSH RPUSH LPOP RPOP LRANGE LINDEX LINSERT LLEN LSET LREM LMOVE LMPOP BLPOP BRPOP BLMOVE |
| Hash | HGET HSET HMGET HMSET HDEL HEXISTS HLEN HKEYS HVALS HGETALL HINCRBY HINCRBYFLOAT HSCAN HRANDFIELD |
| Set | SADD SREM SMEMBERS SISMEMBER SMISMEMBER SCARD SINTER SUNION SDIFF SINTERSTORE SUNIONSTORE SDIFFSTORE SRANDMEMBER SPOP SMOVE SSCAN |
| ZSet | ZADD ZRANGE ZRANGEBYLEX ZRANGEBYSCORE ZRANGEBYRANK ZREVRANGE ZRANK ZREVRANK ZSCORE ZMSCORE ZREM ZREMRANGEBYLEX ZREMRANGEBYSCORE ZREMRANGEBYRANK ZCARD ZINCRBY ZUNIONSTORE ZINTERSTORE ZDIFFSTORE ZPOPMIN ZPOPMAX BZPOPMIN BZPOPMAX ZRANDMEMBER ZSCAN ZRANGESTORE |
| 通用 | DEL UNLINK KEYS SCAN TYPE RENAME RENAMENX TTL PTTL EXPIRETIME PEXPIRETIME EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST EXISTS OBJECT WAIT DUMP RESTORE SORT OBJECT TOUCH OBJECT ENCODING OBJECT REFCOUNT OBJECT IDLETIME OBJECT FREQ |
| Server | INFO CONFIG GET/SET/REWRITE/RESETSTAT DBSIZE FLUSHDB FLUSHALL DEBUG SELECT MOVE COPY COMMAND COMMAND COUNT COMMAND INFO SLOWLOG LATENCY TIME MEMORY USAGE MEMORY DOCTOR RESET |
| Stream | XADD XREAD XRANGE XREVRANGE XLEN XTRIM XDEL XINFO XGROUP CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER XREADGROUP XACK XCLAIM XAUTOCLAIM XPENDING |
| Pub/Sub | SUBSCRIBE UNSUBSCRIBE PUBLISH PSUBSCRIBE PUNSUBSCRIBE PUBSUB CHANNELS/NUMSUB/NUMPAT/SHARDCHANNELS/SHARDNUMSUB SSUBSCRIBE SUNSUBSCRIBE SPUBLISH |
| 事务 | MULTI EXEC DISCARD WATCH UNWATCH |
| 脚本 | EVAL EVALSHA EVALRO EVALSHARO SCRIPT LOAD/EXISTS/FLUSH/DEBUG FCALL FCALL_RO FUNCTION LOAD/DELETE/LIST/DUMP/RESTORE/FLUSH/STATS |
| Geo | GEOADD GEODIST GEOPOS GEOHASH GEOSEARCH GEOSEARCHSTORE GEORADIUS GEORADIUSBYMEMBER |
| HyperLogLog | PFADD PFCOUNT PFMERGE |

---

## 7. 持久化

### RDB（对照 `rdb.c`，字节级兼容官方 Redis 7.x）

- 魔数 `REDIS0011`（RDB version 11）
- 编码：LZF 压缩字符串、整数编码（RDB_ENC_INT8/16/32）、各数据结构原生序列化
- `BGSAVE`：独立线程模拟 fork，COW 用快照替代
- 加载时完全兼容官方 RDB 文件（可互读）

### AOF（对照 `aof.c`，字节级兼容）

- fsync 策略：`always` / `everysec`（默认）/ `no`
- AOF 重写：`BGREWRITEAOF`，独立线程生成，重写期间增量追加到缓冲区
- AOF-RDB 混合持久化（`aof-use-rdb-preamble yes`）

### 启动加载顺序

1. AOF 开启 → 优先加载 AOF
2. 否则加载 RDB
3. 都没有 → 空库启动

---

## 8. 主从复制

对照 `replication.c`：

- **PSYNC2 协议**：`replid` + `repl_offset` 实现部分重同步
- **全量同步**：主库后台生成 RDB 发给从库，期间写命令缓存到复制缓冲区
- **增量同步**：断线重连用 `repl_backlog`（环形缓冲区，默认 1MB）补齐
- **命令传播**：每条写命令同步传播到所有从库
- 完整支持 `INFO replication`、`REPLICAOF`、`SLAVEOF`、`WAIT`

---

## 9. Sentinel

对照 `sentinel.c`，独立进程（独立 main 入口）：

- 三条连接：PING 连接 + INFO 连接 + Pub/Sub 订阅连接
- 主观下线（SDOWN）+ 客观下线（ODOWN，quorum 投票）
- Raft 协议选举领头 Sentinel，执行故障转移
- 完整 Sentinel 命令：`SENTINEL masters/slaves/sentinels/failover/reset/get-master-addr-by-name`

---

## 10. Redis Cluster

对照 `cluster.c`：

- 16384 个槽，CRC16 哈希键分配
- Gossip 协议：`MEET` / `PING` / `PONG` / `FAIL` / `UPDATE` 消息
- `MOVED` 和 `ASK` 重定向
- 槽迁移：`CLUSTER SETSLOT MIGRATING/IMPORTING` + `MIGRATE` 命令
- 集群命令：`CLUSTER INFO`、`CLUSTER NODES`、`CLUSTER SLOTS`、`CLUSTER SHARDS`、`CLUSTER MEET`、`CLUSTER FORGET`、`CLUSTER REPLICATE`、`CLUSTER FAILOVER`、`CLUSTER RESET`

---

## 11. 测试策略

| 层级 | 工具 | 说明 |
|------|------|------|
| 单元测试 | JUnit 5 + Mockito | 每个数据结构和命令独立测试 |
| 集成测试 | JUnit 5 + Jedis | 启动本实现，用 Jedis 对比官方 Redis 行为 |
| 兼容性测试 | redis-cli + tcl | 跑官方 Redis 测试套件子集 |
| 持久化兼容测试 | 官方 redis-server | 写入本实现 → 官方加载，反向同样验证 |

---

## 12. 依赖清单

| 依赖 | 版本 | 用途 |
|------|------|------|
| Java | 8 | 运行时 |
| Lombok | 1.18.x | 减少样板代码 |
| Logback + SLF4J | 1.4.x | 日志 |
| JUnit 5 | 5.10.x | 测试框架 |
| Mockito | 5.x | Mock 框架 |
| Jedis | 4.x | 集成测试客户端 |
| LZF Codec | 1.x | RDB LZF 压缩 |
| Luaj | 3.x | Lua 脚本引擎 |
