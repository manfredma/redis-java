# Phase 1: Redis 单机核心实现计划

**日期**: 2026-05-01  
**目标**: 用 Maven + Java 8 实现 Redis 单机核心  
**技术栈**: Java 8, 纯 NIO 事件循环, Lombok, Logback, JUnit 5, Mockito, Jedis（集成测试）

---

## 模块结构

```
redis-java/
├── pom.xml                  # 父 pom（packaging=pom）
├── redis-core/              # 核心数据结构
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/redisimpl/core/
│       │   ├── object/      # RedisObject, 类型/编码常量
│       │   ├── sds/         # Sds
│       │   ├── intset/      # IntSet
│       │   ├── listpack/    # ListPack
│       │   ├── dict/        # Dict (渐进式 rehash)
│       │   ├── quicklist/   # QuickList
│       │   └── zskiplist/   # ZSkipList
│       └── test/java/com/redisimpl/core/
├── redis-server/            # 服务端：协议、事件循环、命令
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/redisimpl/server/
│       │   ├── resp/        # RespDecoder, RespEncoder
│       │   ├── ae/          # AeEventLoop, FileEvent, TimeEvent
│       │   ├── client/      # RedisClient
│       │   ├── bio/         # BioThread (3条)
│       │   ├── command/     # @RedisCommand, CommandTable
│       │   ├── commands/    # String/List/Hash/Set/ZSet/Generic/Server 命令
│       │   ├── db/          # RedisDb (多DB支持)
│       │   ├── expire/      # 过期键机制
│       │   └── RedisServer.java
│       └── test/
└── redis-test/              # 集成测试（Jedis）
    ├── pom.xml
    └── src/test/java/com/redisimpl/test/
```

---

## 执行顺序（TDD 模式）

### Step 1: Maven 项目骨架

**任务**: 创建父 pom + 3个子模块骨架

父 pom 依赖管理:
- `org.projectlombok:lombok:1.18.30`
- `ch.qos.logback:logback-classic:1.4.14`
- `org.junit.jupiter:junit-jupiter:5.10.1`
- `org.mockito:mockito-core:5.8.0`
- `redis.clients:jedis:5.1.0`
- Java 8, maven-compiler-plugin 3.11.0
- maven-surefire-plugin 3.2.2（JUnit 5）

**提交**: `chore: initialize Maven multi-module project skeleton`

---

### Step 2: RedisObject

**文件**: `redis-core/src/main/java/com/redisimpl/core/object/`

```java
// 类型常量
OBJ_TYPE_STRING = 0
OBJ_TYPE_LIST   = 1
OBJ_TYPE_SET    = 2
OBJ_TYPE_ZSET   = 3
OBJ_TYPE_HASH   = 4

// 编码常量
OBJ_ENCODING_RAW        = 0  // SDS
OBJ_ENCODING_INT        = 1  // long
OBJ_ENCODING_HT         = 2  // Dict
OBJ_ENCODING_ZIPLIST    = 3  // (legacy)
OBJ_ENCODING_INTSET     = 4  // IntSet
OBJ_ENCODING_SKIPLIST   = 5  // ZSkipList + Dict
OBJ_ENCODING_EMBSTR     = 8  // embstr SDS ≤44字节
OBJ_ENCODING_QUICKLIST  = 9  // QuickList
OBJ_ENCODING_LISTPACK   = 11 // ListPack

// RedisObject 字段
int type
int encoding
int lruClock    // LRU 时钟（24位）
int refcount
Object ptr
```

测试: 类型/编码常量正确，对象创建/引用计数

**提交**: `feat: add RedisObject with type/encoding constants`

---

### Step 3: Sds（简单动态字符串）

**文件**: `redis-core/.../sds/Sds.java`

核心字段:
- `byte[] buf` — 实际字节内容
- `int len` — 已用长度
- `int alloc` — 分配容量

关键方法:
- `static Sds fromString(String s)` / `static Sds fromBytes(byte[] b)`
- `Sds append(byte[] b)` — 返回新 Sds（空间不足时扩容：len<1MB 翻倍，否则+1MB）
- `String toStr()` / `byte[] toBytes()`
- `int length()`, `boolean isEmpty()`
- `Sds sdsgrowzero(int len)` — 扩展并填零
- `Sds sdsrange(int start, int end)` — 子串

测试: 创建、追加、扩容策略、子串、边界

**提交**: `feat: add Sds (Simple Dynamic String)`

---

### Step 4: IntSet（整数集合）

**文件**: `redis-core/.../intset/IntSet.java`

核心:
- 编码: `INTSET_ENC_INT16=2`, `INT32=4`, `INT64=8`
- `byte[] contents` — 小端序紧凑存储
- 支持升级（int16→int32→int64）
- 二分查找

关键方法:
- `IntSet add(long value)` — 返回新 IntSet（可能升级）
- `boolean contains(long value)`
- `IntSet remove(long value)` — 返回新 IntSet
- `long[] toArray()`
- `int length()`
- `IntSet upgrade(int newEncoding)` — 升级编码

阈值: `INTSET_MAX_ENTRIES = 512`（超过则转 HT）

测试: 添加、升级、删除、二分查找、边界值

**提交**: `feat: add IntSet with encoding upgrade`

---

### Step 5: ListPack（紧凑列表）

**文件**: `redis-core/.../listpack/ListPack.java`

结构（对照 Redis listpack.c）:
- 4字节 total-bytes
- 2字节 num-elements  
- N个 entry（prevlen + encoding + data）
- 1字节 0xFF 结束符

Entry 编码:
- 小整数: 7位（0xxxxxxx）, 13位（110xxxxx xxxxxxxx）, 16/24/32/64位整数
- 字节串: 6位长度（10xxxxxx + data），12位长度，32位长度

关键方法:
- `ListPack create()`
- `ListPack append(byte[] element)` / `ListPack prepend(byte[] element)`
- `ListPack insert(int index, byte[] element)`
- `ListPack delete(int index)`
- `byte[] get(int index)`
- `int size()`
- `List<byte[]> toList()`

阈值: `LIST_MAX_LISTPACK_SIZE = 128`，`LIST_MAX_LISTPACK_VALUE = 64`

测试: 创建、追加、插入、删除、遍历、整数编码优化

**提交**: `feat: add ListPack (compact list)`

---

### Step 6: Dict（哈希表，渐进式 rehash）

**文件**: `redis-core/.../dict/Dict.java`

核心（对照 Redis dict.c）:
- 两个 `DictHashTable`（ht[0], ht[1]）
- `rehashidx = -1` 表示未在 rehash
- 渐进式 rehash: 每次操作迁移 1 个桶

关键方法:
- `Dict put(byte[] key, Object value)` — 返回新 Dict（immutable 风格，但内部可变）
- `Object get(byte[] key)`
- `Dict delete(byte[] key)`
- `boolean containsKey(byte[] key)`
- `int size()`
- `void rehashStep()` — 迁移一个桶
- `void dictResize()` — 扩容触发
- `Set<byte[]> keySet()` / `Collection<Object> values()`
- `Iterator<Map.Entry<byte[], Object>> iterator()`

哈希函数: SipHash-1-2（或 MurmurHash2 作为简化）

扩容策略: 使用率 > 100% 触发扩容，目标大小为 size*2 的下一个 2 的幂

测试: put/get/delete、渐进式 rehash、扩容、迭代器

**提交**: `feat: add Dict with incremental rehash`

---

### Step 7: QuickList

**文件**: `redis-core/.../quicklist/QuickList.java`

结构:
- 双向链表，每个节点是 `QuickListNode`
- 每个节点包含一个 `ListPack`
- 节点满时（元素数 > 128 或字节数 > 8KB）拆分

关键方法:
- `QuickList create()`
- `QuickList lpush(byte[] value)` / `QuickList rpush(byte[] value)`
- `byte[] lpop()` / `byte[] rpop()`
- `byte[] index(long idx)` — 支持负索引
- `List<byte[]> range(long start, long stop)`
- `long llen()`
- `QuickList linsert(byte[] pivot, boolean before, byte[] value)`
- `QuickList lset(long index, byte[] value)`
- `long lrem(long count, byte[] value)`
- `QuickList lmove(QuickList dst, boolean srcLeft, boolean dstLeft)`

测试: push/pop、range、insert、set、rem、负索引

**提交**: `feat: add QuickList (doubly-linked list of listpacks)`

---

### Step 8: ZSkipList（跳跃表）

**文件**: `redis-core/.../zskiplist/ZSkipList.java`

结构（对照 Redis t_zset.c）:
- `ZSkipListNode header` — 哑头节点，64层
- `ZSkipListNode tail`
- `int length`
- `int level`（当前最高层）

ZSkipListNode:
- `byte[] ele`
- `double score`
- `ZSkipListNode backward`
- `ZSkipListLevel[] levels`（forward + span）

关键方法:
- `ZSkipListNode insert(double score, byte[] ele)`
- `boolean delete(double score, byte[] ele)`
- `ZSkipListNode find(double score, byte[] ele)`
- `long rank(double score, byte[] ele)` — 1-based
- `List<ZSkipListNode> rangeByScore(double min, double max, boolean withScores)`
- `List<ZSkipListNode> rangeByRank(long start, long stop)`
- `List<ZSkipListNode> rangeByLex(byte[] min, byte[] max)` — 支持 `[`, `(`, `-`, `+`
- `long count(double min, double max)`
- `int randomLevel()` — P=0.25, 最大 64 层

测试: insert/delete、rank、rangeByScore、rangeByRank、rangeByLex

**提交**: `feat: add ZSkipList (skip list for sorted sets)`

---

### Step 9: RESP 协议

**文件**: `redis-server/.../resp/`

#### RespDecoder

流式解析，处理半包/粘包:
- 内部维护 `ByteBuffer readBuf`
- `List<Object> decode(ByteBuffer data)` — 返回完整命令列表
- 支持 RESP2: `+`, `-`, `:`, `$`, `*`
- 支持 RESP3: `_`, `#`, `,`, `(`, `=`, `%`, `~`, `|`, `>`
- 支持内联命令（不以 `*` 开头）

#### RespEncoder

```java
static byte[] encodeSimpleString(String s)
static byte[] encodeError(String msg)
static byte[] encodeInteger(long n)
static byte[] encodeBulkString(byte[] data)  // null -> $-1\r\n
static byte[] encodeArray(List<Object> items)
static byte[] encodeNull()          // RESP3: _\r\n
static byte[] encodeBoolean(boolean b)
static byte[] encodeDouble(double d)
```

测试: 编码/解码往返、半包处理、粘包处理、内联命令、RESP3类型

**提交**: `feat: add RESP2/3 protocol encoder and decoder`

---

### Step 10: AeEventLoop

**文件**: `redis-server/.../ae/`

```
AeEventLoop
├── Selector selector
├── Map<Integer, FileEvent> fileEvents
├── List<TimeEvent> timeEvents
├── long timeEventNextId
└── volatile boolean stop
```

关键方法:
- `void aeCreateFileEvent(int fd, int mask, AeFileProc proc)` — READ=1, WRITE=2
- `void aeDeleteFileEvent(int fd, int mask)`
- `long aeCreateTimeEvent(long ms, AeTimeProc proc)` — 返回 event id
- `void aeDeleteTimeEvent(long id)`
- `void aeMain()` — 主循环
- `int aeProcessEvents(int flags)` — 处理就绪事件
- `void aeStop()`

serverCron 注册: 每 100ms 触发，执行:
- 过期键定期删除（每次抽样 20 个）
- 统计更新（connected clients, memory usage）

测试: FileEvent 注册/删除、TimeEvent 触发精度、事件循环停止

**提交**: `feat: add AeEventLoop (NIO-based event loop)`

---

### Step 11: RedisClient + BIO 线程

**文件**: `redis-server/.../client/RedisClient.java`

字段（对照 Redis client 结构体）:
```java
int fd
Sds querybuf          // 输入缓冲区
int argc
byte[][] argv         // 解析后的命令参数
RedisCommand cmd      // 当前命令
int flags             // CLIENT_SLAVE, CLIENT_MASTER, CLIENT_MONITOR 等
int db                // 当前 DB 索引
long lastInteraction  // 最后交互时间（ms）
// 输出缓冲区
byte[] buf            // 16KB 固定缓冲区
int bufpos
List<byte[]> reply    // 动态 reply list（buf满时使用）
long replyBytes       // 动态缓冲区总字节数
```

BIO 线程（`redis-server/.../bio/BioThread.java`）:
- `BIO_CLOSE_FILE` — 关闭文件描述符
- `BIO_AOF_FSYNC` — AOF fsync
- `BIO_LAZY_FREE` — 惰性释放内存

测试: querybuf 追加、输出缓冲区写入/溢出、BIO 任务提交

**提交**: `feat: add RedisClient and BIO background threads`

---

### Step 12: 命令系统骨架

**文件**: `redis-server/.../command/`

```java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RedisCommand {
    String name();
    int arity();        // 正数=精确，负数=最少
    String flags();     // "write", "read", "admin" 等
    int firstKey();
    int lastKey();
    int step();
}
```

CommandTable:
- 扫描所有 `@RedisCommand` 注解的方法，注册到 `Map<String, CommandEntry>`
- `CommandEntry execute(RedisClient client, byte[][] argv)`

测试: 命令注册、查找、arity 验证

**提交**: `feat: add command system with @RedisCommand annotation`

---

### Step 13: String 命令

实现所有 String 命令，含编码转换:
- 整数值 → `OBJ_ENCODING_INT`
- ≤44字节字符串 → `OBJ_ENCODING_EMBSTR`
- 其余 → `OBJ_ENCODING_RAW`

命令列表:
`GET, SET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, GETRANGE, SETRANGE, SETNX, GETSET, GETDEL, GETEX, STRLEN, MSETNX, PSETEX, SETEX`

SET 选项: `EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET`

错误消息（与官方 Redis 完全一致）:
- `"ERR value is not an integer or out of range"`
- `"ERR increment would produce NaN or Infinity"`
- `"WRONGTYPE Operation against a key holding the wrong kind of value"`

测试: 每个命令的正常路径 + 错误路径

**提交**: `feat: implement String commands with encoding conversion`

---

### Step 14: List 命令

编码转换:
- 初始: `OBJ_ENCODING_LISTPACK`（元素数 ≤128 且每元素 ≤64字节）
- 超过阈值: 转为 `OBJ_ENCODING_QUICKLIST`

命令列表:
`LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LINSERT, LLEN, LSET, LREM, LMOVE, LMPOP, BLPOP, BRPOP`

BLPOP/BRPOP: 阻塞等待（超时后返回 nil）

**提交**: `feat: implement List commands with encoding conversion`

---

### Step 15: Hash 命令

编码转换:
- 初始: `OBJ_ENCODING_LISTPACK`（字段数 ≤128 且每值 ≤64字节）
- 超过阈值: 转为 `OBJ_ENCODING_HT`

命令列表:
`HGET, HSET, HMGET, HMSET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL, HINCRBY, HINCRBYFLOAT, HSCAN, HRANDFIELD, HSETNX`

**提交**: `feat: implement Hash commands with encoding conversion`

---

### Step 16: Set 命令

编码转换:
- 全整数且数量 ≤512: `OBJ_ENCODING_INTSET`
- 元素数 ≤128 且每元素 ≤64字节: `OBJ_ENCODING_LISTPACK`
- 其余: `OBJ_ENCODING_HT`

命令列表:
`SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SRANDMEMBER, SPOP, SMOVE, SSCAN`

**提交**: `feat: implement Set commands with encoding conversion`

---

### Step 17: ZSet 命令

编码转换:
- 初始: `OBJ_ENCODING_LISTPACK`（元素数 ≤128 且每元素 ≤64字节）
- 超过阈值: 转为 `OBJ_ENCODING_SKIPLIST`（ZSkipList + Dict）

命令列表:
`ZADD, ZRANGE, ZRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGE, ZRANK, ZREVRANK, ZSCORE, ZMSCORE, ZREM, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZCARD, ZINCRBY, ZUNIONSTORE, ZINTERSTORE, ZPOPMIN, ZPOPMAX, ZRANDMEMBER, ZSCAN, ZRANGESTORE`

**提交**: `feat: implement ZSet commands with encoding conversion`

---

### Step 18: 通用命令

`DEL, UNLINK, KEYS, SCAN, TYPE, RENAME, RENAMENX, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, EXISTS, OBJECT, WAIT, DUMP, RESTORE, COPY, MOVE, SELECT, DBSIZE, FLUSHDB, FLUSHALL, RANDOMKEY, TOUCH`

**提交**: `feat: implement generic commands`

---

### Step 19: Server 命令

`PING, ECHO, INFO, CONFIG GET/SET, COMMAND, COMMAND COUNT, COMMAND INFO, SLOWLOG GET/LEN/RESET, TIME, DEBUG, RESET, QUIT, AUTH`

**提交**: `feat: implement server commands`

---

### Step 20: 过期键机制

1. **惰性删除**: 每次 `lookupKey` 时检查是否过期
2. **定期删除**: `serverCron` 中:
   - 每次从每个 DB 随机抽取 20 个键
   - 删除其中过期的键
   - 如果过期比例 > 25%，继续抽样（最多 250ms）

**提交**: `feat: implement key expiration (lazy + periodic)`

---

### Step 21: 多 DB 支持

- 默认 16 个 DB（`databases = 16`）
- `SELECT index` 命令切换
- 每个 DB 独立的 `Dict dict` 和 `Dict expires`

**提交**: `feat: add multi-database support (SELECT command)`

---

### Step 22: RedisServer 主入口

**文件**: `redis-server/.../RedisServer.java`

```java
public class RedisServer {
    int port = 6399;  // 避免与本机 Redis 冲突
    String bindAddr = "127.0.0.1";
    AeEventLoop eventLoop;
    List<RedisDb> dbs;
    CommandTable commandTable;
    // 统计信息
    long totalCommandsProcessed;
    long totalConnections;
    long startTime;
    
    void start() throws IOException
    void stop()
    static void main(String[] args)
}
```

**提交**: `feat: add RedisServer main entry point`

---

### Step 23: 集成测试

**文件**: `redis-test/.../integration/`

测试类:
- `StringCommandsIntegrationTest` — 启动 server，用 Jedis 测试所有 String 命令
- `ListCommandsIntegrationTest`
- `HashCommandsIntegrationTest`
- `SetCommandsIntegrationTest`
- `ZSetCommandsIntegrationTest`
- `GenericCommandsIntegrationTest`
- `ServerCommandsIntegrationTest`

每个测试:
1. `@BeforeAll` 启动 RedisServer（随机端口）
2. `@BeforeEach` 用 Jedis 连接，FLUSHALL
3. 执行命令，断言与期望值一致
4. `@AfterAll` 停止 server

**提交**: `test: add integration tests with Jedis`

---

## 关键阈值（与 Redis 官方一致）

| 数据结构 | 转换条件 |
|---------|---------|
| String INT→EMBSTR | 不能表示为 long |
| String EMBSTR→RAW | 长度 > 44 字节 |
| List LISTPACK→QUICKLIST | 元素数 > 128 或单元素 > 64 字节 |
| Hash LISTPACK→HT | 字段数 > 128 或单值 > 64 字节 |
| Set INTSET→HT | 元素数 > 512 或添加非整数 |
| Set LISTPACK→HT | 元素数 > 128 或单元素 > 64 字节 |
| ZSet LISTPACK→SKIPLIST | 元素数 > 128 或单元素 > 64 字节 |
| IntSet 升级 | 添加超出当前编码范围的整数 |

## 错误消息（与官方 Redis 完全一致）

```
WRONGTYPE Operation against a key holding the wrong kind of value
ERR value is not an integer or out of range
ERR increment would produce NaN or Infinity
ERR no such key
ERR syntax error
ERR invalid expire time in 'set' command
ERR DB index is out of range
ERR value is out of range, must be positive
ERR bit offset is not an integer or out of range
```
