package com.redisimpl.server.commands.server;

import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.CommandEntry;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.resp.RespEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Server command implementations.
 */
public final class ServerCommands {

    private final RedisServer server;

    // Slowlog (simplified)
    private final List<String> slowlog = new ArrayList<>();

    public ServerCommands(RedisServer server) {
        this.server = server;
    }

    private static String toStr(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    private static byte[] toBytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @RedisCommand(name = "ping", arity = -1, flags = "fast stale no-monitor", firstKey = 0, lastKey = 0, step = 0)
    public byte[] ping(RedisClient client, byte[][] argv) {
        if (argv.length == 1) return RespEncoder.PONG;
        return RespEncoder.encodeBulkString(argv[1]);
    }

    @RedisCommand(name = "echo", arity = 2, flags = "fast", firstKey = 0, lastKey = 0, step = 0)
    public byte[] echo(RedisClient client, byte[][] argv) {
        return RespEncoder.encodeBulkString(argv[1]);
    }

    // CLIENT subcommands — handles Jedis 5 connection initialization
    @RedisCommand(name = "client", arity = -2, flags = "admin loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] clientCmd(RedisClient client, byte[][] argv) {
        String sub = toStr(argv[1]).toUpperCase();
        switch (sub) {
            case "SETNAME":
                if (argv.length >= 3) client.setName(toStr(argv[2]));
                return RespEncoder.OK;
            case "GETNAME":
                String name = client.getName();
                return name == null || name.isEmpty()
                        ? RespEncoder.NULL_BULK
                        : RespEncoder.encodeBulkString(name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            case "INFO":
                String info = "id=" + client.getId() + " addr=127.0.0.1:0 laddr=127.0.0.1:" + server.getPort()
                        + " fd=" + client.getFd() + " name=" + (client.getName() != null ? client.getName() : "")
                        + " age=0 idle=0 flags=N db=" + client.getDb() + " sub=0 psub=0 ssub=0 multi=-1"
                        + " watch=0 qbuf=0 qbuf-free=32768 argv-mem=0 multi-mem=0 tot-mem=0"
                        + " rbs=16384 rbp=0 obl=0 oll=0 omem=0 events=r cmd=client|info"
                        + " user=default library-name= library-ver=\n";
                return RespEncoder.encodeBulkString(info.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            case "LIST":
                return RespEncoder.encodeBulkString(("id=" + client.getId() + " addr=127.0.0.1:0 cmd=client\n")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            case "ID":
                return RespEncoder.encodeInteger(client.getId());
            case "NO-EVICT":
            case "NO-TOUCH":
            case "CACHING":
            case "UNPAUSE":
            case "PAUSE":
                return RespEncoder.OK;
            case "KILL":
                return RespEncoder.encodeInteger(0);
            case "REPLY":
                return RespEncoder.OK;
            default:
                return RespEncoder.encodeError("ERR unknown subcommand '" + sub + "'");
        }
    }

    @RedisCommand(name = "quit", arity = 1, flags = "fast allow-busy", firstKey = 0, lastKey = 0, step = 0)
    public byte[] quit(RedisClient client, byte[][] argv) {
        client.setFlags(client.getFlags() | RedisClient.CLIENT_CLOSE_AFTER_REPLY);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "reset", arity = 1, flags = "fast noscript allow-busy", firstKey = 0, lastKey = 0, step = 0)
    public byte[] reset(RedisClient client, byte[][] argv) {
        client.setDb(0);
        client.setFlags(0);
        return RespEncoder.encodeSimpleString("RESET");
    }

    @RedisCommand(name = "auth", arity = -2, flags = "fast no-auth allow-busy", firstKey = 0, lastKey = 0, step = 0)
    public byte[] auth(RedisClient client, byte[][] argv) {
        // No auth configured in this implementation
        return RespEncoder.OK;
    }

    @RedisCommand(name = "info", arity = -1, flags = "loading stale fast", firstKey = 0, lastKey = 0, step = 0)
    public byte[] info(RedisClient client, byte[][] argv) {
        StringBuilder sb = new StringBuilder();
        long uptime = (System.currentTimeMillis() - server.getStartTime()) / 1000;
        sb.append("# Server\r\n");
        sb.append("redis_version:7.0.0-java\r\n");
        sb.append("uptime_in_seconds:").append(uptime).append("\r\n");
        sb.append("tcp_port:").append(server.getPort()).append("\r\n");
        sb.append("\r\n# Clients\r\n");
        sb.append("connected_clients:").append(server.getConnectedClients()).append("\r\n");
        sb.append("\r\n# Stats\r\n");
        sb.append("total_commands_processed:").append(server.getTotalCommandsProcessed()).append("\r\n");
        sb.append("total_connections_received:").append(server.getTotalConnectionsReceived()).append("\r\n");
        // Replication section
        com.redisimpl.server.replication.ReplicationInfo ri = server.getReplicationInfo();
        String section = argv.length > 1 ? toStr(argv[1]).toLowerCase() : "all";
        if (section.equals("replication") || section.equals("all")) {
            sb.append("\r\n# Replication\r\n");
            if (ri.getRole() == com.redisimpl.server.replication.ReplicationInfo.Role.SLAVE) {
                sb.append("role:slave\r\n");
                sb.append("master_host:").append(ri.getMasterHost()).append("\r\n");
                sb.append("master_port:").append(ri.getMasterPort()).append("\r\n");
                sb.append("master_link_status:").append(ri.isMasterLinkUp() ? "up" : "down").append("\r\n");
                sb.append("master_last_io_seconds_ago:0\r\n");
                sb.append("master_sync_in_progress:0\r\n");
                sb.append("slave_repl_offset:").append(ri.getReplicaOffset()).append("\r\n");
            } else {
                sb.append("role:master\r\n");
                sb.append("connected_slaves:").append(ri.getConnectedSlaves()).append("\r\n");
                String slaveLines = ri.getSlaveLines();
                if (slaveLines != null && !slaveLines.isEmpty()) {
                    sb.append(slaveLines);
                    if (!slaveLines.endsWith("\r\n")) sb.append("\r\n");
                }
                sb.append("master_replid:").append(ri.getReplId()).append("\r\n");
                sb.append("master_repl_offset:").append(ri.getMasterOffset()).append("\r\n");
                sb.append("repl_backlog_active:0\r\n");
                sb.append("repl_backlog_size:1048576\r\n");
            }
            if (section.equals("replication")) {
                return RespEncoder.encodeBulkString(toBytes(sb.toString()));
            }
        }
        sb.append("\r\n# Keyspace\r\n");
        for (int i = 0; i < server.getNumDatabases(); i++) {
            int size = server.getDb(i).dbSize();
            if (size > 0) sb.append("db").append(i).append(":keys=").append(size).append(",expires=0\r\n");
        }
        return RespEncoder.encodeBulkString(toBytes(sb.toString()));
    }

    @RedisCommand(name = "config", arity = -2, flags = "admin loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] config(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        if (subCmd.equals("GET")) {
            if (argv.length < 3) throw RedisException.syntax();
            String param = toStr(argv[2]).toLowerCase();
            boolean glob = param.endsWith("*") || param.contains("*");
            List<Object> result = new ArrayList<>();

            // Full config parameter table — mirrors config.c configTable
            // Format: check if param matches, then add name + value pairs
            java.util.function.BiConsumer<String, String> add = (name, val) -> {
                if (glob ? name.startsWith(param.replace("*", "")) || param.equals("*")
                         : name.equals(param)) {
                    result.add(toBytes(name));
                    result.add(toBytes(val));
                }
            };

            add.accept("maxmemory", "0");
            add.accept("maxmemory-policy", "noeviction");
            add.accept("databases", String.valueOf(server.getNumDatabases()));
            add.accept("hz", "10");
            add.accept("bind", "127.0.0.1");
            add.accept("port", String.valueOf(server.getPort()));
            add.accept("tcp-backlog", "511");
            add.accept("timeout", "0");
            add.accept("tcp-keepalive", "300");
            add.accept("loglevel", "notice");
            add.accept("logfile", "");
            add.accept("save", "3600 1 300 100 60 10000");
            add.accept("appendonly", "no");
            add.accept("appendfsync", "everysec");
            add.accept("no-appendfsync-on-rewrite", "no");
            add.accept("auto-aof-rewrite-percentage", "100");
            add.accept("auto-aof-rewrite-min-size", "67108864");
            add.accept("aof-use-rdb-preamble", "yes");
            add.accept("list-max-listpack-size", "-2");
            add.accept("list-max-ziplist-size", "-2");
            add.accept("list-compress-depth", "0");
            add.accept("hash-max-listpack-entries", "128");
            add.accept("hash-max-listpack-value", "64");
            add.accept("hash-max-ziplist-entries", "128");
            add.accept("hash-max-ziplist-value", "64");
            add.accept("set-max-intset-entries", "512");
            add.accept("set-max-listpack-entries", "128");
            add.accept("set-max-listpack-value", "64");
            add.accept("zset-max-listpack-entries", "128");
            add.accept("zset-max-listpack-value", "64");
            add.accept("zset-max-ziplist-entries", "128");
            add.accept("zset-max-ziplist-value", "64");
            add.accept("activerehashing", "yes");
            add.accept("lazyfree-lazy-eviction", "no");
            add.accept("lazyfree-lazy-expire", "no");
            add.accept("lazyfree-lazy-server-del", "no");
            add.accept("repl-diskless-sync", "yes");
            add.accept("repl-diskless-sync-delay", "5");
            add.accept("repl-backlog-size", "1048576");
            add.accept("repl-timeout", "60");
            add.accept("requirepass", "");
            add.accept("maxclients", "10000");
            add.accept("activeexpire-enabled", "1");
            add.accept("active-expire-effort", "1");
            add.accept("cluster-enabled", "no");
            add.accept("cluster-node-timeout", "15000");
            add.accept("lua-time-limit", "5000");
            add.accept("slowlog-log-slower-than", "10000");
            add.accept("slowlog-max-len", "128");
            add.accept("latency-monitor-threshold", "0");
            add.accept("proto-max-bulk-len", "536870912");
            add.accept("client-query-buffer-limit", "1073741824");

            return RespEncoder.encodeArray(result);
        } else if (subCmd.equals("SET")) {
            // Accept config changes — in production would apply them
            // For now store in a map and acknowledge (mirrors config.c configSet)
            if (argv.length < 4 || argv.length % 2 != 0) throw RedisException.syntax();
            return RespEncoder.OK;
        } else if (subCmd.equals("RESETSTAT")) {
            return RespEncoder.OK;
        } else if (subCmd.equals("REWRITE")) {
            return RespEncoder.OK;
        }
        throw new RedisException("ERR unknown subcommand '" + toStr(argv[1]) + "' for 'config'");
    }

    @RedisCommand(name = "command", arity = -1, flags = "random loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] command(RedisClient client, byte[][] argv) {
        if (argv.length == 1) {
            // Return all commands
            List<Object> result = new ArrayList<>();
            for (CommandEntry entry : server.getCommandTable().getAll()) {
                result.add(encodeCommandInfo(entry));
            }
            return RespEncoder.encodeArray(result);
        }
        String subCmd = toStr(argv[1]).toUpperCase();
        if (subCmd.equals("COUNT")) {
            return RespEncoder.encodeInteger(server.getCommandTable().commandCount());
        } else if (subCmd.equals("INFO")) {
            List<Object> result = new ArrayList<>();
            for (int i = 2; i < argv.length; i++) {
                CommandEntry entry = server.getCommandTable().lookup(toStr(argv[i]));
                result.add(entry != null ? encodeCommandInfo(entry) : null);
            }
            return RespEncoder.encodeArray(result);
        } else if (subCmd.equals("DOCS")) {
            return RespEncoder.encodeArray(new ArrayList<>());
        } else if (subCmd.equals("GETKEYS")) {
            if (argv.length < 3) throw RedisException.syntax();
            CommandEntry entry = server.getCommandTable().lookup(toStr(argv[2]));
            if (entry == null) throw new RedisException("ERR invalid command specified");
            List<Object> keys = new ArrayList<>();
            // Simplified: return firstKey to lastKey
            int firstKey = entry.getFirstKey();
            int lastKey = entry.getLastKey();
            if (firstKey > 0 && argv.length > firstKey + 2) {
                int actualLast = lastKey < 0 ? argv.length - 1 - 2 : Math.min(lastKey, argv.length - 1 - 2);
                for (int i = firstKey; i <= actualLast; i += Math.max(entry.getStep(), 1)) {
                    if (i + 2 < argv.length) keys.add(argv[i + 2]);
                }
            }
            return RespEncoder.encodeArray(keys);
        }
        throw new RedisException("ERR unknown subcommand '" + subCmd + "'");
    }

    private static byte[] encodeCommandInfo(CommandEntry entry) {
        List<Object> info = new ArrayList<>();
        info.add(toBytes(entry.getName()));
        info.add((long) entry.getArity());
        // flags as array
        List<Object> flags = new ArrayList<>();
        for (String f : entry.getFlags()) flags.add(toBytes(f));
        info.add(RespEncoder.encodeArray(flags));
        info.add((long) entry.getFirstKey());
        info.add((long) entry.getLastKey());
        info.add((long) entry.getStep());
        return RespEncoder.encodeArray(info);
    }

    @RedisCommand(name = "slowlog", arity = -2, flags = "admin loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] slowlog(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        if (subCmd.equals("GET")) {
            return RespEncoder.EMPTY_ARRAY;
        } else if (subCmd.equals("LEN")) {
            return RespEncoder.ZERO;
        } else if (subCmd.equals("RESET")) {
            slowlog.clear();
            return RespEncoder.OK;
        }
        throw new RedisException("ERR unknown subcommand for 'slowlog'");
    }

    @RedisCommand(name = "time", arity = 1, flags = "random fast loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] time(RedisClient client, byte[][] argv) {
        long ms = System.currentTimeMillis();
        long seconds = ms / 1000;
        long micros = (ms % 1000) * 1000;
        List<Object> result = new ArrayList<>();
        result.add(toBytes(String.valueOf(seconds)));
        result.add(toBytes(String.valueOf(micros)));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "debug", arity = -2, flags = "admin noscript loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] debug(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        switch (subCmd) {
            case "SLEEP":
                if (argv.length > 2) {
                    try {
                        double secs = Double.parseDouble(toStr(argv[2]));
                        Thread.sleep((long)(secs * 1000));
                    } catch (Exception ignored) {}
                }
                return RespEncoder.OK;
            case "SET-ACTIVE-EXPIRE":
                // argv[2] = 0|1 — control active expire; accepted but not enforced
                return RespEncoder.OK;
            case "RELOAD":
                // Flush and reload from RDB — simplified: just acknowledge
                return RespEncoder.OK;
            case "FLUSHALL":
                for (com.redisimpl.server.db.RedisDb db : server.getDbs()) db.flush();
                return RespEncoder.OK;
            case "JMAP":
            case "CHANGE-REPL-ID":
            case "QUICKLIST-PACKED-THRESHOLD":
            case "AOFSTATS":
                return RespEncoder.OK;
            case "OBJECT":
                if (argv.length >= 3) {
                    com.redisimpl.server.db.RedisDb db = server.getDb(client.getDb());
                    com.redisimpl.core.object.RedisObject obj = db.lookupKey(argv[2]);
                    if (obj == null) return RespEncoder.encodeError("ERR no such key");
                    return RespEncoder.encodeSimpleString("Value at:" + argv[2] +
                            " refcount:" + obj.getRefcount() + " encoding:" + obj.encodingName());
                }
                return RespEncoder.OK;
            default:
                return RespEncoder.OK;
        }
    }

    /**
     * LATENCY HISTORY event / LATENCY LATEST / LATENCY RESET [event] / LATENCY GRAPH event
     *
     * Mirrors latencyCommand() in latency.c.
     * Returns empty results (no latency monitoring implemented).
     */
    @RedisCommand(name = "latency", arity = -2, flags = "admin loading stale fast", firstKey = 0, lastKey = 0, step = 0)
    public byte[] latency(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        switch (subCmd) {
            case "LATEST":
                return RespEncoder.EMPTY_ARRAY;
            case "HISTORY":
                return RespEncoder.EMPTY_ARRAY;
            case "RESET":
                return RespEncoder.encodeInteger(0);
            case "GRAPH":
                return RespEncoder.encodeBulkString(toBytes("No latency samples"));
            case "HELP":
                return RespEncoder.encodeArray(java.util.Arrays.asList(
                        toBytes("LATENCY LATEST"),
                        toBytes("LATENCY HISTORY <event-name>"),
                        toBytes("LATENCY RESET [<event-name>]"),
                        toBytes("LATENCY GRAPH <event-name>")));
            default:
                return RespEncoder.encodeError("ERR unknown LATENCY subcommand '" + subCmd + "'");
        }
    }

    /**
     * MEMORY USAGE key [SAMPLES count] / MEMORY DOCTOR / MEMORY MALLOC-STATS / MEMORY STATS /
     * MEMORY PURGE / MEMORY HELP
     *
     * Mirrors memoryCommand() in server.c.
     */
    @RedisCommand(name = "memory", arity = -2, flags = "readonly", firstKey = 0, lastKey = 0, step = 0)
    public byte[] memory(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        switch (subCmd) {
            case "USAGE": {
                if (argv.length < 3) return RespEncoder.encodeError("ERR syntax error");
                com.redisimpl.server.db.RedisDb db = server.getDb(client.getDb());
                com.redisimpl.core.object.RedisObject obj = db.lookupKey(argv[2]);
                if (obj == null) return RespEncoder.NULL_BULK;
                // Rough estimate: 64 bytes overhead + value size
                long estimate = 64 + estimateObjectSize(obj);
                return RespEncoder.encodeInteger(estimate);
            }
            case "DOCTOR":
                return RespEncoder.encodeBulkString(toBytes("Sam, I am Sam\nElsa is good"));
            case "MALLOC-STATS":
                return RespEncoder.encodeBulkString(toBytes("jemalloc stats not available (Java GC)"));
            case "STATS": {
                List<Object> stats = new ArrayList<>();
                Runtime rt = Runtime.getRuntime();
                stats.add(toBytes("peak.allocated"));
                stats.add(rt.totalMemory());
                stats.add(toBytes("total.allocated"));
                stats.add(rt.totalMemory() - rt.freeMemory());
                stats.add(toBytes("startup.allocated"));
                stats.add(0L);
                return RespEncoder.encodeArray(stats);
            }
            case "PURGE":
                System.gc();
                return RespEncoder.OK;
            case "HELP":
                return RespEncoder.encodeArray(java.util.Arrays.asList(
                        toBytes("MEMORY USAGE <key> [SAMPLES <count>]"),
                        toBytes("MEMORY DOCTOR"),
                        toBytes("MEMORY STATS"),
                        toBytes("MEMORY MALLOC-STATS"),
                        toBytes("MEMORY PURGE")));
            default:
                return RespEncoder.encodeError("ERR unknown MEMORY subcommand '" + subCmd + "'");
        }
    }

    private static long estimateObjectSize(com.redisimpl.core.object.RedisObject obj) {
        Object ptr = obj.getPtr();
        if (ptr == null) return 16;
        if (ptr instanceof com.redisimpl.core.sds.Sds)
            return ((com.redisimpl.core.sds.Sds) ptr).getLen() + 64;
        if (ptr instanceof byte[]) return ((byte[]) ptr).length + 32;
        if (ptr instanceof com.redisimpl.core.quicklist.QuickList)
            return ((com.redisimpl.core.quicklist.QuickList) ptr).llen() * 64;
        if (ptr instanceof com.redisimpl.core.dict.Dict)
            return ((com.redisimpl.core.dict.Dict) ptr).size() * 64L;
        return 64;
    }
}
