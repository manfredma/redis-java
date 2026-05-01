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
            List<Object> result = new ArrayList<>();
            if (param.equals("maxmemory") || param.equals("*")) {
                result.add(toBytes("maxmemory"));
                result.add(toBytes("0"));
            }
            if (param.equals("databases") || param.equals("*")) {
                result.add(toBytes("databases"));
                result.add(toBytes(String.valueOf(server.getNumDatabases())));
            }
            if (param.equals("hz") || param.equals("*")) {
                result.add(toBytes("hz"));
                result.add(toBytes("10"));
            }
            return RespEncoder.encodeArray(result);
        } else if (subCmd.equals("SET")) {
            return RespEncoder.OK; // Accept but ignore config changes
        } else if (subCmd.equals("RESETSTAT")) {
            return RespEncoder.OK;
        } else if (subCmd.equals("REWRITE")) {
            return RespEncoder.OK;
        }
        throw new RedisException("ERR unknown subcommand for 'config'");
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
        if (subCmd.equals("SLEEP")) {
            if (argv.length > 2) {
                try {
                    double secs = Double.parseDouble(toStr(argv[2]));
                    Thread.sleep((long) (secs * 1000));
                } catch (Exception ignored) {}
            }
            return RespEncoder.OK;
        }
        if (subCmd.equals("RELOAD") || subCmd.equals("FLUSHALL")) {
            return RespEncoder.OK;
        }
        return RespEncoder.OK;
    }
}
