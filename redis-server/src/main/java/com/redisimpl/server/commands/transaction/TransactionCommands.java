package com.redisimpl.server.commands.transaction;

import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.resp.RespEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Transaction command implementations: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
 */
public final class TransactionCommands {

    private final RedisServer server;

    public TransactionCommands(RedisServer server) {
        this.server = server;
    }

    @RedisCommand(name = "multi", arity = 1, flags = "fast no-monitor allow-busy", firstKey = 0, lastKey = 0, step = 0)
    public byte[] multi(RedisClient client, byte[][] argv) {
        if (client.isInMulti()) {
            return RespEncoder.encodeError("ERR MULTI calls can not be nested");
        }
        client.setInMulti(true);
        client.getTxQueue().clear();
        client.setTxDirty(false);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "exec", arity = 1, flags = "no-monitor allow-busy", firstKey = 0, lastKey = 0, step = 0)
    public byte[] exec(RedisClient client, byte[][] argv) {
        if (!client.isInMulti()) {
            return RespEncoder.encodeError("ERR EXEC without MULTI");
        }

        // If WATCH was violated, return null array
        if (client.isTxDirty()) {
            client.setInMulti(false);
            client.getTxQueue().clear();
            client.getWatchedKeys().clear();
            return "*-1\r\n".getBytes(StandardCharsets.UTF_8);
        }

        List<byte[][]> queue = client.getTxQueue();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + queue.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            // Execute each queued command
            for (byte[][] cmdArgv : queue) {
                String cmdName = new String(cmdArgv[0], StandardCharsets.UTF_8).toLowerCase();
                byte[] result = server.executeCommand(client, cmdArgv);
                out.write(result != null ? result : RespEncoder.NULL_BULK);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        client.setInMulti(false);
        client.getTxQueue().clear();
        client.getWatchedKeys().clear();
        client.setTxDirty(false);
        return out.toByteArray();
    }

    @RedisCommand(name = "discard", arity = 1, flags = "fast no-monitor allow-busy", firstKey = 0, lastKey = 0, step = 0)
    public byte[] discard(RedisClient client, byte[][] argv) {
        if (!client.isInMulti()) {
            return RespEncoder.encodeError("ERR DISCARD without MULTI");
        }
        client.setInMulti(false);
        client.getTxQueue().clear();
        client.getWatchedKeys().clear();
        client.setTxDirty(false);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "watch", arity = -2, flags = "fast no-monitor allow-busy", firstKey = 1, lastKey = -1, step = 1)
    public byte[] watch(RedisClient client, byte[][] argv) {
        if (client.isInMulti()) {
            return RespEncoder.encodeError("ERR WATCH inside MULTI is not allowed");
        }
        for (int i = 1; i < argv.length; i++) {
            String key = new String(argv[i], StandardCharsets.UTF_8);
            client.getWatchedKeys().add(key);
            server.registerWatch(client, key);
        }
        return RespEncoder.OK;
    }

    @RedisCommand(name = "unwatch", arity = 1, flags = "fast no-monitor allow-busy", firstKey = 0, lastKey = 0, step = 0)
    public byte[] unwatch(RedisClient client, byte[][] argv) {
        server.unregisterWatches(client);
        client.getWatchedKeys().clear();
        client.setTxDirty(false);
        return RespEncoder.OK;
    }
}
