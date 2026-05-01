package com.redisimpl.server.commands.pubsub;

import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.pubsub.PubSubManager;
import com.redisimpl.server.resp.RespEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pub/Sub command implementations.
 * SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB
 */
public final class PubSubCommands {

    private final RedisServer server;
    private final PubSubManager pubSub;

    public PubSubCommands(RedisServer server, PubSubManager pubSub) {
        this.server = server;
        this.pubSub = pubSub;
    }

    private static String toStr(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @RedisCommand(name = "subscribe", arity = -2, flags = "pubsub fast no-monitor", firstKey = 0, lastKey = 0, step = 0)
    public byte[] subscribe(RedisClient client, byte[][] argv) {
        String[] channels = new String[argv.length - 1];
        for (int i = 1; i < argv.length; i++) channels[i - 1] = toStr(argv[i]);
        List<byte[]> replies = pubSub.subscribe(client, channels);
        return concat(replies);
    }

    @RedisCommand(name = "unsubscribe", arity = -1, flags = "pubsub fast no-monitor", firstKey = 0, lastKey = 0, step = 0)
    public byte[] unsubscribe(RedisClient client, byte[][] argv) {
        String[] channels = new String[argv.length - 1];
        for (int i = 1; i < argv.length; i++) channels[i - 1] = toStr(argv[i]);
        List<byte[]> replies = pubSub.unsubscribe(client, channels);
        return concat(replies);
    }

    @RedisCommand(name = "psubscribe", arity = -2, flags = "pubsub fast no-monitor", firstKey = 0, lastKey = 0, step = 0)
    public byte[] psubscribe(RedisClient client, byte[][] argv) {
        String[] patterns = new String[argv.length - 1];
        for (int i = 1; i < argv.length; i++) patterns[i - 1] = toStr(argv[i]);
        List<byte[]> replies = pubSub.psubscribe(client, patterns);
        return concat(replies);
    }

    @RedisCommand(name = "punsubscribe", arity = -1, flags = "pubsub fast no-monitor", firstKey = 0, lastKey = 0, step = 0)
    public byte[] punsubscribe(RedisClient client, byte[][] argv) {
        String[] patterns = new String[argv.length - 1];
        for (int i = 1; i < argv.length; i++) patterns[i - 1] = toStr(argv[i]);
        List<byte[]> replies = pubSub.punsubscribe(client, patterns);
        return concat(replies);
    }

    @RedisCommand(name = "publish", arity = 3, flags = "pubsub fast may-replicate", firstKey = 0, lastKey = 0, step = 0)
    public byte[] publish(RedisClient client, byte[][] argv) {
        String channel = toStr(argv[1]);
        byte[] message = argv[2];
        long count = pubSub.publish(channel, message, server::flushClient);
        return RespEncoder.encodeInteger(count);
    }

    @RedisCommand(name = "pubsub", arity = -2, flags = "pubsub random loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] pubsub(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        switch (subCmd) {
            case "CHANNELS": {
                String pattern = argv.length >= 3 ? toStr(argv[2]) : null;
                List<String> chs = pubSub.pubsubChannels(pattern);
                List<Object> items = new ArrayList<>(chs);
                return RespEncoder.encodeArray(items);
            }
            case "NUMSUB": {
                String[] names = new String[argv.length - 2];
                for (int i = 2; i < argv.length; i++) names[i - 2] = toStr(argv[i]);
                Map<String, Long> counts = pubSub.pubsubNumsub(names);
                List<Object> items = new ArrayList<>();
                for (Map.Entry<String, Long> e : counts.entrySet()) {
                    items.add(e.getKey());
                    items.add(e.getValue());
                }
                return RespEncoder.encodeArray(items);
            }
            case "NUMPAT": {
                return RespEncoder.encodeInteger(pubSub.pubsubNumpat());
            }
            default:
                return RespEncoder.encodeError("ERR unknown subcommand '" + subCmd + "' for 'pubsub'");
        }
    }

    /** Concatenate multiple RESP replies into one byte array. */
    private static byte[] concat(List<byte[]> parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            for (byte[] part : parts) out.write(part);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }
}
