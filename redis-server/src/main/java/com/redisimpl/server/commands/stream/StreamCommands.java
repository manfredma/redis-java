package com.redisimpl.server.commands.stream;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.db.RedisDb;
import com.redisimpl.server.resp.RespEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis Stream command implementations.
 * Covers: XADD, XREAD, XRANGE, XREVRANGE, XLEN, XTRIM, XDEL,
 *         XGROUP, XREADGROUP, XACK, XCLAIM, XAUTOCLAIM, XPENDING, XINFO
 */
public final class StreamCommands {

    /** Type constant for Stream objects (7, after Hash=4, Module=5, Stream=6 in real Redis) */
    public static final int OBJ_TYPE_STREAM = 6;
    /** Encoding constant for stream */
    public static final int OBJ_ENCODING_STREAM = 14;

    private final RedisServer server;

    public StreamCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) {
        return server.getDb(client.getDb());
    }

    private static byte[] toBytes(String s) {
        if (s == null) return null;
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String toStr(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    // ---- Helper: get or create StreamObject from db ----

    private StreamObject getOrCreateStream(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null) {
            StreamObject stream = new StreamObject();
            db.setKey(key, RedisObject.createObject(OBJ_TYPE_STREAM, OBJ_ENCODING_STREAM, stream));
            return stream;
        }
        if (obj.getType() != OBJ_TYPE_STREAM) {
            throw new RedisException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return (StreamObject) obj.getPtr();
    }

    private StreamObject getStream(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null) return null;
        if (obj.getType() != OBJ_TYPE_STREAM) {
            throw new RedisException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return (StreamObject) obj.getPtr();
    }

    // ---- XADD ----

    @RedisCommand(name = "xadd", arity = -5, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xadd(RedisClient client, byte[][] argv) {
        byte[] key = argv[1];
        int argIdx = 2;

        // Optional MAXLEN
        long maxLen = 0;
        boolean approx = false;
        if (argIdx < argv.length && toStr(argv[argIdx]).equalsIgnoreCase("MAXLEN")) {
            argIdx++;
            if (argIdx < argv.length && toStr(argv[argIdx]).equals("~")) {
                approx = true;
                argIdx++;
            }
            maxLen = Long.parseLong(toStr(argv[argIdx++]));
        }

        // ID
        String idStr = toStr(argv[argIdx++]);
        long reqMillis = -1, reqSeq = -1;
        if (!"*".equals(idStr)) {
            int dash = idStr.lastIndexOf('-');
            if (dash < 0) {
                reqMillis = Long.parseLong(idStr);
            } else {
                reqMillis = Long.parseLong(idStr.substring(0, dash));
                String seqStr = idStr.substring(dash + 1);
                reqSeq = "*".equals(seqStr) ? -1 : Long.parseLong(seqStr);
            }
        }

        // Fields
        Map<String, String> fields = new LinkedHashMap<>();
        while (argIdx + 1 < argv.length) {
            fields.put(toStr(argv[argIdx]), toStr(argv[argIdx + 1]));
            argIdx += 2;
        }
        if (fields.isEmpty()) {
            return RespEncoder.encodeError("ERR wrong number of arguments for 'xadd' command");
        }

        try {
            StreamObject stream = getOrCreateStream(db(client), key);
            if (maxLen > 0) stream.setMaxLen(maxLen);
            StreamEntry entry = stream.add(reqMillis, reqSeq, fields);
            return RespEncoder.encodeBulkString(toBytes(entry.getId()));
        } catch (IllegalArgumentException e) {
            return RespEncoder.encodeError(e.getMessage());
        }
    }

    // ---- XLEN ----

    @RedisCommand(name = "xlen", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xlen(RedisClient client, byte[][] argv) {
        StreamObject stream = getStream(db(client), argv[1]);
        return RespEncoder.encodeInteger(stream == null ? 0 : stream.size());
    }

    // ---- XRANGE ----

    @RedisCommand(name = "xrange", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xrange(RedisClient client, byte[][] argv) {
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return RespEncoder.EMPTY_ARRAY;

        String start = toStr(argv[2]);
        String end   = toStr(argv[3]);
        int count = 0;
        if (argv.length >= 6 && toStr(argv[4]).equalsIgnoreCase("COUNT")) {
            count = Integer.parseInt(toStr(argv[5]));
        }

        List<StreamEntry> entries = stream.range(start, end, count);
        return encodeEntries(entries);
    }

    // ---- XREVRANGE ----

    @RedisCommand(name = "xrevrange", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xrevrange(RedisClient client, byte[][] argv) {
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return RespEncoder.EMPTY_ARRAY;

        String end   = toStr(argv[2]);
        String start = toStr(argv[3]);
        int count = 0;
        if (argv.length >= 6 && toStr(argv[4]).equalsIgnoreCase("COUNT")) {
            count = Integer.parseInt(toStr(argv[5]));
        }

        List<StreamEntry> entries = stream.revrange(end, start, count);
        return encodeEntries(entries);
    }

    // ---- XREAD ----

    @RedisCommand(name = "xread", arity = -4, flags = "read-only", firstKey = 0, lastKey = 0, step = 0)
    public byte[] xread(RedisClient client, byte[][] argv) {
        int count = 0;
        int argIdx = 1;

        while (argIdx < argv.length) {
            String opt = toStr(argv[argIdx]).toUpperCase();
            if ("COUNT".equals(opt)) {
                count = Integer.parseInt(toStr(argv[++argIdx]));
                argIdx++;
            } else if ("BLOCK".equals(opt)) {
                argIdx += 2; // skip block timeout (not implemented)
            } else if ("STREAMS".equals(opt)) {
                argIdx++;
                break;
            } else {
                break;
            }
        }

        // remaining: keys then ids
        int remaining = argv.length - argIdx;
        if (remaining % 2 != 0) {
            return RespEncoder.encodeError("ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified");
        }
        int numStreams = remaining / 2;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + numStreams + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (int i = 0; i < numStreams; i++) {
                byte[] streamKey = argv[argIdx + i];
                String afterId   = toStr(argv[argIdx + numStreams + i]);
                if ("$".equals(afterId)) {
                    // $ means "only new entries" — return empty for now
                    out.write(("*2\r\n").getBytes(StandardCharsets.UTF_8));
                    out.write(RespEncoder.encodeBulkString(streamKey));
                    out.write(RespEncoder.EMPTY_ARRAY);
                    continue;
                }
                StreamObject stream = getStream(db(client), streamKey);
                List<StreamEntry> entries = stream == null
                    ? new ArrayList<>()
                    : stream.read(afterId, count);

                out.write(("*2\r\n").getBytes(StandardCharsets.UTF_8));
                out.write(RespEncoder.encodeBulkString(streamKey));
                out.write(encodeEntries(entries));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- XDEL ----

    @RedisCommand(name = "xdel", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xdel(RedisClient client, byte[][] argv) {
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return RespEncoder.ZERO;

        List<String> ids = new ArrayList<>();
        for (int i = 2; i < argv.length; i++) ids.add(toStr(argv[i]));
        return RespEncoder.encodeInteger(stream.delete(ids));
    }

    // ---- XTRIM ----

    @RedisCommand(name = "xtrim", arity = -4, flags = "write", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xtrim(RedisClient client, byte[][] argv) {
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return RespEncoder.ZERO;

        String strategy = toStr(argv[2]).toUpperCase();
        if (!"MAXLEN".equals(strategy)) {
            return RespEncoder.encodeError("ERR unsupported XTRIM strategy");
        }
        int idx = 3;
        boolean approx = false;
        if (idx < argv.length && "~".equals(toStr(argv[idx]))) {
            approx = true;
            idx++;
        }
        long maxLen = Long.parseLong(toStr(argv[idx]));
        return RespEncoder.encodeInteger(stream.trim(maxLen, approx));
    }

    // ---- XGROUP ----

    @RedisCommand(name = "xgroup", arity = -2, flags = "write", firstKey = 2, lastKey = 2, step = 1)
    public byte[] xgroup(RedisClient client, byte[][] argv) {
        if (argv.length < 2) return RespEncoder.encodeError("ERR wrong number of arguments for 'xgroup' command");
        String subCmd = toStr(argv[1]).toUpperCase();

        switch (subCmd) {
            case "CREATE": return xgroupCreate(client, argv);
            case "SETID":  return xgroupSetId(client, argv);
            case "DESTROY": return xgroupDestroy(client, argv);
            case "CREATECONSUMER": return xgroupCreateConsumer(client, argv);
            case "DELCONSUMER": return xgroupDelConsumer(client, argv);
            default:
                return RespEncoder.encodeError("ERR unknown subcommand '" + subCmd + "' for 'xgroup'");
        }
    }

    private byte[] xgroupCreate(RedisClient client, byte[][] argv) {
        if (argv.length < 5) return RespEncoder.encodeError("ERR syntax error");
        byte[] key = argv[2];
        String groupName = toStr(argv[3]);
        String startId   = toStr(argv[4]);
        boolean mkstream = argv.length >= 6 && toStr(argv[5]).equalsIgnoreCase("MKSTREAM");

        StreamObject stream = mkstream ? getOrCreateStream(db(client), key) : getStream(db(client), key);
        if (stream == null) return RespEncoder.encodeError("ERR no such key");
        if (stream.getGroups().containsKey(groupName)) {
            return RespEncoder.encodeError("BUSYGROUP Consumer Group '" + groupName + "' already exists");
        }

        long millis = 0, seq = 0;
        if ("$".equals(startId)) {
            millis = stream.getLastMillis();
            seq    = stream.getLastSeq();
        } else if (!"-".equals(startId) && !"0".equals(startId)) {
            long[] parts = StreamObject.parseId(startId);
            millis = parts[0];
            seq    = parts[1];
        }
        stream.getGroups().put(groupName, new StreamConsumerGroup(groupName, millis, seq));
        return RespEncoder.OK;
    }

    private byte[] xgroupSetId(RedisClient client, byte[][] argv) {
        if (argv.length < 5) return RespEncoder.encodeError("ERR syntax error");
        StreamObject stream = getStream(db(client), argv[2]);
        if (stream == null) return RespEncoder.encodeError("ERR no such key");
        String groupName = toStr(argv[3]);
        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return RespEncoder.encodeError("ERR no such consumer group '" + groupName + "'");
        String startId = toStr(argv[4]);
        long[] parts = StreamObject.parseId(startId);
        group.setLastDelivered(parts[0], parts[1]);
        return RespEncoder.OK;
    }

    private byte[] xgroupDestroy(RedisClient client, byte[][] argv) {
        if (argv.length < 4) return RespEncoder.encodeError("ERR syntax error");
        StreamObject stream = getStream(db(client), argv[2]);
        if (stream == null) return RespEncoder.ZERO;
        String groupName = toStr(argv[3]);
        return stream.getGroups().remove(groupName) != null ? RespEncoder.ONE : RespEncoder.ZERO;
    }

    private byte[] xgroupCreateConsumer(RedisClient client, byte[][] argv) {
        if (argv.length < 5) return RespEncoder.encodeError("ERR syntax error");
        StreamObject stream = getStream(db(client), argv[2]);
        if (stream == null) return RespEncoder.encodeError("ERR no such key");
        String groupName = toStr(argv[3]);
        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return RespEncoder.encodeError("ERR no such consumer group");
        String consumerName = toStr(argv[4]);
        if (group.getConsumers().containsKey(consumerName)) return RespEncoder.ZERO;
        group.getOrCreateConsumer(consumerName);
        return RespEncoder.ONE;
    }

    private byte[] xgroupDelConsumer(RedisClient client, byte[][] argv) {
        if (argv.length < 5) return RespEncoder.encodeError("ERR syntax error");
        StreamObject stream = getStream(db(client), argv[2]);
        if (stream == null) return RespEncoder.ZERO;
        String groupName = toStr(argv[3]);
        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return RespEncoder.ZERO;
        String consumerName = toStr(argv[4]);
        StreamConsumerGroup.StreamConsumer consumer = group.getConsumers().remove(consumerName);
        if (consumer == null) return RespEncoder.ZERO;
        // Remove PEL entries for this consumer
        group.getPel().entrySet().removeIf(e -> e.getValue().getConsumerName().equals(consumerName));
        return RespEncoder.encodeInteger(consumer.getPelCount());
    }

    // ---- XREADGROUP ----

    @RedisCommand(name = "xreadgroup", arity = -7, flags = "write", firstKey = 0, lastKey = 0, step = 0)
    public byte[] xreadgroup(RedisClient client, byte[][] argv) {
        // XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]
        if (argv.length < 7) return RespEncoder.encodeError("ERR wrong number of arguments");
        if (!toStr(argv[1]).equalsIgnoreCase("GROUP")) return RespEncoder.encodeError("ERR syntax error");
        String groupName    = toStr(argv[2]);
        String consumerName = toStr(argv[3]);

        int count = 0;
        boolean noack = false;
        int argIdx = 4;
        while (argIdx < argv.length) {
            String opt = toStr(argv[argIdx]).toUpperCase();
            if ("COUNT".equals(opt)) {
                count = Integer.parseInt(toStr(argv[++argIdx]));
                argIdx++;
            } else if ("BLOCK".equals(opt)) {
                argIdx += 2;
            } else if ("NOACK".equals(opt)) {
                noack = true;
                argIdx++;
            } else if ("STREAMS".equals(opt)) {
                argIdx++;
                break;
            } else {
                argIdx++;
            }
        }

        int remaining = argv.length - argIdx;
        if (remaining % 2 != 0) return RespEncoder.encodeError("ERR Unbalanced STREAMS");
        int numStreams = remaining / 2;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + numStreams + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (int i = 0; i < numStreams; i++) {
                byte[] streamKey = argv[argIdx + i];
                String afterId   = toStr(argv[argIdx + numStreams + i]);
                StreamObject stream = getStream(db(client), streamKey);
                List<StreamEntry> entries = new ArrayList<>();

                if (stream != null) {
                    StreamConsumerGroup group = stream.getGroups().get(groupName);
                    if (group != null) {
                        StreamConsumerGroup.StreamConsumer consumer = group.getOrCreateConsumer(consumerName);
                        consumer.touch();

                        // Jedis 5.x sends NEW_ENTRY as "*" (same semantics as ">")
                        if (">".equals(afterId) || "*".equals(afterId)) {
                            // Deliver new entries
                            String lastId = group.getLastDeliveredMillis() + "-" + group.getLastDeliveredSeq();
                            entries = stream.read(lastId, count);
                            long now = System.currentTimeMillis();
                            for (StreamEntry e : entries) {
                                if (!noack) {
                                    group.getPel().put(e.getId(),
                                        new StreamConsumerGroup.PelEntry(e.getId(), consumerName, now));
                                    consumer.incPel();
                                }
                                group.setLastDelivered(e.getMillis(), e.getSeq());
                            }
                        } else {
                            // Re-deliver PEL entries
                            long[] parts = StreamObject.parseId(afterId);
                            long now = System.currentTimeMillis();
                            for (StreamConsumerGroup.PelEntry pel : group.getPel().values()) {
                                long[] pid = StreamObject.parseId(pel.getEntryId());
                                if (StreamObject.compareIds(pel.getEntryId(), afterId) > 0) {
                                    StreamEntry e = stream.getEntry(pel.getEntryId());
                                    if (e != null) {
                                        entries.add(e);
                                        pel.incrementDelivery(now);
                                    }
                                }
                                if (count > 0 && entries.size() >= count) break;
                            }
                        }
                    }
                }

                out.write(("*2\r\n").getBytes(StandardCharsets.UTF_8));
                out.write(RespEncoder.encodeBulkString(streamKey));
                out.write(encodeEntries(entries));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- XACK ----

    @RedisCommand(name = "xack", arity = -4, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xack(RedisClient client, byte[][] argv) {
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return RespEncoder.ZERO;
        String groupName = toStr(argv[2]);
        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return RespEncoder.ZERO;

        long acked = 0;
        for (int i = 3; i < argv.length; i++) {
            String id = toStr(argv[i]);
            StreamConsumerGroup.PelEntry pel = group.getPel().remove(id);
            if (pel != null) {
                StreamConsumerGroup.StreamConsumer consumer = group.getConsumers().get(pel.getConsumerName());
                if (consumer != null) consumer.decPel();
                acked++;
            }
        }
        return RespEncoder.encodeInteger(acked);
    }

    // ---- XCLAIM ----

    @RedisCommand(name = "xclaim", arity = -6, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xclaim(RedisClient client, byte[][] argv) {
        // XCLAIM key group consumer min-idle-time id [id ...]
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return RespEncoder.EMPTY_ARRAY;
        String groupName    = toStr(argv[2]);
        String consumerName = toStr(argv[3]);
        long minIdle        = Long.parseLong(toStr(argv[4]));
        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return RespEncoder.EMPTY_ARRAY;

        long now = System.currentTimeMillis();
        List<StreamEntry> claimed = new ArrayList<>();
        for (int i = 5; i < argv.length; i++) {
            String id = toStr(argv[i]);
            StreamConsumerGroup.PelEntry pel = group.getPel().get(id);
            if (pel != null && (now - pel.getDeliveryTime()) >= minIdle) {
                // Re-assign to new consumer
                group.getPel().put(id,
                    new StreamConsumerGroup.PelEntry(id, consumerName, now));
                StreamEntry e = stream.getEntry(id);
                if (e != null) claimed.add(e);
            }
        }
        return encodeEntries(claimed);
    }

    // ---- XAUTOCLAIM ----

    @RedisCommand(name = "xautoclaim", arity = -7, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xautoclaim(RedisClient client, byte[][] argv) {
        // XAUTOCLAIM key group consumer min-idle-time start [COUNT count]
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return encodeAutoclaimResult("0-0", new ArrayList<>());
        String groupName    = toStr(argv[2]);
        String consumerName = toStr(argv[3]);
        long minIdle        = Long.parseLong(toStr(argv[4]));
        String startId      = toStr(argv[5]);
        int count = 100;
        if (argv.length >= 8 && toStr(argv[6]).equalsIgnoreCase("COUNT")) {
            count = Integer.parseInt(toStr(argv[7]));
        }

        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return encodeAutoclaimResult("0-0", new ArrayList<>());

        long now = System.currentTimeMillis();
        List<StreamEntry> claimed = new ArrayList<>();
        String nextId = "0-0";
        int processed = 0;

        for (Map.Entry<String, StreamConsumerGroup.PelEntry> e : group.getPel().entrySet()) {
            if (StreamObject.compareIds(e.getKey(), startId) < 0) continue;
            if (processed >= count) {
                nextId = e.getKey();
                break;
            }
            StreamConsumerGroup.PelEntry pel = e.getValue();
            if ((now - pel.getDeliveryTime()) >= minIdle) {
                group.getPel().put(e.getKey(),
                    new StreamConsumerGroup.PelEntry(e.getKey(), consumerName, now));
                StreamEntry se = stream.getEntry(e.getKey());
                if (se != null) claimed.add(se);
            }
            processed++;
        }
        return encodeAutoclaimResult(nextId, claimed);
    }

    private byte[] encodeAutoclaimResult(String nextId, List<StreamEntry> entries) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
            out.write(RespEncoder.encodeBulkString(toBytes(nextId)));
            out.write(encodeEntries(entries));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- XPENDING ----

    @RedisCommand(name = "xpending", arity = -3, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] xpending(RedisClient client, byte[][] argv) {
        StreamObject stream = getStream(db(client), argv[1]);
        if (stream == null) return RespEncoder.encodeError("ERR no such key");
        String groupName = toStr(argv[2]);
        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return RespEncoder.encodeError("ERR no such consumer group");

        // Summary form: XPENDING key group
        if (argv.length == 3) {
            return encodePendingSummary(group);
        }

        // Extended form: XPENDING key group [IDLE min-idle-time] start end count [consumer]
        int argIdx = 3;
        long minIdle = 0;
        if (toStr(argv[argIdx]).equalsIgnoreCase("IDLE")) {
            minIdle = Long.parseLong(toStr(argv[++argIdx]));
            argIdx++;
        }
        String startId = toStr(argv[argIdx++]);
        String endId   = toStr(argv[argIdx++]);
        int count      = Integer.parseInt(toStr(argv[argIdx++]));
        String filterConsumer = argIdx < argv.length ? toStr(argv[argIdx]) : null;

        long now = System.currentTimeMillis();
        List<StreamConsumerGroup.PelEntry> result = new ArrayList<>();
        for (StreamConsumerGroup.PelEntry pel : group.getPel().values()) {
            if (StreamObject.compareIds(pel.getEntryId(), startId) < 0) continue;
            if (StreamObject.compareIds(pel.getEntryId(), endId) > 0) break;
            if (filterConsumer != null && !filterConsumer.equals(pel.getConsumerName())) continue;
            if (minIdle > 0 && (now - pel.getDeliveryTime()) < minIdle) continue;
            result.add(pel);
            if (result.size() >= count) break;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + result.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (StreamConsumerGroup.PelEntry pel : result) {
                out.write("*4\r\n".getBytes(StandardCharsets.UTF_8));
                out.write(RespEncoder.encodeBulkString(toBytes(pel.getEntryId())));
                out.write(RespEncoder.encodeBulkString(toBytes(pel.getConsumerName())));
                out.write(RespEncoder.encodeInteger(now - pel.getDeliveryTime()));
                out.write(RespEncoder.encodeInteger(pel.getDeliveryCount()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    private byte[] encodePendingSummary(StreamConsumerGroup group) {
        long count = group.getPel().size();
        String minId = null, maxId = null;
        for (String id : group.getPel().keySet()) {
            if (minId == null || StreamObject.compareIds(id, minId) < 0) minId = id;
            if (maxId == null || StreamObject.compareIds(id, maxId) > 0) maxId = id;
        }

        // Count per consumer
        Map<String, Integer> perConsumer = new LinkedHashMap<>();
        for (StreamConsumerGroup.PelEntry pel : group.getPel().values()) {
            perConsumer.merge(pel.getConsumerName(), 1, Integer::sum);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write("*4\r\n".getBytes(StandardCharsets.UTF_8));
            out.write(RespEncoder.encodeInteger(count));
            out.write(RespEncoder.encodeBulkString(minId == null ? null : toBytes(minId)));
            out.write(RespEncoder.encodeBulkString(maxId == null ? null : toBytes(maxId)));
            out.write(("*" + perConsumer.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<String, Integer> e : perConsumer.entrySet()) {
                out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
                out.write(RespEncoder.encodeBulkString(toBytes(e.getKey())));
                out.write(RespEncoder.encodeBulkString(toBytes(String.valueOf(e.getValue()))));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- XINFO ----

    @RedisCommand(name = "xinfo", arity = -2, flags = "read-only", firstKey = 2, lastKey = 2, step = 1)
    public byte[] xinfo(RedisClient client, byte[][] argv) {
        if (argv.length < 2) return RespEncoder.encodeError("ERR wrong number of arguments");
        String subCmd = toStr(argv[1]).toUpperCase();

        switch (subCmd) {
            case "STREAM": return xinfoStream(client, argv);
            case "GROUPS": return xinfoGroups(client, argv);
            case "CONSUMERS": return xinfoConsumers(client, argv);
            default:
                return RespEncoder.encodeError("ERR unknown subcommand '" + subCmd + "' for 'xinfo'");
        }
    }

    private byte[] xinfoStream(RedisClient client, byte[][] argv) {
        if (argv.length < 3) return RespEncoder.encodeError("ERR syntax error");
        StreamObject stream = getStream(db(client), argv[2]);
        if (stream == null) return RespEncoder.encodeError("ERR no such key");

        List<Object> items = new ArrayList<>();
        items.add("length");       items.add((long) stream.size());
        items.add("groups");       items.add((long) stream.getGroups().size());
        items.add("last-generated-id"); items.add(stream.getLastMillis() + "-" + stream.getLastSeq());
        items.add("first-entry");  items.add(stream.firstEntryId());
        items.add("last-entry");   items.add(stream.lastEntryId());
        return RespEncoder.encodeArray(items);
    }

    private byte[] xinfoGroups(RedisClient client, byte[][] argv) {
        if (argv.length < 3) return RespEncoder.encodeError("ERR syntax error");
        StreamObject stream = getStream(db(client), argv[2]);
        if (stream == null) return RespEncoder.encodeError("ERR no such key");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + stream.getGroups().size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (StreamConsumerGroup g : stream.getGroups().values()) {
                List<Object> info = new ArrayList<>();
                info.add("name");      info.add(g.getName());
                info.add("consumers"); info.add((long) g.getConsumers().size());
                info.add("pending");   info.add((long) g.getPel().size());
                info.add("last-delivered-id"); info.add(g.getLastDeliveredMillis() + "-" + g.getLastDeliveredSeq());
                out.write(RespEncoder.encodeArray(info));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    private byte[] xinfoConsumers(RedisClient client, byte[][] argv) {
        if (argv.length < 4) return RespEncoder.encodeError("ERR syntax error");
        StreamObject stream = getStream(db(client), argv[2]);
        if (stream == null) return RespEncoder.encodeError("ERR no such key");
        String groupName = toStr(argv[3]);
        StreamConsumerGroup group = stream.getGroups().get(groupName);
        if (group == null) return RespEncoder.encodeError("ERR no such consumer group");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + group.getConsumers().size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            long now = System.currentTimeMillis();
            for (StreamConsumerGroup.StreamConsumer c : group.getConsumers().values()) {
                List<Object> info = new ArrayList<>();
                info.add("name");    info.add(c.getName());
                info.add("pending"); info.add((long) c.getPelCount());
                info.add("idle");    info.add(now - c.getSeenTime());
                out.write(RespEncoder.encodeArray(info));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    // ---- Encoding helpers ----

    /** Encode a list of StreamEntry as RESP array of [id, [field, value, ...]] pairs. */
    static byte[] encodeEntries(List<StreamEntry> entries) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + entries.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (StreamEntry e : entries) {
                out.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
                out.write(RespEncoder.encodeBulkString(toBytes(e.getId())));
                // Fields as flat array
                int numFields = e.getFields().size() * 2;
                out.write(("*" + numFields + "\r\n").getBytes(StandardCharsets.UTF_8));
                for (Map.Entry<String, String> f : e.getFields().entrySet()) {
                    out.write(RespEncoder.encodeBulkString(toBytes(f.getKey())));
                    out.write(RespEncoder.encodeBulkString(toBytes(f.getValue())));
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return out.toByteArray();
    }

}
