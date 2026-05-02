package com.redisimpl.server.commands.generic;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.db.RedisDb;
import com.redisimpl.server.resp.RespEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Generic (key-agnostic) command implementations.
 */
public final class GenericCommands {

    private final RedisServer server;

    public GenericCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) { return server.getDb(client.getDb()); }
    private static String toStr(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    private static byte[] toBytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @RedisCommand(name = "del", arity = -2, flags = "write", firstKey = 1, lastKey = -1, step = 1)
    public byte[] del(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        long deleted = 0;
        for (int i = 1; i < argv.length; i++) if (db.delete(argv[i])) deleted++;
        return RespEncoder.encodeInteger(deleted);
    }

    /**
     * UNLINK key [key ...] — mirrors unlinkCommand() in db.c + lazyfree.c.
     *
     * Immediately removes keys from the keyspace (like DEL) but defers freeing
     * large collection values to the BIO lazy-free thread (mirrors dbAsyncDelete +
     * freeObjAsync with LAZYFREE_THRESHOLD=64).
     */
    @RedisCommand(name = "unlink", arity = -2, flags = "write fast", firstKey = 1, lastKey = -1, step = 1)
    public byte[] unlink(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        long deleted = 0;
        for (int i = 1; i < argv.length; i++) {
            if (db.asyncDelete(argv[i])) deleted++;
        }
        return RespEncoder.encodeInteger(deleted);
    }

    @RedisCommand(name = "exists", arity = -2, flags = "read-only fast", firstKey = 1, lastKey = -1, step = 1)
    public byte[] exists(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        long count = 0;
        for (int i = 1; i < argv.length; i++) {
            if (db.lookupKey(argv[i]) != null) count++;
        }
        return RespEncoder.encodeInteger(count);
    }

    @RedisCommand(name = "type", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] type(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKey(argv[1]);
        if (obj == null) return RespEncoder.encodeSimpleString("none");
        return RespEncoder.encodeSimpleString(obj.typeName());
    }

    @RedisCommand(name = "keys", arity = 2, flags = "read-only sort_for_script", firstKey = 0, lastKey = 0, step = 0)
    public byte[] keys(RedisClient client, byte[][] argv) {
        String pattern = toStr(argv[1]);
        List<byte[]> keys = db(client).keys(pattern);
        List<Object> result = new ArrayList<>(keys);
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "scan", arity = -2, flags = "read-only random", firstKey = 0, lastKey = 0, step = 0)
    public byte[] scan(RedisClient client, byte[][] argv) {
        long cursor;
        try { cursor = Long.parseLong(toStr(argv[1])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        String pattern = null;
        int count = 10;
        for (int i = 2; i < argv.length; i++) {
            String opt = toStr(argv[i]).toUpperCase();
            if (opt.equals("MATCH") && i + 1 < argv.length) { pattern = toStr(argv[++i]); }
            else if (opt.equals("COUNT") && i + 1 < argv.length) {
                try { count = (int) Long.parseLong(toStr(argv[++i])); }
                catch (NumberFormatException e) { throw RedisException.notInteger(); }
            }
        }
        RedisDb.ScanResult result = db(client).scan(cursor, pattern, count);
        List<Object> response = new ArrayList<>();
        response.add(toBytes(String.valueOf(result.cursor)));
        List<Object> keys = new ArrayList<>(result.keys);
        response.add(RespEncoder.encodeArray(keys));
        return RespEncoder.encodeArray(response);
    }

    @RedisCommand(name = "rename", arity = 3, flags = "write", firstKey = 1, lastKey = 2, step = 1)
    public byte[] rename(RedisClient client, byte[][] argv) {
        db(client).rename(argv[1], argv[2]);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "renamenx", arity = 3, flags = "write fast", firstKey = 1, lastKey = 2, step = 1)
    public byte[] renamenx(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        if (db.lookupKey(argv[2]) != null) return RespEncoder.ZERO;
        db.rename(argv[1], argv[2]);
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "expire", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] expire(RedisClient client, byte[][] argv) {
        long seconds;
        try { seconds = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        return setExpiry(client, argv[1], seconds * 1000);
    }

    @RedisCommand(name = "pexpire", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] pexpire(RedisClient client, byte[][] argv) {
        long ms;
        try { ms = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        return setExpiry(client, argv[1], ms);
    }

    @RedisCommand(name = "expireat", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] expireat(RedisClient client, byte[][] argv) {
        long unixSec;
        try { unixSec = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        return setExpiryAbsolute(client, argv[1], unixSec * 1000);
    }

    @RedisCommand(name = "pexpireat", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] pexpireat(RedisClient client, byte[][] argv) {
        long unixMs;
        try { unixMs = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        return setExpiryAbsolute(client, argv[1], unixMs);
    }

    private byte[] setExpiry(RedisClient client, byte[] key, long relativeMs) {
        RedisDb db = db(client);
        if (db.lookupKey(key) == null) return RespEncoder.ZERO;
        db.setExpiry(key, System.currentTimeMillis() + relativeMs);
        return RespEncoder.ONE;
    }

    private byte[] setExpiryAbsolute(RedisClient client, byte[] key, long absoluteMs) {
        RedisDb db = db(client);
        if (db.lookupKey(key) == null) return RespEncoder.ZERO;
        db.setExpiry(key, absoluteMs);
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "ttl", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] ttl(RedisClient client, byte[][] argv) {
        long exp = db(client).getExpiry(argv[1]);
        if (exp == -2) return RespEncoder.encodeInteger(-2);
        if (exp == -1) return RespEncoder.encodeInteger(-1);
        long ttl = (exp - System.currentTimeMillis()) / 1000;
        return RespEncoder.encodeInteger(Math.max(ttl, 0));
    }

    @RedisCommand(name = "pttl", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] pttl(RedisClient client, byte[][] argv) {
        long exp = db(client).getExpiry(argv[1]);
        if (exp == -2) return RespEncoder.encodeInteger(-2);
        if (exp == -1) return RespEncoder.encodeInteger(-1);
        long ttl = exp - System.currentTimeMillis();
        return RespEncoder.encodeInteger(Math.max(ttl, 0));
    }

    @RedisCommand(name = "persist", arity = 2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] persist(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        if (db.getExpiry(argv[1]) == -1 || db.getExpiry(argv[1]) == -2) return RespEncoder.ZERO;
        db.removeExpiry(argv[1]);
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "select", arity = 2, flags = "fast loading stale", firstKey = 0, lastKey = 0, step = 0)
    public byte[] select(RedisClient client, byte[][] argv) {
        int idx;
        try { idx = (int) Long.parseLong(toStr(argv[1])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (idx < 0 || idx >= server.getNumDatabases()) throw RedisException.dbIndex();
        client.setDb(idx);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "dbsize", arity = 1, flags = "read-only fast", firstKey = 0, lastKey = 0, step = 0)
    public byte[] dbsize(RedisClient client, byte[][] argv) {
        return RespEncoder.encodeInteger(db(client).dbSize());
    }

    @RedisCommand(name = "flushdb", arity = -1, flags = "write", firstKey = 0, lastKey = 0, step = 0)
    public byte[] flushdb(RedisClient client, byte[][] argv) {
        db(client).flush();
        return RespEncoder.OK;
    }

    @RedisCommand(name = "flushall", arity = -1, flags = "write", firstKey = 0, lastKey = 0, step = 0)
    public byte[] flushall(RedisClient client, byte[][] argv) {
        for (RedisDb db : server.getDbs()) db.flush();
        return RespEncoder.OK;
    }

    @RedisCommand(name = "randomkey", arity = 1, flags = "read-only random", firstKey = 0, lastKey = 0, step = 0)
    public byte[] randomkey(RedisClient client, byte[][] argv) {
        byte[] key = db(client).randomKey();
        return key != null ? RespEncoder.encodeBulkString(key) : RespEncoder.NULL_BULK;
    }

    @RedisCommand(name = "move", arity = 3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] move(RedisClient client, byte[][] argv) {
        int targetIdx;
        try { targetIdx = (int) Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (targetIdx < 0 || targetIdx >= server.getNumDatabases()) throw RedisException.dbIndex();
        RedisDb src = db(client);
        RedisDb dst = server.getDb(targetIdx);
        RedisObject obj = src.lookupKey(argv[1]);
        if (obj == null) return RespEncoder.ZERO;
        if (dst.lookupKey(argv[1]) != null) return RespEncoder.ZERO;
        Long expiry = src.getExpiry(argv[1]);
        src.delete(argv[1]);
        dst.setKey(argv[1], obj);
        if (expiry > 0) dst.setExpiry(argv[1], expiry);
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "copy", arity = -3, flags = "write", firstKey = 1, lastKey = 2, step = 1)
    public byte[] copy(RedisClient client, byte[][] argv) {
        RedisDb src = db(client), dst = db(client);
        boolean replace = false;
        for (int i = 3; i < argv.length; i++) {
            String opt = toStr(argv[i]).toUpperCase();
            if (opt.equals("REPLACE")) replace = true;
            else if (opt.equals("DB") && i + 1 < argv.length) {
                int dbIdx;
                try { dbIdx = (int) Long.parseLong(toStr(argv[++i])); }
                catch (NumberFormatException e) { throw RedisException.notInteger(); }
                dst = server.getDb(dbIdx);
            }
        }
        RedisObject obj = src.lookupKey(argv[1]);
        if (obj == null) return RespEncoder.ZERO;
        if (dst.lookupKey(argv[2]) != null && !replace) return RespEncoder.ZERO;
        dst.delete(argv[2]);
        dst.setKey(argv[2], obj);
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "touch", arity = -2, flags = "read-only fast", firstKey = 1, lastKey = -1, step = 1)
    public byte[] touch(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        long count = 0;
        for (int i = 1; i < argv.length; i++) if (db.lookupKey(argv[i]) != null) count++;
        return RespEncoder.encodeInteger(count);
    }

    @RedisCommand(name = "object", arity = -2, flags = "slow", firstKey = 2, lastKey = 2, step = 1)
    public byte[] object(RedisClient client, byte[][] argv) {
        String subCmd = toStr(argv[1]).toUpperCase();
        switch (subCmd) {
            case "ENCODING": {
                RedisObject obj = db(client).lookupKey(argv[2]);
                if (obj == null) throw new RedisException(RedisException.ERR_NO_SUCH_KEY);
                return RespEncoder.encodeBulkString(toBytes(obj.encodingName()));
            }
            case "REFCOUNT": {
                RedisObject obj = db(client).lookupKey(argv[2]);
                if (obj == null) throw new RedisException(RedisException.ERR_NO_SUCH_KEY);
                return RespEncoder.encodeInteger(obj.getRefcount());
            }
            case "IDLETIME": {
                RedisObject obj = db(client).lookupKey(argv[2]);
                if (obj == null) throw new RedisException(RedisException.ERR_NO_SUCH_KEY);
                return RespEncoder.encodeInteger(0); // simplified
            }
            case "FREQ": {
                RedisObject obj = db(client).lookupKey(argv[2]);
                if (obj == null) throw new RedisException(RedisException.ERR_NO_SUCH_KEY);
                return RespEncoder.encodeInteger(0);
            }
            case "HELP":
                return RespEncoder.encodeArray(java.util.Arrays.asList(
                        toBytes("OBJECT <subcommand> [<arg> [value] [opt] ...]. subcommands are:"),
                        toBytes("ENCODING <key>"),
                        toBytes("REFCOUNT <key>"),
                        toBytes("IDLETIME <key>"),
                        toBytes("FREQ <key>")));
            default:
                throw new RedisException("ERR unknown subcommand or wrong number of arguments for '" + subCmd + "' command");
        }
    }

    @RedisCommand(name = "wait", arity = 3, flags = "noscript", firstKey = 0, lastKey = 0, step = 0)
    public byte[] wait(RedisClient client, byte[][] argv) {
        return RespEncoder.ZERO; // No replicas in single-node mode
    }

    @RedisCommand(name = "dump", arity = 2, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] dump(RedisClient client, byte[][] argv) {
        // Simplified: not implementing full RDB serialization
        return RespEncoder.NULL_BULK;
    }

    @RedisCommand(name = "restore", arity = -4, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] restore(RedisClient client, byte[][] argv) {
        throw new RedisException("ERR DUMP payload version or checksum are wrong");
    }

    @RedisCommand(name = "expiretime", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] expiretime(RedisClient client, byte[][] argv) {
        long exp = db(client).getExpiry(argv[1]);
        if (exp == -2) return RespEncoder.encodeInteger(-2);
        if (exp == -1) return RespEncoder.encodeInteger(-1);
        return RespEncoder.encodeInteger(exp / 1000);
    }

    @RedisCommand(name = "pexpiretime", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] pexpiretime(RedisClient client, byte[][] argv) {
        long exp = db(client).getExpiry(argv[1]);
        if (exp == -2) return RespEncoder.encodeInteger(-2);
        if (exp == -1) return RespEncoder.encodeInteger(-1);
        return RespEncoder.encodeInteger(exp);
    }
}
