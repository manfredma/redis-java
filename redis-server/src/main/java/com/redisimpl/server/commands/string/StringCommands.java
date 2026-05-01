package com.redisimpl.server.commands.string;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.sds.Sds;
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
 * String command implementations.
 */
public final class StringCommands {

    private final RedisServer server;

    public StringCommands(RedisServer server) {
        this.server = server;
    }

    // ---- Helper methods ----

    private RedisDb db(RedisClient client) {
        return server.getDb(client.getDb());
    }

    private static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String toStr(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    /**
     * Create a string RedisObject with appropriate encoding.
     * INT if it fits in a long, EMBSTR if <=44 bytes, else RAW.
     */
    public static RedisObject createStringObject(byte[] value) {
        // Try INT encoding
        try {
            long l = Long.parseLong(toStr(value));
            return RedisObject.createObject(
                    RedisObjectConstants.OBJ_TYPE_STRING,
                    RedisObjectConstants.OBJ_ENCODING_INT,
                    l);
        } catch (NumberFormatException ignored) {}

        // EMBSTR or RAW
        int encoding = value.length <= RedisObjectConstants.OBJ_ENCODING_EMBSTR_SIZE_LIMIT
                ? RedisObjectConstants.OBJ_ENCODING_EMBSTR
                : RedisObjectConstants.OBJ_ENCODING_RAW;
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                encoding,
                Sds.fromBytes(value));
    }

    /**
     * Get the byte[] value of a string object.
     */
    public static byte[] getStringValue(RedisObject obj) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INT) {
            return String.valueOf((Long) obj.getPtr()).getBytes(StandardCharsets.UTF_8);
        }
        return ((Sds) obj.getPtr()).toBytes();
    }

    /**
     * Get the string value of a string object.
     */
    public static String getStringStr(RedisObject obj) {
        return toStr(getStringValue(obj));
    }

    /**
     * Try to parse a long from a string object.
     */
    private static long getLongValue(RedisObject obj) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INT) {
            return (Long) obj.getPtr();
        }
        try {
            return Long.parseLong(getStringStr(obj));
        } catch (NumberFormatException e) {
            throw RedisException.notInteger();
        }
    }

    private static void checkStringType(RedisObject obj) {
        if (obj != null && obj.getType() != RedisObjectConstants.OBJ_TYPE_STRING) {
            throw RedisException.wrongType();
        }
    }

    // ---- Commands ----

    @RedisCommand(name = "get", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] get(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        if (obj == null) return RespEncoder.NULL_BULK;
        return RespEncoder.encodeBulkString(getStringValue(obj));
    }

    @RedisCommand(name = "set", arity = -3, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] set(RedisClient client, byte[][] argv) {
        byte[] key = argv[1];
        byte[] value = argv[2];
        RedisDb db = db(client);

        // Parse options
        Long expiryMs = null;
        boolean nx = false, xx = false, get = false, keepttl = false;

        for (int i = 3; i < argv.length; i++) {
            String opt = toStr(argv[i]).toUpperCase();
            switch (opt) {
                case "EX":
                    if (i + 1 >= argv.length) throw RedisException.syntax();
                    try { expiryMs = Long.parseLong(toStr(argv[++i])) * 1000; }
                    catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    if (expiryMs <= 0) throw new RedisException("ERR invalid expire time in 'set' command");
                    break;
                case "PX":
                    if (i + 1 >= argv.length) throw RedisException.syntax();
                    try { expiryMs = Long.parseLong(toStr(argv[++i])); }
                    catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    if (expiryMs <= 0) throw new RedisException("ERR invalid expire time in 'set' command");
                    break;
                case "EXAT":
                    if (i + 1 >= argv.length) throw RedisException.syntax();
                    try { expiryMs = (Long.parseLong(toStr(argv[++i])) - System.currentTimeMillis() / 1000) * 1000; }
                    catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    break;
                case "PXAT":
                    if (i + 1 >= argv.length) throw RedisException.syntax();
                    try { expiryMs = Long.parseLong(toStr(argv[++i])) - System.currentTimeMillis(); }
                    catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    break;
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "GET": get = true; break;
                case "KEEPTTL": keepttl = true; break;
                default: throw RedisException.syntax();
            }
        }

        RedisObject existing = db.lookupKey(key);
        if (existing != null && existing.getType() != RedisObjectConstants.OBJ_TYPE_STRING && get) {
            throw RedisException.wrongType();
        }

        byte[] oldValue = null;
        if (get && existing != null) {
            checkStringType(existing);
            oldValue = getStringValue(existing);
        }

        if (nx && existing != null) return get ? RespEncoder.NULL_BULK : RespEncoder.NULL_BULK;
        if (xx && existing == null) return get ? RespEncoder.NULL_BULK : RespEncoder.NULL_BULK;

        Long currentExpiry = keepttl ? db.getExpiry(key) : null;

        RedisObject obj = createStringObject(value);
        db.setKey(key, obj);

        if (keepttl && currentExpiry != null && currentExpiry > 0) {
            db.setExpiry(key, currentExpiry);
        } else if (expiryMs != null) {
            db.setExpiry(key, System.currentTimeMillis() + expiryMs);
        }

        if (get) {
            return oldValue != null ? RespEncoder.encodeBulkString(oldValue) : RespEncoder.NULL_BULK;
        }
        return RespEncoder.OK;
    }

    @RedisCommand(name = "mget", arity = -2, flags = "read-only fast", firstKey = 1, lastKey = -1, step = 1)
    public byte[] mget(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        List<Object> results = new ArrayList<>();
        for (int i = 1; i < argv.length; i++) {
            RedisObject obj = db.lookupKey(argv[i]);
            if (obj == null || obj.getType() != RedisObjectConstants.OBJ_TYPE_STRING) {
                results.add(null);
            } else {
                results.add(getStringValue(obj));
            }
        }
        return RespEncoder.encodeArray(results);
    }

    @RedisCommand(name = "mset", arity = -3, flags = "write denyoom", firstKey = 1, lastKey = -1, step = 2)
    public byte[] mset(RedisClient client, byte[][] argv) {
        if ((argv.length - 1) % 2 != 0) throw RedisException.syntax();
        RedisDb db = db(client);
        for (int i = 1; i < argv.length; i += 2) {
            db.setKey(argv[i], createStringObject(argv[i + 1]));
        }
        return RespEncoder.OK;
    }

    @RedisCommand(name = "msetnx", arity = -3, flags = "write denyoom", firstKey = 1, lastKey = -1, step = 2)
    public byte[] msetnx(RedisClient client, byte[][] argv) {
        if ((argv.length - 1) % 2 != 0) throw RedisException.syntax();
        RedisDb db = db(client);
        // Check all keys don't exist
        for (int i = 1; i < argv.length; i += 2) {
            if (db.lookupKey(argv[i]) != null) return RespEncoder.ZERO;
        }
        for (int i = 1; i < argv.length; i += 2) {
            db.setKey(argv[i], createStringObject(argv[i + 1]));
        }
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "incr", arity = 2, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] incr(RedisClient client, byte[][] argv) {
        return incrBy(client, argv[1], 1);
    }

    @RedisCommand(name = "decr", arity = 2, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] decr(RedisClient client, byte[][] argv) {
        return incrBy(client, argv[1], -1);
    }

    @RedisCommand(name = "incrby", arity = 3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] incrby(RedisClient client, byte[][] argv) {
        long incr;
        try { incr = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        return incrBy(client, argv[1], incr);
    }

    @RedisCommand(name = "decrby", arity = 3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] decrby(RedisClient client, byte[][] argv) {
        long decr;
        try { decr = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        return incrBy(client, argv[1], -decr);
    }

    private byte[] incrBy(RedisClient client, byte[] key, long incr) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(key, RedisObjectConstants.OBJ_TYPE_STRING);
        long value = 0;
        if (obj != null) {
            value = getLongValue(obj);
        }
        // Check overflow
        if ((incr > 0 && value > Long.MAX_VALUE - incr) ||
            (incr < 0 && value < Long.MIN_VALUE - incr)) {
            throw new RedisException("ERR increment or decrement would overflow");
        }
        value += incr;
        RedisObject newObj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_INT,
                value);
        db.setKey(key, newObj);
        return RespEncoder.encodeInteger(value);
    }

    @RedisCommand(name = "incrbyfloat", arity = 3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] incrbyfloat(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        double incr;
        try { incr = Double.parseDouble(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (Double.isNaN(incr) || Double.isInfinite(incr)) throw RedisException.nanOrInf();

        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        double value = 0;
        if (obj != null) {
            try { value = Double.parseDouble(getStringStr(obj)); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }
        value += incr;
        if (Double.isNaN(value) || Double.isInfinite(value)) throw RedisException.nanOrInf();

        // Format without trailing zeros
        String result = formatDouble(value);
        byte[] resultBytes = toBytes(result);
        db.setKey(argv[1], createStringObject(resultBytes));
        return RespEncoder.encodeBulkString(resultBytes);
    }

    private static String formatDouble(double d) {
        if (d == Math.floor(d) && !Double.isInfinite(d) && Math.abs(d) < 1e15) {
            return String.valueOf((long) d);
        }
        return String.valueOf(d);
    }

    @RedisCommand(name = "append", arity = 3, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] append(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        byte[] current = obj != null ? getStringValue(obj) : new byte[0];
        byte[] appended = Arrays.copyOf(current, current.length + argv[2].length);
        System.arraycopy(argv[2], 0, appended, current.length, argv[2].length);
        db.setKey(argv[1], createStringObject(appended));
        return RespEncoder.encodeInteger(appended.length);
    }

    @RedisCommand(name = "strlen", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] strlen(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        if (obj == null) return RespEncoder.ZERO;
        return RespEncoder.encodeInteger(getStringValue(obj).length);
    }

    @RedisCommand(name = "getrange", arity = 4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] getrange(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        if (obj == null) return RespEncoder.encodeBulkString(new byte[0]);
        byte[] value = getStringValue(obj);
        int len = value.length;
        int start, end;
        try {
            start = (int) Long.parseLong(toStr(argv[2]));
            end   = (int) Long.parseLong(toStr(argv[3]));
        } catch (NumberFormatException e) { throw RedisException.notInteger(); }

        if (start < 0) start = Math.max(len + start, 0);
        if (end < 0)   end   = len + end;
        if (start > end || start >= len) return RespEncoder.encodeBulkString(new byte[0]);
        end = Math.min(end, len - 1);
        return RespEncoder.encodeBulkString(Arrays.copyOfRange(value, start, end + 1));
    }

    @RedisCommand(name = "setrange", arity = 4, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] setrange(RedisClient client, byte[][] argv) {
        int offset;
        try { offset = (int) Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (offset < 0) throw new RedisException("ERR bit offset is not an integer or out of range");

        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        byte[] current = obj != null ? getStringValue(obj) : new byte[0];
        byte[] patch = argv[3];
        int newLen = Math.max(current.length, offset + patch.length);
        byte[] result = Arrays.copyOf(current, newLen); // zero-fills extension
        System.arraycopy(patch, 0, result, offset, patch.length);
        db.setKey(argv[1], createStringObject(result));
        return RespEncoder.encodeInteger(newLen);
    }

    @RedisCommand(name = "setnx", arity = 3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] setnx(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        if (db.lookupKey(argv[1]) != null) return RespEncoder.ZERO;
        db.setKey(argv[1], createStringObject(argv[2]));
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "setex", arity = 4, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] setex(RedisClient client, byte[][] argv) {
        long seconds;
        try { seconds = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (seconds <= 0) throw new RedisException("ERR invalid expire time in 'setex' command");
        RedisDb db = db(client);
        db.setKey(argv[1], createStringObject(argv[3]));
        db.setExpiry(argv[1], System.currentTimeMillis() + seconds * 1000);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "psetex", arity = 4, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] psetex(RedisClient client, byte[][] argv) {
        long millis;
        try { millis = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (millis <= 0) throw new RedisException("ERR invalid expire time in 'psetex' command");
        RedisDb db = db(client);
        db.setKey(argv[1], createStringObject(argv[3]));
        db.setExpiry(argv[1], System.currentTimeMillis() + millis);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "getset", arity = 3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] getset(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject old = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        byte[] oldVal = old != null ? getStringValue(old) : null;
        db.setKey(argv[1], createStringObject(argv[2]));
        return oldVal != null ? RespEncoder.encodeBulkString(oldVal) : RespEncoder.NULL_BULK;
    }

    @RedisCommand(name = "getdel", arity = 2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] getdel(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        if (obj == null) return RespEncoder.NULL_BULK;
        byte[] val = getStringValue(obj);
        db.delete(argv[1]);
        return RespEncoder.encodeBulkString(val);
    }

    @RedisCommand(name = "getex", arity = -2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] getex(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_STRING);
        if (obj == null) return RespEncoder.NULL_BULK;
        byte[] val = getStringValue(obj);

        if (argv.length > 2) {
            String opt = toStr(argv[2]).toUpperCase();
            switch (opt) {
                case "EX":
                    if (argv.length < 4) throw RedisException.syntax();
                    long sec; try { sec = Long.parseLong(toStr(argv[3])); } catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    db.setExpiry(argv[1], System.currentTimeMillis() + sec * 1000);
                    break;
                case "PX":
                    if (argv.length < 4) throw RedisException.syntax();
                    long ms; try { ms = Long.parseLong(toStr(argv[3])); } catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    db.setExpiry(argv[1], System.currentTimeMillis() + ms);
                    break;
                case "EXAT":
                    if (argv.length < 4) throw RedisException.syntax();
                    long exat; try { exat = Long.parseLong(toStr(argv[3])); } catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    db.setExpiry(argv[1], exat * 1000);
                    break;
                case "PXAT":
                    if (argv.length < 4) throw RedisException.syntax();
                    long pxat; try { pxat = Long.parseLong(toStr(argv[3])); } catch (NumberFormatException e) { throw RedisException.notInteger(); }
                    db.setExpiry(argv[1], pxat);
                    break;
                case "PERSIST":
                    db.removeExpiry(argv[1]);
                    break;
                default:
                    throw RedisException.syntax();
            }
        }
        return RespEncoder.encodeBulkString(val);
    }
}
