package com.redisimpl.server.commands.hash;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.db.RedisDb;
import com.redisimpl.server.resp.RespEncoder;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Hash command implementations.
 */
public final class HashCommands {

    private final RedisServer server;

    public HashCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) { return server.getDb(client.getDb()); }
    private static String toStr(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    private static byte[] toBytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    // ---- Encoding helpers ----

    private static RedisObject getOrCreateHash(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKeyOrReply(key, RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) {
            obj = RedisObject.createObject(
                    RedisObjectConstants.OBJ_TYPE_HASH,
                    RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                    ListPack.create());
            db.setKey(key, obj);
        }
        return obj;
    }

    /** Convert listpack hash to HT if needed */
    private static void convertIfNeeded(RedisObject obj, byte[] field, byte[] value) {
        if (obj.getEncoding() != RedisObjectConstants.OBJ_ENCODING_LISTPACK) return;
        ListPack lp = (ListPack) obj.getPtr();
        if (lp.size() / 2 >= RedisObjectConstants.OBJ_HASH_MAX_LISTPACK_ENTRIES
                || (field != null && field.length > RedisObjectConstants.OBJ_HASH_MAX_LISTPACK_VALUE)
                || (value != null && value.length > RedisObjectConstants.OBJ_HASH_MAX_LISTPACK_VALUE)) {
            // Convert to HT
            Dict dict = Dict.create();
            for (int i = 0; i < lp.size(); i += 2) {
                dict.put(lp.get(i), lp.get(i + 1));
            }
            obj.setEncoding(RedisObjectConstants.OBJ_ENCODING_HT);
            obj.setPtr(dict);
        }
    }

    private static void hashSet(RedisObject obj, byte[] field, byte[] value) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            // Find existing field
            for (int i = 0; i < lp.size(); i += 2) {
                if (Arrays.equals(lp.get(i), field)) {
                    lp = lp.set(i + 1, value);
                    obj.setPtr(lp);
                    convertIfNeeded(obj, field, value);
                    return;
                }
            }
            // Not found: append field+value
            lp = lp.append(field).append(value);
            obj.setPtr(lp);
            convertIfNeeded(obj, field, value);
        } else {
            ((Dict) obj.getPtr()).put(field, value);
        }
    }

    private static byte[] hashGet(RedisObject obj, byte[] field) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i < lp.size(); i += 2) {
                if (Arrays.equals(lp.get(i), field)) return lp.get(i + 1);
            }
            return null;
        }
        return (byte[]) ((Dict) obj.getPtr()).get(field);
    }

    private static boolean hashDel(RedisObject obj, byte[] field) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i < lp.size(); i += 2) {
                if (Arrays.equals(lp.get(i), field)) {
                    lp = lp.delete(i + 1).delete(i);
                    obj.setPtr(lp);
                    return true;
                }
            }
            return false;
        }
        return ((Dict) obj.getPtr()).delete(field);
    }

    private static boolean hashExists(RedisObject obj, byte[] field) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i < lp.size(); i += 2) {
                if (Arrays.equals(lp.get(i), field)) return true;
            }
            return false;
        }
        return ((Dict) obj.getPtr()).containsKey(field);
    }

    private static long hashLen(RedisObject obj) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            return ((ListPack) obj.getPtr()).size() / 2;
        }
        return ((Dict) obj.getPtr()).size();
    }

    private static List<byte[]> hashKeys(RedisObject obj) {
        List<byte[]> keys = new ArrayList<>();
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i < lp.size(); i += 2) keys.add(lp.get(i));
        } else {
            for (Dict.Entry e : (Dict) obj.getPtr()) keys.add(e.getKey());
        }
        return keys;
    }

    private static List<byte[]> hashValues(RedisObject obj) {
        List<byte[]> vals = new ArrayList<>();
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 1; i < lp.size(); i += 2) vals.add(lp.get(i));
        } else {
            for (Dict.Entry e : (Dict) obj.getPtr()) vals.add((byte[]) e.getValue());
        }
        return vals;
    }

    private static List<byte[]> hashGetAll(RedisObject obj) {
        List<byte[]> all = new ArrayList<>();
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            all.addAll(((ListPack) obj.getPtr()).toList());
        } else {
            for (Dict.Entry e : (Dict) obj.getPtr()) {
                all.add(e.getKey());
                all.add((byte[]) e.getValue());
            }
        }
        return all;
    }

    // ---- Commands ----

    @RedisCommand(name = "hset", arity = -4, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hset(RedisClient client, byte[][] argv) {
        if ((argv.length - 2) % 2 != 0) throw RedisException.syntax();
        RedisObject obj = getOrCreateHash(db(client), argv[1]);
        int added = 0;
        for (int i = 2; i < argv.length; i += 2) {
            if (!hashExists(obj, argv[i])) added++;
            hashSet(obj, argv[i], argv[i + 1]);
        }
        return RespEncoder.encodeInteger(added);
    }

    @RedisCommand(name = "hmset", arity = -4, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hmset(RedisClient client, byte[][] argv) {
        hset(client, argv);
        return RespEncoder.OK;
    }

    @RedisCommand(name = "hget", arity = 3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hget(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.NULL_BULK;
        byte[] val = hashGet(obj, argv[2]);
        return val != null ? RespEncoder.encodeBulkString(val) : RespEncoder.NULL_BULK;
    }

    @RedisCommand(name = "hmget", arity = -3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hmget(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        List<Object> results = new ArrayList<>();
        for (int i = 2; i < argv.length; i++) {
            byte[] val = obj != null ? hashGet(obj, argv[i]) : null;
            results.add(val);
        }
        return RespEncoder.encodeArray(results);
    }

    @RedisCommand(name = "hdel", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hdel(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.ZERO;
        long deleted = 0;
        for (int i = 2; i < argv.length; i++) {
            if (hashDel(obj, argv[i])) deleted++;
        }
        if (hashLen(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeInteger(deleted);
    }

    @RedisCommand(name = "hexists", arity = 3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hexists(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.ZERO;
        return hashExists(obj, argv[2]) ? RespEncoder.ONE : RespEncoder.ZERO;
    }

    @RedisCommand(name = "hlen", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hlen(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.ZERO;
        return RespEncoder.encodeInteger(hashLen(obj));
    }

    @RedisCommand(name = "hkeys", arity = 2, flags = "read-only sort_for_script", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hkeys(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        List<Object> result = new ArrayList<>(hashKeys(obj));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "hvals", arity = 2, flags = "read-only sort_for_script", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hvals(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        List<Object> result = new ArrayList<>(hashValues(obj));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "hgetall", arity = 2, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hgetall(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        List<Object> result = new ArrayList<>(hashGetAll(obj));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "hincrby", arity = 4, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hincrby(RedisClient client, byte[][] argv) {
        long incr;
        try { incr = Long.parseLong(toStr(argv[3])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        RedisObject obj = getOrCreateHash(db(client), argv[1]);
        byte[] val = hashGet(obj, argv[2]);
        long current = 0;
        if (val != null) {
            try { current = Long.parseLong(toStr(val)); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }
        current += incr;
        hashSet(obj, argv[2], toBytes(String.valueOf(current)));
        return RespEncoder.encodeInteger(current);
    }

    @RedisCommand(name = "hincrbyfloat", arity = 4, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hincrbyfloat(RedisClient client, byte[][] argv) {
        double incr;
        try { incr = Double.parseDouble(toStr(argv[3])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (Double.isNaN(incr) || Double.isInfinite(incr)) throw RedisException.nanOrInf();
        RedisObject obj = getOrCreateHash(db(client), argv[1]);
        byte[] val = hashGet(obj, argv[2]);
        double current = 0;
        if (val != null) {
            try { current = Double.parseDouble(toStr(val)); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }
        current += incr;
        if (Double.isNaN(current) || Double.isInfinite(current)) throw RedisException.nanOrInf();
        String result = formatDouble(current);
        hashSet(obj, argv[2], toBytes(result));
        return RespEncoder.encodeBulkString(toBytes(result));
    }

    private static String formatDouble(double d) {
        if (d == Math.floor(d) && !Double.isInfinite(d)) return String.valueOf((long) d);
        return String.valueOf(d);
    }

    @RedisCommand(name = "hsetnx", arity = 4, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hsetnx(RedisClient client, byte[][] argv) {
        RedisObject obj = getOrCreateHash(db(client), argv[1]);
        if (hashExists(obj, argv[2])) return RespEncoder.ZERO;
        hashSet(obj, argv[2], argv[3]);
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "hscan", arity = -3, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hscan(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        List<byte[]> all = obj != null ? hashGetAll(obj) : new ArrayList<>();
        // Simple implementation: return all fields in one shot (cursor always 0)
        List<Object> result = new ArrayList<>();
        result.add(toBytes("0")); // next cursor
        List<Object> items = new ArrayList<>(all);
        result.add(RespEncoder.encodeArray(items));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "hrandfield", arity = -2, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hrandfield(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        if (obj == null) return RespEncoder.NULL_BULK;
        List<byte[]> keys = hashKeys(obj);
        if (keys.isEmpty()) return RespEncoder.NULL_BULK;

        if (argv.length == 2) {
            // Return single random field
            return RespEncoder.encodeBulkString(keys.get(new Random().nextInt(keys.size())));
        }

        long count;
        try { count = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        boolean withValues = argv.length > 3 && toStr(argv[3]).equalsIgnoreCase("WITHSCORES");

        List<Object> result = new ArrayList<>();
        if (count >= 0) {
            List<byte[]> shuffled = new ArrayList<>(keys);
            Collections.shuffle(shuffled);
            for (int i = 0; i < Math.min(count, shuffled.size()); i++) {
                result.add(shuffled.get(i));
                if (withValues) result.add(hashGet(obj, shuffled.get(i)));
            }
        } else {
            for (int i = 0; i < -count; i++) {
                byte[] k = keys.get(new Random().nextInt(keys.size()));
                result.add(k);
                if (withValues) result.add(hashGet(obj, k));
            }
        }
        return RespEncoder.encodeArray(result);
    }

    // ---- Hash field TTL commands (Redis 7.4+, mirrors t_hash.c) ----
    // Return codes (mirrors HFE_GET_* / HSETEX_* constants in t_hash.h):
    //   -2 = field does not exist
    //   -1 = field exists but has no TTL (persistent)
    //    0 = field already expired (treat as not-exist)
    //   >0 = TTL value

    private static final long HFE_NO_FIELD = -2;
    private static final long HFE_NO_TTL   = -1;

    // Per-field TTL storage: key → (field → expireAtMs)
    // This mirrors the listpack_ex / hash-field-expiry mechanism in Redis 7.4.
    // We store it as a separate map associated with the RedisObject via its identity.
    private static final java.util.concurrent.ConcurrentHashMap<
            Integer, Map<String, Long>> fieldExpiries = new java.util.concurrent.ConcurrentHashMap<>();

    private static Map<String, Long> getFieldExpiries(RedisObject obj) {
        return fieldExpiries.computeIfAbsent(System.identityHashCode(obj),
                k -> new java.util.concurrent.ConcurrentHashMap<>());
    }

    private static void removeFieldExpiries(RedisObject obj) {
        fieldExpiries.remove(System.identityHashCode(obj));
    }

    /**
     * HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
     *
     * Set TTL (in seconds) on one or more hash fields.
     * Mirrors hexpireCommand() → hexpireGenericCommand() in t_hash.c.
     *
     * Options:
     *   NX: Set only if field has no expiry
     *   XX: Set only if field already has expiry
     *   GT: Set only if new expiry > current
     *   LT: Set only if new expiry < current
     *
     * Return array of per-field result codes:
     *   -2 field does not exist
     *    0 condition not met (NX/XX/GT/LT)
     *    1 TTL set
     *    2 field deleted (TTL in the past)
     */
    @RedisCommand(name = "hexpire", arity = -6, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hexpire(RedisClient client, byte[][] argv) {
        return hexpireGeneric(client, argv, false, false);
    }

    /** HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS numfields field [...] */
    @RedisCommand(name = "hpexpire", arity = -6, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hpexpire(RedisClient client, byte[][] argv) {
        return hexpireGeneric(client, argv, true, false);
    }

    /** HEXPIREAT key unix-time-seconds [NX|XX|GT|LT] FIELDS numfields field [...] */
    @RedisCommand(name = "hexpireat", arity = -6, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hexpireat(RedisClient client, byte[][] argv) {
        return hexpireGeneric(client, argv, false, true);
    }

    /** HPEXPIREAT key unix-time-ms [NX|XX|GT|LT] FIELDS numfields field [...] */
    @RedisCommand(name = "hpexpireat", arity = -6, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hpexpireat(RedisClient client, byte[][] argv) {
        return hexpireGeneric(client, argv, true, true);
    }

    private byte[] hexpireGeneric(RedisClient client, byte[][] argv,
                                   boolean millis, boolean absolute) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);

        // Parse: argv[2]=time, optional condition flag, then FIELDS numfields field...
        long rawTime;
        try { rawTime = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }

        // Detect optional NX/XX/GT/LT flag
        int fieldsIdx = 3;
        String condition = null;
        String argAtIdx3 = argv.length > 3 ? toStr(argv[3]).toUpperCase() : "";
        if (argAtIdx3.equals("NX") || argAtIdx3.equals("XX") ||
                argAtIdx3.equals("GT") || argAtIdx3.equals("LT")) {
            condition = argAtIdx3;
            fieldsIdx = 4;
        }

        if (argv.length <= fieldsIdx + 1 || !toStr(argv[fieldsIdx]).equalsIgnoreCase("FIELDS"))
            throw RedisException.syntax();

        int numFields;
        try { numFields = Integer.parseInt(toStr(argv[fieldsIdx + 1])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }

        if (numFields <= 0) throw new RedisException("ERR Number of fields must be a positive integer");
        if (argv.length < fieldsIdx + 2 + numFields) throw RedisException.syntax();

        // Compute absolute expiry in ms
        long now = System.currentTimeMillis();
        long expireAtMs;
        if (absolute) {
            expireAtMs = millis ? rawTime : rawTime * 1000L;
        } else {
            expireAtMs = now + (millis ? rawTime : rawTime * 1000L);
        }

        List<Object> result = new ArrayList<>();

        if (obj == null) {
            for (int i = 0; i < numFields; i++) result.add(HFE_NO_FIELD);
            return RespEncoder.encodeArray(result);
        }

        Map<String, Long> expiries = getFieldExpiries(obj);

        for (int i = 0; i < numFields; i++) {
            String field = toStr(argv[fieldsIdx + 2 + i]);
            if (hashGet(obj, toBytes(field)) == null) {
                result.add(HFE_NO_FIELD);
                continue;
            }

            // Check expiry in the past → delete field
            if (expireAtMs <= now) {
                hashDel(obj, toBytes(field));
                expiries.remove(field);
                if (hashLen(obj) == 0) { db.delete(argv[1]); removeFieldExpiries(obj); }
                result.add(2L); // deleted
                continue;
            }

            Long current = expiries.get(field);
            // Apply condition
            if (condition != null) {
                switch (condition) {
                    case "NX": if (current != null) { result.add(0L); continue; } break;
                    case "XX": if (current == null) { result.add(0L); continue; } break;
                    case "GT": if (current != null && expireAtMs <= current) { result.add(0L); continue; } break;
                    case "LT": if (current != null && expireAtMs >= current) { result.add(0L); continue; } break;
                }
            }

            expiries.put(field, expireAtMs);
            result.add(1L); // set
        }

        return RespEncoder.encodeArray(result);
    }

    /**
     * HTTL key FIELDS numfields field [field ...]
     *
     * Returns remaining TTL in seconds for each field.
     * Mirrors httlCommand() → httlGenericCommand() in t_hash.c.
     */
    @RedisCommand(name = "httl", arity = -5, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] httl(RedisClient client, byte[][] argv) {
        return httlGeneric(client, argv, false, false);
    }

    /** HPTTL key FIELDS numfields field [...] — returns TTL in ms */
    @RedisCommand(name = "hpttl", arity = -5, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hpttl(RedisClient client, byte[][] argv) {
        return httlGeneric(client, argv, true, false);
    }

    /** HEXPIRETIME key FIELDS numfields field [...] — returns absolute expiry in seconds */
    @RedisCommand(name = "hexpiretime", arity = -5, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hexpiretime(RedisClient client, byte[][] argv) {
        return httlGeneric(client, argv, false, true);
    }

    /** HPEXPIRETIME key FIELDS numfields field [...] — returns absolute expiry in ms */
    @RedisCommand(name = "hpexpiretime", arity = -5, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hpexpiretime(RedisClient client, byte[][] argv) {
        return httlGeneric(client, argv, true, true);
    }

    private byte[] httlGeneric(RedisClient client, byte[][] argv,
                                boolean millis, boolean absolute) {
        if (argv.length < 5 || !toStr(argv[2]).equalsIgnoreCase("FIELDS"))
            throw RedisException.syntax();

        int numFields;
        try { numFields = Integer.parseInt(toStr(argv[3])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }

        if (argv.length < 4 + numFields) throw RedisException.syntax();

        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        List<Object> result = new ArrayList<>();

        if (obj == null) {
            for (int i = 0; i < numFields; i++) result.add(HFE_NO_FIELD);
            return RespEncoder.encodeArray(result);
        }

        Map<String, Long> expiries = getFieldExpiries(obj);
        long now = System.currentTimeMillis();

        for (int i = 0; i < numFields; i++) {
            String field = toStr(argv[4 + i]);
            if (hashGet(obj, toBytes(field)) == null) {
                result.add(HFE_NO_FIELD);
                continue;
            }
            Long expireAtMs = expiries.get(field);
            if (expireAtMs == null) {
                result.add(HFE_NO_TTL); // no TTL
            } else if (absolute) {
                result.add(millis ? expireAtMs : expireAtMs / 1000L);
            } else {
                long remaining = expireAtMs - now;
                if (remaining <= 0) {
                    result.add(HFE_NO_FIELD); // expired
                } else {
                    result.add(millis ? remaining : remaining / 1000L);
                }
            }
        }

        return RespEncoder.encodeArray(result);
    }

    /**
     * HPERSIST key FIELDS numfields field [field ...]
     *
     * Remove the TTL from hash fields (make them persistent).
     * Mirrors hpersistCommand() in t_hash.c.
     *
     * Returns per-field:
     *   -2 = field does not exist
     *   -1 = field has no TTL (already persistent)
     *    1 = TTL removed
     */
    @RedisCommand(name = "hpersist", arity = -5, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] hpersist(RedisClient client, byte[][] argv) {
        if (argv.length < 5 || !toStr(argv[2]).equalsIgnoreCase("FIELDS"))
            throw RedisException.syntax();

        int numFields;
        try { numFields = Integer.parseInt(toStr(argv[3])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }

        if (argv.length < 4 + numFields) throw RedisException.syntax();

        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_HASH);
        List<Object> result = new ArrayList<>();

        if (obj == null) {
            for (int i = 0; i < numFields; i++) result.add(HFE_NO_FIELD);
            return RespEncoder.encodeArray(result);
        }

        Map<String, Long> expiries = getFieldExpiries(obj);

        for (int i = 0; i < numFields; i++) {
            String field = toStr(argv[4 + i]);
            if (hashGet(obj, toBytes(field)) == null) {
                result.add(HFE_NO_FIELD);
            } else if (expiries.remove(field) != null) {
                result.add(1L); // TTL removed
            } else {
                result.add(HFE_NO_TTL); // already persistent
            }
        }

        return RespEncoder.encodeArray(result);
    }
}
