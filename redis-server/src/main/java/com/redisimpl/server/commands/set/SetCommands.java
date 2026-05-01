package com.redisimpl.server.commands.set;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.intset.IntSet;
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
 * Set command implementations.
 */
public final class SetCommands {

    private final RedisServer server;

    public SetCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) { return server.getDb(client.getDb()); }
    private static String toStr(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    private static byte[] toBytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    // ---- Encoding helpers ----

    private static RedisObject createSet() {
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_SET,
                RedisObjectConstants.OBJ_ENCODING_INTSET,
                IntSet.create());
    }

    private static boolean setAdd(RedisObject obj, byte[] member) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
            try {
                long v = Long.parseLong(toStr(member));
                IntSet is = (IntSet) obj.getPtr();
                if (is.contains(v)) return false;
                is = is.add(v);
                obj.setPtr(is);
                if (is.length() > RedisObjectConstants.OBJ_SET_MAX_INTSET_ENTRIES) {
                    convertIntSetToHT(obj, is);
                }
                return true;
            } catch (NumberFormatException e) {
                // Not an integer: convert to listpack or HT
                IntSet is = (IntSet) obj.getPtr();
                if (is.length() < RedisObjectConstants.OBJ_SET_MAX_LISTPACK_ENTRIES
                        && member.length <= RedisObjectConstants.OBJ_SET_MAX_LISTPACK_VALUE) {
                    convertIntSetToListPack(obj, is);
                } else {
                    convertIntSetToHT(obj, is);
                }
            }
        }

        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            if (lp.indexOf(member) >= 0) return false;
            lp = lp.append(member);
            obj.setPtr(lp);
            if (lp.size() > RedisObjectConstants.OBJ_SET_MAX_LISTPACK_ENTRIES
                    || member.length > RedisObjectConstants.OBJ_SET_MAX_LISTPACK_VALUE) {
                convertListPackToHT(obj, lp);
            }
            return true;
        }

        // HT encoding
        Dict dict = (Dict) obj.getPtr();
        if (dict.containsKey(member)) return false;
        dict.put(member, null);
        return true;
    }

    private static boolean setRemove(RedisObject obj, byte[] member) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
            try {
                long v = Long.parseLong(toStr(member));
                IntSet is = (IntSet) obj.getPtr();
                if (!is.contains(v)) return false;
                obj.setPtr(is.remove(v));
                return true;
            } catch (NumberFormatException e) { return false; }
        }
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            int idx = lp.indexOf(member);
            if (idx < 0) return false;
            obj.setPtr(lp.delete(idx));
            return true;
        }
        return ((Dict) obj.getPtr()).delete(member);
    }

    private static boolean setContains(RedisObject obj, byte[] member) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
            try { return ((IntSet) obj.getPtr()).contains(Long.parseLong(toStr(member))); }
            catch (NumberFormatException e) { return false; }
        }
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            return ((ListPack) obj.getPtr()).indexOf(member) >= 0;
        }
        return ((Dict) obj.getPtr()).containsKey(member);
    }

    private static long setCard(RedisObject obj) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) return ((IntSet) obj.getPtr()).length();
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) return ((ListPack) obj.getPtr()).size();
        return ((Dict) obj.getPtr()).size();
    }

    private static Set<String> setMembers(RedisObject obj) {
        Set<String> members = new LinkedHashSet<>();
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
            for (long v : ((IntSet) obj.getPtr()).toArray()) members.add(String.valueOf(v));
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            for (byte[] e : ((ListPack) obj.getPtr()).toList()) members.add(toStr(e));
        } else {
            for (Dict.Entry e : (Dict) obj.getPtr()) members.add(toStr(e.getKey()));
        }
        return members;
    }

    private static void convertIntSetToHT(RedisObject obj, IntSet is) {
        Dict dict = Dict.create();
        for (long v : is.toArray()) dict.put(toBytes(String.valueOf(v)), null);
        obj.setEncoding(RedisObjectConstants.OBJ_ENCODING_HT);
        obj.setPtr(dict);
    }

    private static void convertIntSetToListPack(RedisObject obj, IntSet is) {
        ListPack lp = ListPack.create();
        for (long v : is.toArray()) lp = lp.append(toBytes(String.valueOf(v)));
        obj.setEncoding(RedisObjectConstants.OBJ_ENCODING_LISTPACK);
        obj.setPtr(lp);
    }

    private static void convertListPackToHT(RedisObject obj, ListPack lp) {
        Dict dict = Dict.create();
        for (byte[] e : lp.toList()) dict.put(e, null);
        obj.setEncoding(RedisObjectConstants.OBJ_ENCODING_HT);
        obj.setPtr(dict);
    }

    // ---- Commands ----

    @RedisCommand(name = "sadd", arity = -3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] sadd(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (obj == null) { obj = createSet(); db.setKey(argv[1], obj); }
        long added = 0;
        for (int i = 2; i < argv.length; i++) if (setAdd(obj, argv[i])) added++;
        return RespEncoder.encodeInteger(added);
    }

    @RedisCommand(name = "srem", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] srem(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (obj == null) return RespEncoder.ZERO;
        long removed = 0;
        for (int i = 2; i < argv.length; i++) if (setRemove(obj, argv[i])) removed++;
        if (setCard(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeInteger(removed);
    }

    @RedisCommand(name = "smembers", arity = 2, flags = "read-only sort_for_script", firstKey = 1, lastKey = 1, step = 1)
    public byte[] smembers(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        List<Object> result = new ArrayList<>();
        for (String m : setMembers(obj)) result.add(toBytes(m));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "sismember", arity = 3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] sismember(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (obj == null) return RespEncoder.ZERO;
        return setContains(obj, argv[2]) ? RespEncoder.ONE : RespEncoder.ZERO;
    }

    @RedisCommand(name = "smismember", arity = -3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] smismember(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        List<Object> result = new ArrayList<>();
        for (int i = 2; i < argv.length; i++) {
            boolean found = obj != null && setContains(obj, argv[i]);
            result.add(found ? 1L : 0L);
        }
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "scard", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] scard(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (obj == null) return RespEncoder.ZERO;
        return RespEncoder.encodeInteger(setCard(obj));
    }

    @RedisCommand(name = "sinter", arity = -2, flags = "read-only sort_for_script", firstKey = 1, lastKey = -1, step = 1)
    public byte[] sinter(RedisClient client, byte[][] argv) {
        return encodeSet(intersection(client, argv, 1));
    }

    @RedisCommand(name = "sunion", arity = -2, flags = "read-only sort_for_script", firstKey = 1, lastKey = -1, step = 1)
    public byte[] sunion(RedisClient client, byte[][] argv) {
        return encodeSet(union(client, argv, 1));
    }

    @RedisCommand(name = "sdiff", arity = -2, flags = "read-only sort_for_script", firstKey = 1, lastKey = -1, step = 1)
    public byte[] sdiff(RedisClient client, byte[][] argv) {
        return encodeSet(diff(client, argv, 1));
    }

    @RedisCommand(name = "sinterstore", arity = -3, flags = "write denyoom", firstKey = 1, lastKey = -1, step = 1)
    public byte[] sinterstore(RedisClient client, byte[][] argv) {
        Set<String> result = intersection(client, argv, 2);
        storeSet(db(client), argv[1], result);
        return RespEncoder.encodeInteger(result.size());
    }

    @RedisCommand(name = "sunionstore", arity = -3, flags = "write denyoom", firstKey = 1, lastKey = -1, step = 1)
    public byte[] sunionstore(RedisClient client, byte[][] argv) {
        Set<String> result = union(client, argv, 2);
        storeSet(db(client), argv[1], result);
        return RespEncoder.encodeInteger(result.size());
    }

    @RedisCommand(name = "sdiffstore", arity = -3, flags = "write denyoom", firstKey = 1, lastKey = -1, step = 1)
    public byte[] sdiffstore(RedisClient client, byte[][] argv) {
        Set<String> result = diff(client, argv, 2);
        storeSet(db(client), argv[1], result);
        return RespEncoder.encodeInteger(result.size());
    }

    @RedisCommand(name = "srandmember", arity = -2, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] srandmember(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (obj == null) {
            return argv.length == 2 ? RespEncoder.NULL_BULK : RespEncoder.EMPTY_ARRAY;
        }
        List<String> members = new ArrayList<>(setMembers(obj));
        if (argv.length == 2) {
            if (members.isEmpty()) return RespEncoder.NULL_BULK;
            return RespEncoder.encodeBulkString(toBytes(members.get(new Random().nextInt(members.size()))));
        }
        long count;
        try { count = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        List<Object> result = new ArrayList<>();
        if (count >= 0) {
            Collections.shuffle(members);
            for (int i = 0; i < Math.min(count, members.size()); i++) result.add(toBytes(members.get(i)));
        } else {
            for (int i = 0; i < -count; i++) result.add(toBytes(members.get(new Random().nextInt(members.size()))));
        }
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "spop", arity = -2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] spop(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (obj == null) return argv.length == 2 ? RespEncoder.NULL_BULK : RespEncoder.EMPTY_ARRAY;
        List<String> members = new ArrayList<>(setMembers(obj));
        if (members.isEmpty()) return argv.length == 2 ? RespEncoder.NULL_BULK : RespEncoder.EMPTY_ARRAY;

        int count = 1;
        if (argv.length > 2) {
            try { count = (int) Long.parseLong(toStr(argv[2])); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }

        Collections.shuffle(members);
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < Math.min(count, members.size()); i++) {
            String m = members.get(i);
            setRemove(obj, toBytes(m));
            result.add(toBytes(m));
        }
        if (setCard(obj) == 0) db.delete(argv[1]);

        if (argv.length == 2) return RespEncoder.encodeBulkString((byte[]) result.get(0));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "smove", arity = 4, flags = "write fast", firstKey = 1, lastKey = 2, step = 1)
    public byte[] smove(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject src = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        if (src == null) return RespEncoder.ZERO;
        if (!setContains(src, argv[3])) return RespEncoder.ZERO;
        setRemove(src, argv[3]);
        if (setCard(src) == 0) db.delete(argv[1]);
        RedisObject dst = db.lookupKeyOrReply(argv[2], RedisObjectConstants.OBJ_TYPE_SET);
        if (dst == null) { dst = createSet(); db.setKey(argv[2], dst); }
        setAdd(dst, argv[3]);
        return RespEncoder.ONE;
    }

    @RedisCommand(name = "sscan", arity = -3, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] sscan(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_SET);
        List<Object> members = new ArrayList<>();
        if (obj != null) for (String m : setMembers(obj)) members.add(toBytes(m));
        List<Object> result = new ArrayList<>();
        result.add(toBytes("0"));
        result.add(RespEncoder.encodeArray(members));
        return RespEncoder.encodeArray(result);
    }

    // ---- Set operations ----

    private Set<String> intersection(RedisClient client, byte[][] argv, int startIdx) {
        RedisDb db = db(client);
        Set<String> result = null;
        for (int i = startIdx; i < argv.length; i++) {
            RedisObject obj = db.lookupKeyOrReply(argv[i], RedisObjectConstants.OBJ_TYPE_SET);
            Set<String> members = obj != null ? setMembers(obj) : new HashSet<>();
            if (result == null) result = new HashSet<>(members);
            else result.retainAll(members);
        }
        return result != null ? result : new HashSet<>();
    }

    private Set<String> union(RedisClient client, byte[][] argv, int startIdx) {
        RedisDb db = db(client);
        Set<String> result = new HashSet<>();
        for (int i = startIdx; i < argv.length; i++) {
            RedisObject obj = db.lookupKeyOrReply(argv[i], RedisObjectConstants.OBJ_TYPE_SET);
            if (obj != null) result.addAll(setMembers(obj));
        }
        return result;
    }

    private Set<String> diff(RedisClient client, byte[][] argv, int startIdx) {
        RedisDb db = db(client);
        RedisObject first = db.lookupKeyOrReply(argv[startIdx], RedisObjectConstants.OBJ_TYPE_SET);
        Set<String> result = first != null ? new HashSet<>(setMembers(first)) : new HashSet<>();
        for (int i = startIdx + 1; i < argv.length; i++) {
            RedisObject obj = db.lookupKeyOrReply(argv[i], RedisObjectConstants.OBJ_TYPE_SET);
            if (obj != null) result.removeAll(setMembers(obj));
        }
        return result;
    }

    private static byte[] encodeSet(Set<String> set) {
        List<Object> result = new ArrayList<>();
        for (String m : set) result.add(toBytes(m));
        return RespEncoder.encodeArray(result);
    }

    private static void storeSet(RedisDb db, byte[] key, Set<String> members) {
        db.delete(key);
        if (members.isEmpty()) return;
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_SET,
                RedisObjectConstants.OBJ_ENCODING_HT,
                Dict.create());
        for (String m : members) ((Dict) obj.getPtr()).put(toBytes(m), null);
        db.setKey(key, obj);
    }
}
