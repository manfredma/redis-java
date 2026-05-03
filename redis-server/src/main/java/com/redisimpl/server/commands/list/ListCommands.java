package com.redisimpl.server.commands.list;

import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.quicklist.QuickList;
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
 * List command implementations.
 */
public final class ListCommands {

    private final RedisServer server;

    public ListCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) {
        return server.getDb(client.getDb());
    }

    private static String toStr(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    // ---- Encoding helpers ----

    private static RedisObject getOrCreateList(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKeyOrReply(key, RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) {
            obj = RedisObject.createObject(
                    RedisObjectConstants.OBJ_TYPE_LIST,
                    RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                    ListPack.create());
            db.setKey(key, obj);
        }
        return obj;
    }

    private static void convertToQuickListIfNeeded(RedisObject obj) {
        if (obj.getEncoding() != RedisObjectConstants.OBJ_ENCODING_LISTPACK) return;
        ListPack lp = (ListPack) obj.getPtr();
        if (lp.size() > RedisObjectConstants.OBJ_LIST_MAX_LISTPACK_ENTRIES) {
            convertToQuickList(obj, lp);
        }
    }

    private static void convertToQuickList(RedisObject obj, ListPack lp) {
        QuickList ql = QuickList.create();
        for (byte[] e : lp.toList()) {
            ql = ql.rpush(e);
        }
        obj.setEncoding(RedisObjectConstants.OBJ_ENCODING_QUICKLIST);
        obj.setPtr(ql);
    }

    private static void listPush(RedisObject obj, byte[] value, boolean left) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            if (left) lp = lp.prepend(value);
            else lp = lp.append(value);
            obj.setPtr(lp);
            // Check if we need to convert
            if (lp.size() > RedisObjectConstants.OBJ_LIST_MAX_LISTPACK_ENTRIES
                    || value.length > RedisObjectConstants.OBJ_LIST_MAX_LISTPACK_VALUE) {
                convertToQuickList(obj, lp);
            }
        } else {
            QuickList ql = (QuickList) obj.getPtr();
            if (left) ql = ql.lpush(value);
            else ql = ql.rpush(value);
            obj.setPtr(ql);
        }
    }

    private static byte[] listPop(RedisObject obj, boolean left) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            if (lp.size() == 0) return null;
            byte[] val = left ? lp.get(0) : lp.get(lp.size() - 1);
            lp = left ? lp.delete(0) : lp.delete(lp.size() - 1);
            obj.setPtr(lp);
            return val;
        } else {
            QuickList ql = (QuickList) obj.getPtr();
            QuickList.PopResult result = left ? ql.lpopResult() : ql.rpopResult();
            obj.setPtr(result.list);
            return result.value;
        }
    }

    private static long listLen(RedisObject obj) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            return ((ListPack) obj.getPtr()).size();
        }
        return ((QuickList) obj.getPtr()).llen();
    }

    private static byte[] listIndex(RedisObject obj, long idx) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            long len = lp.size();
            long actual = idx < 0 ? len + idx : idx;
            if (actual < 0 || actual >= len) return null;
            return lp.get((int) actual);
        }
        return ((QuickList) obj.getPtr()).index(idx);
    }

    private static List<byte[]> listRange(RedisObject obj, long start, long stop) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            long len = lp.size();
            if (start < 0) start = Math.max(len + start, 0);
            if (stop < 0) stop = len + stop;
            if (start > stop || start >= len) return new ArrayList<>();
            stop = Math.min(stop, len - 1);
            List<byte[]> result = new ArrayList<>();
            for (long i = start; i <= stop; i++) result.add(lp.get((int) i));
            return result;
        }
        return ((QuickList) obj.getPtr()).range(start, stop);
    }

    // ---- Commands ----

    @RedisCommand(name = "lpush", arity = -3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] lpush(RedisClient client, byte[][] argv) {
        RedisObject obj = getOrCreateList(db(client), argv[1]);
        for (int i = 2; i < argv.length; i++) listPush(obj, argv[i], true);
        return RespEncoder.encodeInteger(listLen(obj));
    }

    @RedisCommand(name = "rpush", arity = -3, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] rpush(RedisClient client, byte[][] argv) {
        RedisObject obj = getOrCreateList(db(client), argv[1]);
        for (int i = 2; i < argv.length; i++) listPush(obj, argv[i], false);
        return RespEncoder.encodeInteger(listLen(obj));
    }

    @RedisCommand(name = "lpop", arity = -2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] lpop(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) return RespEncoder.NULL_BULK;

        int count = 1;
        boolean returnArray = argv.length > 2;
        if (returnArray) {
            try { count = (int) Long.parseLong(toStr(argv[2])); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }

        if (!returnArray) {
            byte[] val = listPop(obj, true);
            if (listLen(obj) == 0) db.delete(argv[1]);
            return val != null ? RespEncoder.encodeBulkString(val) : RespEncoder.NULL_BULK;
        }
        List<Object> results = new ArrayList<>();
        for (int i = 0; i < count && listLen(obj) > 0; i++) {
            results.add(listPop(obj, true));
        }
        if (listLen(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeArray(results);
    }

    @RedisCommand(name = "rpop", arity = -2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] rpop(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) return RespEncoder.NULL_BULK;

        int count = 1;
        boolean returnArray = argv.length > 2;
        if (returnArray) {
            try { count = (int) Long.parseLong(toStr(argv[2])); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }

        if (!returnArray) {
            byte[] val = listPop(obj, false);
            if (listLen(obj) == 0) db.delete(argv[1]);
            return val != null ? RespEncoder.encodeBulkString(val) : RespEncoder.NULL_BULK;
        }
        List<Object> results = new ArrayList<>();
        for (int i = 0; i < count && listLen(obj) > 0; i++) {
            results.add(listPop(obj, false));
        }
        if (listLen(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeArray(results);
    }

    @RedisCommand(name = "llen", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] llen(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) return RespEncoder.ZERO;
        return RespEncoder.encodeInteger(listLen(obj));
    }

    @RedisCommand(name = "lrange", arity = 4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] lrange(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        long start, stop;
        try {
            start = Long.parseLong(toStr(argv[2]));
            stop  = Long.parseLong(toStr(argv[3]));
        } catch (NumberFormatException e) { throw RedisException.notInteger(); }
        List<byte[]> items = listRange(obj, start, stop);
        List<Object> result = new ArrayList<>(items);
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "lindex", arity = 3, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] lindex(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) return RespEncoder.NULL_BULK;
        long idx;
        try { idx = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        byte[] val = listIndex(obj, idx);
        return val != null ? RespEncoder.encodeBulkString(val) : RespEncoder.NULL_BULK;
    }

    @RedisCommand(name = "lset", arity = 4, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] lset(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) throw new RedisException(RedisException.ERR_NO_SUCH_KEY);
        long idx;
        try { idx = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        long len = listLen(obj);
        long actual = idx < 0 ? len + idx : idx;
        if (actual < 0 || actual >= len) throw new RedisException("ERR index out of range");

        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            obj.setPtr(lp.set((int) actual, argv[3]));
        } else {
            QuickList ql = (QuickList) obj.getPtr();
            obj.setPtr(ql.lset(idx, argv[3]));
        }
        return RespEncoder.OK;
    }

    @RedisCommand(name = "linsert", arity = 5, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] linsert(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) return RespEncoder.ZERO;
        String where = toStr(argv[2]).toUpperCase();
        boolean before = where.equals("BEFORE");
        if (!before && !where.equals("AFTER")) throw RedisException.syntax();

        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            int idx = lp.indexOf(argv[3]);
            if (idx < 0) return RespEncoder.encodeInteger(-1);
            lp = lp.insert(before ? idx : idx + 1, argv[4]);
            obj.setPtr(lp);
            if (lp.size() > RedisObjectConstants.OBJ_LIST_MAX_LISTPACK_ENTRIES) convertToQuickList(obj, lp);
        } else {
            QuickList ql = (QuickList) obj.getPtr();
            QuickList result = ql.linsert(argv[3], before, argv[4]);
            if (result == null) return RespEncoder.encodeInteger(-1);
            obj.setPtr(result);
        }
        return RespEncoder.encodeInteger(listLen(obj));
    }

    @RedisCommand(name = "lrem", arity = 4, flags = "write", firstKey = 1, lastKey = 1, step = 1)
    public byte[] lrem(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) return RespEncoder.ZERO;
        long count;
        try { count = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }

        long removed;
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            removed = 0;
            if (count >= 0) {
                int i = 0;
                while (i < lp.size() && (count == 0 || removed < count)) {
                    if (Arrays.equals(lp.get(i), argv[3])) { lp = lp.delete(i); removed++; }
                    else i++;
                }
            } else {
                long abs = -count;
                int i = lp.size() - 1;
                while (i >= 0 && removed < abs) {
                    if (Arrays.equals(lp.get(i), argv[3])) { lp = lp.delete(i); removed++; }
                    i--;
                }
            }
            obj.setPtr(lp);
        } else {
            QuickList ql = (QuickList) obj.getPtr();
            QuickList.LremResult res = ql.lremResult(count, argv[3]);
            removed = res.removed;
            obj.setPtr(res.list);
        }
        if (listLen(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeInteger(removed);
    }

    @RedisCommand(name = "lmove", arity = 5, flags = "write denyoom", firstKey = 1, lastKey = 2, step = 1)
    public byte[] lmove(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject src = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (src == null) return RespEncoder.NULL_BULK;
        String srcDir = toStr(argv[3]).toUpperCase();
        String dstDir = toStr(argv[4]).toUpperCase();
        if (!srcDir.equals("LEFT") && !srcDir.equals("RIGHT")) throw RedisException.syntax();
        if (!dstDir.equals("LEFT") && !dstDir.equals("RIGHT")) throw RedisException.syntax();

        byte[] val = listPop(src, srcDir.equals("LEFT"));
        if (val == null) return RespEncoder.NULL_BULK;
        if (listLen(src) == 0) db.delete(argv[1]);

        RedisObject dst = getOrCreateList(db, argv[2]);
        listPush(dst, val, dstDir.equals("LEFT"));
        return RespEncoder.encodeBulkString(val);
    }

    @RedisCommand(name = "lmpop", arity = -4, flags = "write fast", firstKey = 2, lastKey = 2, step = 1)
    public byte[] lmpop(RedisClient client, byte[][] argv) {
        int numKeys;
        try { numKeys = (int) Long.parseLong(toStr(argv[1])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (numKeys <= 0) throw new RedisException("ERR numkeys must be a positive integer");
        if (argv.length < 3 + numKeys) throw RedisException.syntax();

        String dir = toStr(argv[2 + numKeys]).toUpperCase();
        if (!dir.equals("LEFT") && !dir.equals("RIGHT")) throw RedisException.syntax();

        int count = 1;
        if (argv.length > 3 + numKeys) {
            String opt = toStr(argv[3 + numKeys]).toUpperCase();
            if (!opt.equals("COUNT")) throw RedisException.syntax();
            if (argv.length < 5 + numKeys) throw RedisException.syntax();
            try { count = (int) Long.parseLong(toStr(argv[4 + numKeys])); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }

        RedisDb db = db(client);
        for (int k = 0; k < numKeys; k++) {
            byte[] key = argv[2 + k];
            RedisObject obj = db.lookupKeyOrReply(key, RedisObjectConstants.OBJ_TYPE_LIST);
            if (obj == null) continue;

            List<Object> popped = new ArrayList<>();
            for (int i = 0; i < count && listLen(obj) > 0; i++) {
                popped.add(listPop(obj, dir.equals("LEFT")));
            }
            if (listLen(obj) == 0) db.delete(key);

            List<Object> result = new ArrayList<>();
            result.add(key);
            result.add(popped);
            return RespEncoder.encodeArray(result);
        }
        return RespEncoder.encodeArray(null); // null array
    }

    @RedisCommand(name = "blpop", arity = -3, flags = "write noscript", firstKey = 1, lastKey = -2, step = 1)
    public byte[] blpop(RedisClient client, byte[][] argv) {
        // Non-blocking implementation: check if any key has data, otherwise return nil
        double timeout;
        try { timeout = Double.parseDouble(toStr(argv[argv.length - 1])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }

        RedisDb db = db(client);
        for (int i = 1; i < argv.length - 1; i++) {
            RedisObject obj = db.lookupKeyOrReply(argv[i], RedisObjectConstants.OBJ_TYPE_LIST);
            if (obj != null && listLen(obj) > 0) {
                byte[] val = listPop(obj, true);
                if (listLen(obj) == 0) db.delete(argv[i]);
                List<Object> result = new ArrayList<>();
                result.add(argv[i]);
                result.add(val);
                return RespEncoder.encodeArray(result);
            }
        }
        return RespEncoder.encodeArray(null);
    }

    @RedisCommand(name = "brpop", arity = -3, flags = "write noscript", firstKey = 1, lastKey = -2, step = 1)
    public byte[] brpop(RedisClient client, byte[][] argv) {
        double timeout;
        try { timeout = Double.parseDouble(toStr(argv[argv.length - 1])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }

        RedisDb db = db(client);
        for (int i = 1; i < argv.length - 1; i++) {
            RedisObject obj = db.lookupKeyOrReply(argv[i], RedisObjectConstants.OBJ_TYPE_LIST);
            if (obj != null && listLen(obj) > 0) {
                byte[] val = listPop(obj, false);
                if (listLen(obj) == 0) db.delete(argv[i]);
                List<Object> result = new ArrayList<>();
                result.add(argv[i]);
                result.add(val);
                return RespEncoder.encodeArray(result);
            }
        }
        return RespEncoder.encodeArray(null);
    }

    /**
     * LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
     *
     * Mirrors lposCommand() in t_list.c.
     * rank:    which match to return (1=first, 2=second, negative=from tail)
     * count:   return array of up to count matches; 0=all; -1=not given (return single)
     * maxlen:  scan at most maxlen elements; 0=all
     */
    @RedisCommand(name = "lpos", arity = -3, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] lpos(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        byte[] ele = argv[2];

        long rank = 1;
        long count = -1;   // -1 means not given → return single integer
        long maxlen = 0;   // 0 = no limit
        boolean fromTail = false;

        for (int j = 3; j < argv.length; j++) {
            String opt = toStr(argv[j]).toUpperCase();
            if ((opt.equals("RANK") || opt.equals("COUNT") || opt.equals("MAXLEN"))
                    && j + 1 >= argv.length) {
                throw RedisException.syntax();
            }
            if (opt.equals("RANK")) {
                rank = Long.parseLong(toStr(argv[++j]));
                if (rank == 0)
                    throw new RedisException(
                            "ERR RANK can't be zero: use 1 to start from the first match, "
                                    + "2 from the second ... or use negative to start from the end of the list");
            } else if (opt.equals("COUNT")) {
                count = Long.parseLong(toStr(argv[++j]));
                if (count < 0) throw new RedisException("ERR COUNT can't be negative");
            } else if (opt.equals("MAXLEN")) {
                maxlen = Long.parseLong(toStr(argv[++j]));
                if (maxlen < 0) throw new RedisException("ERR MAXLEN can't be negative");
            } else {
                throw RedisException.syntax();
            }
        }

        if (rank < 0) {
            rank = -rank;
            fromTail = true;
        }

        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_LIST);
        if (obj == null) {
            return (count != -1) ? RespEncoder.EMPTY_ARRAY : RespEncoder.NULL_BULK;
        }

        List<byte[]> all = listRange(obj, 0, -1);
        long llen = all.size();
        List<Long> matches = new ArrayList<>();
        long scanned = 0;
        long rankCount = 0;

        for (int i = 0; i < llen; i++) {
            int idx = fromTail ? (int)(llen - 1 - i) : i;
            if (maxlen > 0 && scanned >= maxlen) break;
            scanned++;
            if (Arrays.equals(all.get(idx), ele)) {
                rankCount++;
                if (rankCount >= rank) {
                    matches.add(fromTail ? (llen - 1 - i) : (long) i);
                    if (count != -1 && count > 0 && matches.size() >= count) break;
                    if (count == -1) break; // only first match needed
                }
            }
        }

        if (count == -1) {
            // Return single or null
            if (matches.isEmpty()) return RespEncoder.NULL_BULK;
            return RespEncoder.encodeInteger(matches.get(0));
        } else {
            // Return array
            List<Object> result = new ArrayList<>();
            for (Long m : matches) result.add(m);
            return RespEncoder.encodeArray(result);
        }
    }

    /**
     * BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
     *
     * Non-blocking version (timeout is ignored if data is available).
     * Mirrors blmpopCommand() → zmpopGenericCommand() logic in t_list.c.
     */
    @RedisCommand(name = "blmpop", arity = -5, flags = "write noscript", firstKey = 3, lastKey = 3, step = 1)
    public byte[] blmpop(RedisClient client, byte[][] argv) {
        // argv: blmpop timeout numkeys key [key...] LEFT|RIGHT [COUNT count]
        // timeout is argv[1], numkeys is argv[2]
        int numKeys;
        try { numKeys = (int) Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        if (numKeys <= 0) throw new RedisException("ERR numkeys must be a positive integer");
        if (argv.length < 4 + numKeys) throw RedisException.syntax();

        String dir = toStr(argv[3 + numKeys]).toUpperCase();
        if (!dir.equals("LEFT") && !dir.equals("RIGHT")) throw RedisException.syntax();
        boolean fromLeft = dir.equals("LEFT");

        int count = 1;
        if (argv.length > 4 + numKeys) {
            String opt = toStr(argv[4 + numKeys]).toUpperCase();
            if (!opt.equals("COUNT")) throw RedisException.syntax();
            if (argv.length < 6 + numKeys) throw RedisException.syntax();
            try { count = (int) Long.parseLong(toStr(argv[5 + numKeys])); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
            if (count <= 0) throw new RedisException("ERR count should be greater than 0");
        }

        RedisDb db = db(client);
        for (int k = 0; k < numKeys; k++) {
            byte[] key = argv[3 + k];
            RedisObject obj = db.lookupKeyOrReply(key, RedisObjectConstants.OBJ_TYPE_LIST);
            if (obj != null && listLen(obj) > 0) {
                List<Object> popped = new ArrayList<>();
                for (int i = 0; i < count && listLen(obj) > 0; i++) {
                    popped.add(listPop(obj, fromLeft));
                }
                if (listLen(obj) == 0) db.delete(key);

                List<Object> result = new ArrayList<>();
                result.add(key);
                result.add(popped);
                return RespEncoder.encodeArray(result);
            }
        }
        // No data available — in a real implementation we would block
        return RespEncoder.encodeArray(null);
    }
}
