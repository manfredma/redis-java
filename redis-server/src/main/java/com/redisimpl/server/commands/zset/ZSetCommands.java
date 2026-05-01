package com.redisimpl.server.commands.zset;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.zskiplist.ZSkipList;
import com.redisimpl.core.zskiplist.ZSkipListNode;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.db.RedisDb;
import com.redisimpl.server.resp.RespEncoder;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Sorted set (ZSet) command implementations.
 */
public final class ZSetCommands {

    private final RedisServer server;

    public ZSetCommands(RedisServer server) {
        this.server = server;
    }

    private RedisDb db(RedisClient client) { return server.getDb(client.getDb()); }
    private static String toStr(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    private static byte[] toBytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    // ---- ZSet data holder for SKIPLIST encoding ----
    public static final class ZSetData {
        public final ZSkipList zsl;
        public final Dict dict; // member -> score (Double)
        ZSetData() { this.zsl = new ZSkipList(); this.dict = Dict.create(); }
    }

    // ---- Encoding helpers ----

    private static RedisObject createZSet() {
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_ZSET,
                RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                ListPack.create());
    }

    private static void convertToSkipList(RedisObject obj) {
        if (obj.getEncoding() != RedisObjectConstants.OBJ_ENCODING_LISTPACK) return;
        ListPack lp = (ListPack) obj.getPtr();
        ZSetData data = new ZSetData();
        for (int i = 0; i < lp.size(); i += 2) {
            byte[] member = lp.get(i);
            double score = Double.parseDouble(toStr(lp.get(i + 1)));
            data.zsl.insert(score, member);
            data.dict.put(member, score);
        }
        obj.setEncoding(RedisObjectConstants.OBJ_ENCODING_SKIPLIST);
        obj.setPtr(data);
    }

    private static boolean zsetAdd(RedisObject obj, double score, byte[] member) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i < lp.size(); i += 2) {
                if (Arrays.equals(lp.get(i), member)) {
                    // Update score
                    lp = lp.set(i + 1, toBytes(formatScore(score)));
                    obj.setPtr(lp);
                    return false; // updated, not added
                }
            }
            lp = lp.append(member).append(toBytes(formatScore(score)));
            obj.setPtr(lp);
            if (lp.size() / 2 > RedisObjectConstants.OBJ_ZSET_MAX_LISTPACK_ENTRIES
                    || member.length > RedisObjectConstants.OBJ_ZSET_MAX_LISTPACK_VALUE) {
                convertToSkipList(obj);
            }
            return true;
        }
        ZSetData data = (ZSetData) obj.getPtr();
        Object existing = data.dict.get(member);
        if (existing != null) {
            double oldScore = (Double) existing;
            data.zsl.delete(oldScore, member);
        }
        data.zsl.insert(score, member);
        data.dict.put(member, score);
        return existing == null;
    }

    private static Double zsetScore(RedisObject obj, byte[] member) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i < lp.size(); i += 2) {
                if (Arrays.equals(lp.get(i), member)) {
                    return Double.parseDouble(toStr(lp.get(i + 1)));
                }
            }
            return null;
        }
        Object v = ((ZSetData) obj.getPtr()).dict.get(member);
        return v != null ? (Double) v : null;
    }

    private static boolean zsetDelete(RedisObject obj, byte[] member) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i < lp.size(); i += 2) {
                if (Arrays.equals(lp.get(i), member)) {
                    lp = lp.delete(i + 1).delete(i);
                    obj.setPtr(lp);
                    return true;
                }
            }
            return false;
        }
        ZSetData data = (ZSetData) obj.getPtr();
        Object existing = data.dict.get(member);
        if (existing == null) return false;
        data.zsl.delete((Double) existing, member);
        data.dict.delete(member);
        return true;
    }

    private static long zsetCard(RedisObject obj) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK)
            return ((ListPack) obj.getPtr()).size() / 2;
        return ((ZSetData) obj.getPtr()).dict.size();
    }

    private static long zsetRank(RedisObject obj, byte[] member, boolean rev) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            int total = lp.size() / 2;
            // Build sorted order
            List<double[]> scored = new ArrayList<>();
            for (int i = 0; i < lp.size(); i += 2) {
                scored.add(new double[]{Double.parseDouble(toStr(lp.get(i + 1))), i / 2});
            }
            scored.sort((a, b) -> Double.compare(a[0], b[0]));
            for (int rank = 0; rank < scored.size(); rank++) {
                int origIdx = (int) scored.get(rank)[1];
                if (Arrays.equals(lp.get(origIdx * 2), member)) {
                    return rev ? total - 1 - rank : rank;
                }
            }
            return -1;
        }
        ZSetData data = (ZSetData) obj.getPtr();
        Object scoreObj = data.dict.get(member);
        if (scoreObj == null) return -1;
        long rank = data.zsl.rank((Double) scoreObj, member);
        if (rank == 0) return -1;
        return rev ? data.zsl.length() - rank : rank - 1;
    }

    private static List<ZSetEntry> zsetRangeByRank(RedisObject obj, long start, long stop, boolean rev) {
        long len = zsetCard(obj);
        if (start < 0) start = Math.max(len + start, 0);
        if (stop < 0) stop = len + stop;
        if (start > stop || start >= len) return new ArrayList<>();
        stop = Math.min(stop, len - 1);

        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            List<ZSetEntry> all = new ArrayList<>();
            for (int i = 0; i < lp.size(); i += 2) {
                all.add(new ZSetEntry(lp.get(i), Double.parseDouble(toStr(lp.get(i + 1)))));
            }
            all.sort((a, b) -> Double.compare(a.score, b.score));
            if (rev) Collections.reverse(all);
            return all.subList((int) start, (int) stop + 1);
        }

        ZSetData data = (ZSetData) obj.getPtr();
        List<ZSkipListNode> nodes = rev
                ? data.zsl.rangeByRank(len - stop, len - start)
                : data.zsl.rangeByRank(start + 1, stop + 1);
        List<ZSetEntry> result = new ArrayList<>();
        for (ZSkipListNode n : nodes) result.add(new ZSetEntry(n.getEle(), n.getScore()));
        if (rev) Collections.reverse(result);
        return result;
    }

    private static String formatScore(double score) {
        if (score == Math.floor(score) && !Double.isInfinite(score)) return String.valueOf((long) score);
        return String.valueOf(score);
    }

    private static double parseScore(String s) {
        if (s.equalsIgnoreCase("+inf") || s.equalsIgnoreCase("inf")) return Double.POSITIVE_INFINITY;
        if (s.equalsIgnoreCase("-inf")) return Double.NEGATIVE_INFINITY;
        try { return Double.parseDouble(s); }
        catch (NumberFormatException e) { throw new RedisException("ERR value is not a valid float"); }
    }

    public static final class ZSetEntry {
        public final byte[] member;
        public final double score;
        ZSetEntry(byte[] member, double score) { this.member = member; this.score = score; }
    }

    // ---- Commands ----

    @RedisCommand(name = "zadd", arity = -4, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zadd(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) { obj = createZSet(); db.setKey(argv[1], obj); }

        // Parse flags
        int scoreIdx = 2;
        boolean nx = false, xx = false, gt = false, lt = false, ch = false, incr = false;
        while (scoreIdx < argv.length) {
            String flag = toStr(argv[scoreIdx]).toUpperCase();
            if (flag.equals("NX")) { nx = true; scoreIdx++; }
            else if (flag.equals("XX")) { xx = true; scoreIdx++; }
            else if (flag.equals("GT")) { gt = true; scoreIdx++; }
            else if (flag.equals("LT")) { lt = true; scoreIdx++; }
            else if (flag.equals("CH")) { ch = true; scoreIdx++; }
            else if (flag.equals("INCR")) { incr = true; scoreIdx++; }
            else break;
        }

        if ((argv.length - scoreIdx) % 2 != 0) throw RedisException.syntax();
        long added = 0, changed = 0;

        for (int i = scoreIdx; i < argv.length; i += 2) {
            double score = parseScore(toStr(argv[i]));
            byte[] member = argv[i + 1];

            Double existing = zsetScore(obj, member);
            if (nx && existing != null) continue;
            if (xx && existing == null) continue;
            if (gt && existing != null && score <= existing) continue;
            if (lt && existing != null && score >= existing) continue;

            if (incr) {
                score = (existing != null ? existing : 0) + score;
                zsetAdd(obj, score, member);
                return RespEncoder.encodeBulkString(toBytes(formatScore(score)));
            }

            boolean isNew = zsetAdd(obj, score, member);
            if (isNew) added++;
            else if (ch && existing != null && existing != score) changed++;
        }

        return RespEncoder.encodeInteger(ch ? added + changed : added);
    }

    @RedisCommand(name = "zcard", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zcard(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.ZERO;
        return RespEncoder.encodeInteger(zsetCard(obj));
    }

    @RedisCommand(name = "zscore", arity = 3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zscore(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.NULL_BULK;
        Double score = zsetScore(obj, argv[2]);
        return score != null ? RespEncoder.encodeBulkString(toBytes(formatScore(score))) : RespEncoder.NULL_BULK;
    }

    @RedisCommand(name = "zmscore", arity = -3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zmscore(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        List<Object> result = new ArrayList<>();
        for (int i = 2; i < argv.length; i++) {
            Double score = obj != null ? zsetScore(obj, argv[i]) : null;
            result.add(score != null ? toBytes(formatScore(score)) : null);
        }
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "zincrby", arity = 4, flags = "write denyoom fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zincrby(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) { obj = createZSet(); db.setKey(argv[1], obj); }
        double incr = parseScore(toStr(argv[2]));
        Double existing = zsetScore(obj, argv[3]);
        double newScore = (existing != null ? existing : 0) + incr;
        zsetAdd(obj, newScore, argv[3]);
        return RespEncoder.encodeBulkString(toBytes(formatScore(newScore)));
    }

    @RedisCommand(name = "zrank", arity = -3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrank(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.NULL_BULK;
        long rank = zsetRank(obj, argv[2], false);
        if (rank < 0) return RespEncoder.NULL_BULK;
        boolean withScore = argv.length > 3 && toStr(argv[3]).equalsIgnoreCase("WITHSCORE");
        if (withScore) {
            List<Object> result = new ArrayList<>();
            result.add(rank);
            result.add(toBytes(formatScore(zsetScore(obj, argv[2]))));
            return RespEncoder.encodeArray(result);
        }
        return RespEncoder.encodeInteger(rank);
    }

    @RedisCommand(name = "zrevrank", arity = -3, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrevrank(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.NULL_BULK;
        long rank = zsetRank(obj, argv[2], true);
        if (rank < 0) return RespEncoder.NULL_BULK;
        return RespEncoder.encodeInteger(rank);
    }

    @RedisCommand(name = "zrange", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrange(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        long start, stop;
        try { start = Long.parseLong(toStr(argv[2])); stop = Long.parseLong(toStr(argv[3])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        boolean withScores = argv.length > 4 && toStr(argv[4]).equalsIgnoreCase("WITHSCORES");
        boolean rev = argv.length > 4 && toStr(argv[4]).equalsIgnoreCase("REV");
        List<ZSetEntry> entries = zsetRangeByRank(obj, start, stop, rev);
        return encodeEntries(entries, withScores);
    }

    @RedisCommand(name = "zrevrange", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrevrange(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        long start, stop;
        try { start = Long.parseLong(toStr(argv[2])); stop = Long.parseLong(toStr(argv[3])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        boolean withScores = argv.length > 4 && toStr(argv[4]).equalsIgnoreCase("WITHSCORES");
        List<ZSetEntry> entries = zsetRangeByRank(obj, start, stop, true);
        return encodeEntries(entries, withScores);
    }

    @RedisCommand(name = "zrangebyscore", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrangebyscore(RedisClient client, byte[][] argv) {
        return rangeByScore(client, argv, false);
    }

    @RedisCommand(name = "zrevrangebyscore", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrevrangebyscore(RedisClient client, byte[][] argv) {
        return rangeByScore(client, argv, true);
    }

    private byte[] rangeByScore(RedisClient client, byte[][] argv, boolean rev) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        String minStr = toStr(rev ? argv[3] : argv[2]);
        String maxStr = toStr(rev ? argv[2] : argv[3]);
        boolean minExcl = minStr.startsWith("(");
        boolean maxExcl = maxStr.startsWith("(");
        if (minExcl) minStr = minStr.substring(1);
        if (maxExcl) maxStr = maxStr.substring(1);
        double min = parseScore(minStr), max = parseScore(maxStr);

        boolean withScores = false;
        for (int i = 4; i < argv.length; i++) {
            if (toStr(argv[i]).equalsIgnoreCase("WITHSCORES")) withScores = true;
        }

        List<ZSetEntry> all = getAllByScore(obj, min, max, minExcl, maxExcl);
        if (rev) Collections.reverse(all);
        return encodeEntries(all, withScores);
    }

    private static List<ZSetEntry> getAllByScore(RedisObject obj, double min, double max,
                                                  boolean minExcl, boolean maxExcl) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            List<ZSetEntry> all = new ArrayList<>();
            for (int i = 0; i < lp.size(); i += 2) {
                double score = Double.parseDouble(toStr(lp.get(i + 1)));
                boolean inMin = minExcl ? score > min : score >= min;
                boolean inMax = maxExcl ? score < max : score <= max;
                if (inMin && inMax) all.add(new ZSetEntry(lp.get(i), score));
            }
            all.sort((a, b) -> Double.compare(a.score, b.score));
            return all;
        }
        ZSetData data = (ZSetData) obj.getPtr();
        List<ZSkipListNode> nodes = data.zsl.rangeByScore(min, max, minExcl, maxExcl);
        List<ZSetEntry> result = new ArrayList<>();
        for (ZSkipListNode n : nodes) result.add(new ZSetEntry(n.getEle(), n.getScore()));
        return result;
    }

    @RedisCommand(name = "zrangebylex", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrangebylex(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        List<ZSetEntry> entries = rangeByLex(obj, toStr(argv[2]), toStr(argv[3]), false);
        return encodeEntries(entries, false);
    }

    @RedisCommand(name = "zrevrangebylex", arity = -4, flags = "read-only", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrevrangebylex(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        List<ZSetEntry> entries = rangeByLex(obj, toStr(argv[3]), toStr(argv[2]), true);
        return encodeEntries(entries, false);
    }

    private static List<ZSetEntry> rangeByLex(RedisObject obj, String min, String max, boolean rev) {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_SKIPLIST) {
            ZSetData data = (ZSetData) obj.getPtr();
            List<ZSkipListNode> nodes = data.zsl.rangeByLex(min, max);
            List<ZSetEntry> result = new ArrayList<>();
            for (ZSkipListNode n : nodes) result.add(new ZSetEntry(n.getEle(), n.getScore()));
            if (rev) Collections.reverse(result);
            return result;
        }
        // listpack: collect all members and filter
        ListPack lp = (ListPack) obj.getPtr();
        List<ZSetEntry> all = new ArrayList<>();
        for (int i = 0; i < lp.size(); i += 2) {
            all.add(new ZSetEntry(lp.get(i), Double.parseDouble(toStr(lp.get(i + 1)))));
        }
        all.sort((a, b) -> compareBytes(a.member, b.member));
        List<ZSetEntry> result = new ArrayList<>();
        for (ZSetEntry e : all) {
            String m = toStr(e.member);
            if (inLexRange(m, min, max)) result.add(e);
        }
        if (rev) Collections.reverse(result);
        return result;
    }

    private static boolean inLexRange(String member, String min, String max) {
        if (min.equals("-") && max.equals("+")) return true;
        boolean minOk = min.equals("-") || (min.startsWith("[") && member.compareTo(min.substring(1)) >= 0)
                || (min.startsWith("(") && member.compareTo(min.substring(1)) > 0);
        boolean maxOk = max.equals("+") || (max.startsWith("[") && member.compareTo(max.substring(1)) <= 0)
                || (max.startsWith("(") && member.compareTo(max.substring(1)) < 0);
        return minOk && maxOk;
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return a.length - b.length;
    }

    @RedisCommand(name = "zrem", arity = -3, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrem(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.ZERO;
        long removed = 0;
        for (int i = 2; i < argv.length; i++) if (zsetDelete(obj, argv[i])) removed++;
        if (zsetCard(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeInteger(removed);
    }

    @RedisCommand(name = "zremrangebyscore", arity = 4, flags = "write", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zremrangebyscore(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.ZERO;
        String minStr = toStr(argv[2]), maxStr = toStr(argv[3]);
        boolean minExcl = minStr.startsWith("("), maxExcl = maxStr.startsWith("(");
        if (minExcl) minStr = minStr.substring(1);
        if (maxExcl) maxStr = maxStr.substring(1);
        double min = parseScore(minStr), max = parseScore(maxStr);
        List<ZSetEntry> toRemove = getAllByScore(obj, min, max, minExcl, maxExcl);
        for (ZSetEntry e : toRemove) zsetDelete(obj, e.member);
        if (zsetCard(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeInteger(toRemove.size());
    }

    @RedisCommand(name = "zremrangebyrank", arity = 4, flags = "write", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zremrangebyrank(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.ZERO;
        long start, stop;
        try { start = Long.parseLong(toStr(argv[2])); stop = Long.parseLong(toStr(argv[3])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        List<ZSetEntry> toRemove = zsetRangeByRank(obj, start, stop, false);
        for (ZSetEntry e : toRemove) zsetDelete(obj, e.member);
        if (zsetCard(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeInteger(toRemove.size());
    }

    @RedisCommand(name = "zremrangebylex", arity = 4, flags = "write", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zremrangebylex(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.ZERO;
        List<ZSetEntry> toRemove = rangeByLex(obj, toStr(argv[2]), toStr(argv[3]), false);
        for (ZSetEntry e : toRemove) zsetDelete(obj, e.member);
        if (zsetCard(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeInteger(toRemove.size());
    }

    @RedisCommand(name = "zcount", arity = 4, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zcount(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.ZERO;
        String minStr = toStr(argv[2]), maxStr = toStr(argv[3]);
        boolean minExcl = minStr.startsWith("("), maxExcl = maxStr.startsWith("(");
        if (minExcl) minStr = minStr.substring(1);
        if (maxExcl) maxStr = maxStr.substring(1);
        double min = parseScore(minStr), max = parseScore(maxStr);
        return RespEncoder.encodeInteger(getAllByScore(obj, min, max, minExcl, maxExcl).size());
    }

    @RedisCommand(name = "zpopmin", arity = -2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zpopmin(RedisClient client, byte[][] argv) {
        return zpop(client, argv, false);
    }

    @RedisCommand(name = "zpopmax", arity = -2, flags = "write fast", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zpopmax(RedisClient client, byte[][] argv) {
        return zpop(client, argv, true);
    }

    private byte[] zpop(RedisClient client, byte[][] argv, boolean max) {
        RedisDb db = db(client);
        RedisObject obj = db.lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return RespEncoder.EMPTY_ARRAY;
        int count = 1;
        if (argv.length > 2) {
            try { count = (int) Long.parseLong(toStr(argv[2])); }
            catch (NumberFormatException e) { throw RedisException.notInteger(); }
        }
        long len = zsetCard(obj);
        List<ZSetEntry> entries = zsetRangeByRank(obj, max ? len - count : 0, max ? len - 1 : count - 1, max);
        List<Object> result = new ArrayList<>();
        for (ZSetEntry e : entries) {
            zsetDelete(obj, e.member);
            result.add(e.member);
            result.add(toBytes(formatScore(e.score)));
        }
        if (zsetCard(obj) == 0) db.delete(argv[1]);
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "zrandmember", arity = -2, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zrandmember(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (obj == null) return argv.length == 2 ? RespEncoder.NULL_BULK : RespEncoder.EMPTY_ARRAY;
        List<ZSetEntry> all = zsetRangeByRank(obj, 0, -1, false);
        if (all.isEmpty()) return argv.length == 2 ? RespEncoder.NULL_BULK : RespEncoder.EMPTY_ARRAY;

        if (argv.length == 2) {
            ZSetEntry e = all.get(new Random().nextInt(all.size()));
            return RespEncoder.encodeBulkString(e.member);
        }
        long count;
        try { count = Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        boolean withScores = argv.length > 3 && toStr(argv[3]).equalsIgnoreCase("WITHSCORES");

        List<Object> result = new ArrayList<>();
        if (count >= 0) {
            Collections.shuffle(all);
            for (int i = 0; i < Math.min(count, all.size()); i++) {
                result.add(all.get(i).member);
                if (withScores) result.add(toBytes(formatScore(all.get(i).score)));
            }
        } else {
            for (int i = 0; i < -count; i++) {
                ZSetEntry e = all.get(new Random().nextInt(all.size()));
                result.add(e.member);
                if (withScores) result.add(toBytes(formatScore(e.score)));
            }
        }
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "zunionstore", arity = -4, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zunionstore(RedisClient client, byte[][] argv) {
        return zstore(client, argv, false);
    }

    @RedisCommand(name = "zinterstore", arity = -4, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zinterstore(RedisClient client, byte[][] argv) {
        return zstore(client, argv, true);
    }

    private byte[] zstore(RedisClient client, byte[][] argv, boolean intersect) {
        int numKeys;
        try { numKeys = (int) Long.parseLong(toStr(argv[2])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        RedisDb db = db(client);

        Map<String, Double> result = null;
        for (int k = 0; k < numKeys; k++) {
            RedisObject obj = db.lookupKeyOrReply(argv[3 + k], RedisObjectConstants.OBJ_TYPE_ZSET);
            Map<String, Double> scores = new LinkedHashMap<>();
            if (obj != null) {
                for (ZSetEntry e : zsetRangeByRank(obj, 0, -1, false)) {
                    scores.put(toStr(e.member), e.score);
                }
            }
            if (result == null) {
                result = new LinkedHashMap<>(scores);
            } else if (intersect) {
                result.keySet().retainAll(scores.keySet());
                for (Map.Entry<String, Double> e : result.entrySet()) {
                    e.setValue(e.getValue() + scores.getOrDefault(e.getKey(), 0.0));
                }
            } else {
                for (Map.Entry<String, Double> e : scores.entrySet()) {
                    result.merge(e.getKey(), e.getValue(), Double::sum);
                }
            }
        }

        db.delete(argv[1]);
        if (result == null || result.isEmpty()) return RespEncoder.ZERO;
        RedisObject dst = createZSet();
        for (Map.Entry<String, Double> e : result.entrySet()) {
            zsetAdd(dst, e.getValue(), toBytes(e.getKey()));
        }
        db.setKey(argv[1], dst);
        return RespEncoder.encodeInteger(result.size());
    }

    @RedisCommand(name = "zscan", arity = -3, flags = "read-only random", firstKey = 1, lastKey = 1, step = 1)
    public byte[] zscan(RedisClient client, byte[][] argv) {
        RedisObject obj = db(client).lookupKeyOrReply(argv[1], RedisObjectConstants.OBJ_TYPE_ZSET);
        List<Object> items = new ArrayList<>();
        if (obj != null) {
            for (ZSetEntry e : zsetRangeByRank(obj, 0, -1, false)) {
                items.add(e.member);
                items.add(toBytes(formatScore(e.score)));
            }
        }
        List<Object> result = new ArrayList<>();
        result.add(toBytes("0"));
        result.add(RespEncoder.encodeArray(items));
        return RespEncoder.encodeArray(result);
    }

    @RedisCommand(name = "zrangestore", arity = -5, flags = "write denyoom", firstKey = 1, lastKey = 2, step = 1)
    public byte[] zrangestore(RedisClient client, byte[][] argv) {
        RedisDb db = db(client);
        RedisObject src = db.lookupKeyOrReply(argv[2], RedisObjectConstants.OBJ_TYPE_ZSET);
        if (src == null) { db.delete(argv[1]); return RespEncoder.ZERO; }
        long start, stop;
        try { start = Long.parseLong(toStr(argv[3])); stop = Long.parseLong(toStr(argv[4])); }
        catch (NumberFormatException e) { throw RedisException.notInteger(); }
        List<ZSetEntry> entries = zsetRangeByRank(src, start, stop, false);
        db.delete(argv[1]);
        if (entries.isEmpty()) return RespEncoder.ZERO;
        RedisObject dst = createZSet();
        for (ZSetEntry e : entries) zsetAdd(dst, e.score, e.member);
        db.setKey(argv[1], dst);
        return RespEncoder.encodeInteger(entries.size());
    }

    // ---- Encoding helper ----

    private static byte[] encodeEntries(List<ZSetEntry> entries, boolean withScores) {
        List<Object> result = new ArrayList<>();
        for (ZSetEntry e : entries) {
            result.add(e.member);
            if (withScores) result.add(toBytes(formatScore(e.score)));
        }
        return RespEncoder.encodeArray(result);
    }
}
