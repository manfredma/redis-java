package com.redisimpl.persistence;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.intset.IntSet;
import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.quicklist.QuickList;
import com.redisimpl.server.commands.zset.ZSetCommands;
import com.redisimpl.server.db.RedisDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * AofLoader — reads an AOF file and replays commands to restore database state.
 *
 * <p>Supports:
 * <ul>
 *   <li>Pure AOF format (RESP arrays)</li>
 *   <li>AOF-RDB mixed format (RDB preamble followed by AOF commands)</li>
 * </ul>
 *
 * <p>Commands replayed: SET, MSET, GET (ignored), DEL, UNLINK, EXPIRE, PEXPIRE,
 * EXPIREAT, PEXPIREAT, PERSIST, SELECT, LPUSH, RPUSH, LPOP, RPOP, LSET,
 * HSET, HMSET, HDEL, SADD, SREM, ZADD, ZREM, INCR, INCRBY, DECR, DECRBY,
 * APPEND, SETEX, PSETEX, SETNX, GETSET, RENAME, RENAMENX, FLUSHDB, FLUSHALL.
 */
public final class AofLoader {

    private static final Logger log = LoggerFactory.getLogger(AofLoader.class);

    /** Magic bytes for RDB preamble detection in AOF */
    private static final byte[] RDB_MAGIC = "REDIS".getBytes(StandardCharsets.US_ASCII);

    private final RedisConfig config;

    public AofLoader(RedisConfig config) {
        this.config = config;
    }

    /**
     * Load the AOF file into the given databases.
     * If the file does not exist, this is a no-op.
     *
     * @throws IOException if the file exists but cannot be read
     */
    public void load(RedisDb[] dbs) throws IOException {
        String filePath = config.getAofFilePath();
        if (!Files.exists(Paths.get(filePath))) {
            log.info("AOF file not found at {}, skipping load", filePath);
            return;
        }

        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        if (fileBytes.length == 0) return;

        // Detect RDB preamble
        if (hasRdbPreamble(fileBytes)) {
            loadMixed(fileBytes, dbs);
        } else {
            loadPureAof(new DataInputStream(
                    new BufferedInputStream(new ByteArrayInputStream(fileBytes))), dbs);
        }

        log.info("AOF loaded from {}", filePath);
    }

    private boolean hasRdbPreamble(byte[] data) {
        if (data.length < RDB_MAGIC.length) return false;
        for (int i = 0; i < RDB_MAGIC.length; i++) {
            if (data[i] != RDB_MAGIC[i]) return false;
        }
        return true;
    }

    /**
     * Load a mixed AOF file: RDB preamble + AOF tail.
     * Finds the boundary by scanning for the first '*' after the RDB EOF.
     */
    private void loadMixed(byte[] fileBytes, RedisDb[] dbs) throws IOException {
        // Find RDB EOF opcode (0xFF) — it's followed by 8-byte CRC
        // Scan from the start to find 0xFF
        int rdbEnd = -1;
        for (int i = 9; i < fileBytes.length - 8; i++) {
            if ((fileBytes[i] & 0xFF) == 0xFF) {
                rdbEnd = i + 1 + 8; // skip 0xFF + 8-byte CRC
                break;
            }
        }

        if (rdbEnd == -1) {
            // Fallback: treat as pure AOF
            loadPureAof(new DataInputStream(new BufferedInputStream(
                    new ByteArrayInputStream(fileBytes))), dbs);
            return;
        }

        // Load RDB portion
        byte[] rdbPortion = Arrays.copyOf(fileBytes, rdbEnd);
        // Write to temp file and load via RdbLoader
        File tmpRdb = File.createTempFile("aof-rdb-", ".rdb");
        try {
            Files.write(tmpRdb.toPath(), rdbPortion);
            RedisConfig tmpConfig = new RedisConfig();
            tmpConfig.setDir(tmpRdb.getParent());
            tmpConfig.setDbfilename(tmpRdb.getName());
            RdbLoader rdbLoader = new RdbLoader(tmpConfig);
            rdbLoader.load(dbs);
        } finally {
            tmpRdb.delete();
        }

        // Load AOF tail
        if (rdbEnd < fileBytes.length) {
            byte[] aofTail = Arrays.copyOfRange(fileBytes, rdbEnd, fileBytes.length);
            loadPureAof(new DataInputStream(new BufferedInputStream(
                    new ByteArrayInputStream(aofTail))), dbs);
        }
    }

    /**
     * Load pure AOF commands (RESP format).
     */
    private void loadPureAof(DataInputStream in, RedisDb[] dbs) throws IOException {
        int currentDbIndex = 0;
        RedisDb currentDb = dbs[0];

        while (true) {
            int first = in.read();
            if (first == -1) break; // EOF
            if (first == '\r' || first == '\n') continue; // skip blank lines

            if (first != '*') {
                // Inline command or garbage — skip line
                skipLine(in);
                continue;
            }

            // Read array length
            String countLine = readLine(in);
            int argc;
            try {
                argc = Integer.parseInt(countLine.trim());
            } catch (NumberFormatException e) {
                continue;
            }

            byte[][] argv = new byte[argc][];
            boolean valid = true;
            for (int i = 0; i < argc; i++) {
                int dollar = in.read();
                if (dollar != '$') { valid = false; break; }
                String lenLine = readLine(in);
                int len;
                try {
                    len = Integer.parseInt(lenLine.trim());
                } catch (NumberFormatException e) {
                    valid = false;
                    break;
                }
                byte[] arg = new byte[len];
                in.readFully(arg);
                in.read(); // \r
                in.read(); // \n
                argv[i] = arg;
            }
            if (!valid || argv[0] == null) continue;

            String cmd = new String(argv[0], StandardCharsets.UTF_8).toUpperCase();

            // Handle SELECT
            if (cmd.equals("SELECT") && argc >= 2) {
                try {
                    currentDbIndex = Integer.parseInt(
                            new String(argv[1], StandardCharsets.UTF_8));
                    if (currentDbIndex >= 0 && currentDbIndex < dbs.length) {
                        currentDb = dbs[currentDbIndex];
                    }
                } catch (NumberFormatException ignored) {}
                continue;
            }

            replayCommand(cmd, argv, currentDb);
        }
    }

    /**
     * Replay a single write command against the given database.
     */
    private void replayCommand(String cmd, byte[][] argv, RedisDb db) {
        try {
            switch (cmd) {
                case "SET":
                    if (argv.length >= 3) {
                        db.setKey(argv[1], strObj(argv[2]));
                        // Handle optional EX/PX/EXAT/PXAT flags
                        for (int i = 3; i < argv.length - 1; i++) {
                            String flag = new String(argv[i], StandardCharsets.UTF_8).toUpperCase();
                            long val = Long.parseLong(new String(argv[i + 1], StandardCharsets.UTF_8));
                            if (flag.equals("EX")) {
                                db.setExpiry(argv[1], System.currentTimeMillis() + val * 1000);
                            } else if (flag.equals("PX")) {
                                db.setExpiry(argv[1], System.currentTimeMillis() + val);
                            } else if (flag.equals("EXAT")) {
                                db.setExpiry(argv[1], val * 1000);
                            } else if (flag.equals("PXAT")) {
                                db.setExpiry(argv[1], val);
                            }
                        }
                    }
                    break;

                case "SETEX":
                    if (argv.length >= 4) {
                        long secs = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        db.setKey(argv[1], strObj(argv[3]));
                        db.setExpiry(argv[1], System.currentTimeMillis() + secs * 1000);
                    }
                    break;

                case "PSETEX":
                    if (argv.length >= 4) {
                        long ms = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        db.setKey(argv[1], strObj(argv[3]));
                        db.setExpiry(argv[1], System.currentTimeMillis() + ms);
                    }
                    break;

                case "SETNX":
                    if (argv.length >= 3 && db.lookupKey(argv[1]) == null) {
                        db.setKey(argv[1], strObj(argv[2]));
                    }
                    break;

                case "MSET":
                case "MSETNX":
                    for (int i = 1; i + 1 < argv.length; i += 2) {
                        db.setKey(argv[i], strObj(argv[i + 1]));
                    }
                    break;

                case "DEL":
                case "UNLINK":
                    for (int i = 1; i < argv.length; i++) {
                        db.delete(argv[i]);
                    }
                    break;

                case "EXPIRE":
                    if (argv.length >= 3) {
                        long secs = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        if (db.exists(argv[1])) {
                            db.setExpiry(argv[1], System.currentTimeMillis() + secs * 1000);
                        }
                    }
                    break;

                case "PEXPIRE":
                    if (argv.length >= 3) {
                        long ms = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        if (db.exists(argv[1])) {
                            db.setExpiry(argv[1], System.currentTimeMillis() + ms);
                        }
                    }
                    break;

                case "EXPIREAT":
                    if (argv.length >= 3) {
                        long ts = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        if (db.exists(argv[1])) {
                            db.setExpiry(argv[1], ts * 1000);
                        }
                    }
                    break;

                case "PEXPIREAT":
                    if (argv.length >= 3) {
                        long ts = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        if (db.exists(argv[1])) {
                            db.setExpiry(argv[1], ts);
                        }
                    }
                    break;

                case "PERSIST":
                    if (argv.length >= 2) db.removeExpiry(argv[1]);
                    break;

                case "RENAME":
                case "RENAMENX":
                    if (argv.length >= 3) {
                        try { db.rename(argv[1], argv[2]); } catch (Exception ignored) {}
                    }
                    break;

                case "INCR":
                    if (argv.length >= 2) incrBy(db, argv[1], 1);
                    break;

                case "INCRBY":
                    if (argv.length >= 3) {
                        long delta = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        incrBy(db, argv[1], delta);
                    }
                    break;

                case "DECR":
                    if (argv.length >= 2) incrBy(db, argv[1], -1);
                    break;

                case "DECRBY":
                    if (argv.length >= 3) {
                        long delta = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                        incrBy(db, argv[1], -delta);
                    }
                    break;

                case "APPEND":
                    if (argv.length >= 3) {
                        RedisObject existing = db.lookupKey(argv[1]);
                        if (existing == null) {
                            db.setKey(argv[1], strObj(argv[2]));
                        } else {
                            byte[] cur = getBytes(existing);
                            byte[] appended = new byte[cur.length + argv[2].length];
                            System.arraycopy(cur, 0, appended, 0, cur.length);
                            System.arraycopy(argv[2], 0, appended, cur.length, argv[2].length);
                            db.setKey(argv[1], strObj(appended));
                        }
                    }
                    break;

                case "GETSET":
                    if (argv.length >= 3) db.setKey(argv[1], strObj(argv[2]));
                    break;

                // List commands
                case "LPUSH":
                case "LPUSHX":
                    if (argv.length >= 3) {
                        RedisObject listObj = getOrCreateList(db, argv[1]);
                        QuickList ql = (QuickList) listObj.getPtr();
                        for (int i = argv.length - 1; i >= 2; i--) {
                            ql = ql.lpush(argv[i]);
                        }
                        listObj.setPtr(ql);
                    }
                    break;

                case "RPUSH":
                case "RPUSHX":
                    if (argv.length >= 3) {
                        RedisObject listObj = getOrCreateList(db, argv[1]);
                        QuickList ql = (QuickList) listObj.getPtr();
                        for (int i = 2; i < argv.length; i++) {
                            ql = ql.rpush(argv[i]);
                        }
                        listObj.setPtr(ql);
                    }
                    break;

                case "LPOP":
                    if (argv.length >= 2) {
                        RedisObject listObj = db.lookupKey(argv[1]);
                        if (listObj != null && listObj.getType() == RedisObjectConstants.OBJ_TYPE_LIST) {
                            QuickList ql = (QuickList) listObj.getPtr();
                            ql = ql.lpopResult().list;
                            if (ql.llen() == 0) db.delete(argv[1]);
                            else listObj.setPtr(ql);
                        }
                    }
                    break;

                case "RPOP":
                    if (argv.length >= 2) {
                        RedisObject listObj = db.lookupKey(argv[1]);
                        if (listObj != null && listObj.getType() == RedisObjectConstants.OBJ_TYPE_LIST) {
                            QuickList ql = (QuickList) listObj.getPtr();
                            ql = ql.rpopResult().list;
                            if (ql.llen() == 0) db.delete(argv[1]);
                            else listObj.setPtr(ql);
                        }
                    }
                    break;

                case "LSET":
                    if (argv.length >= 4) {
                        RedisObject listObj = db.lookupKey(argv[1]);
                        if (listObj != null && listObj.getType() == RedisObjectConstants.OBJ_TYPE_LIST) {
                            long idx = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                            QuickList ql = ((QuickList) listObj.getPtr()).lset(idx, argv[3]);
                            listObj.setPtr(ql);
                        }
                    }
                    break;

                // Hash commands
                case "HSET":
                case "HMSET":
                    if (argv.length >= 4) {
                        RedisObject hashObj = getOrCreateHash(db, argv[1]);
                        Dict dict = (Dict) hashObj.getPtr();
                        for (int i = 2; i + 1 < argv.length; i += 2) {
                            dict.put(argv[i], argv[i + 1]);
                        }
                    }
                    break;

                case "HSETNX":
                    if (argv.length >= 4) {
                        RedisObject hashObj = getOrCreateHash(db, argv[1]);
                        Dict dict = (Dict) hashObj.getPtr();
                        if (dict.get(argv[2]) == null) dict.put(argv[2], argv[3]);
                    }
                    break;

                case "HDEL":
                    if (argv.length >= 3) {
                        RedisObject hashObj = db.lookupKey(argv[1]);
                        if (hashObj != null && hashObj.getType() == RedisObjectConstants.OBJ_TYPE_HASH) {
                            Dict dict = (Dict) hashObj.getPtr();
                            for (int i = 2; i < argv.length; i++) dict.delete(argv[i]);
                        }
                    }
                    break;

                // Set commands
                case "SADD":
                    if (argv.length >= 3) {
                        RedisObject setObj = getOrCreateSet(db, argv[1]);
                        if (setObj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
                            IntSet is = (IntSet) setObj.getPtr();
                            for (int i = 2; i < argv.length; i++) {
                                try {
                                    long v = Long.parseLong(new String(argv[i], StandardCharsets.UTF_8));
                                    is = is.add(v);
                                } catch (NumberFormatException e) {
                                    // Upgrade to HT
                                    Dict d = intSetToDict(is);
                                    d.put(argv[i], Boolean.TRUE);
                                    setObj.setEncoding(RedisObjectConstants.OBJ_ENCODING_HT);
                                    setObj.setPtr(d);
                                    break;
                                }
                            }
                            if (setObj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
                                setObj.setPtr(is);
                            }
                        } else {
                            Dict dict = (Dict) setObj.getPtr();
                            for (int i = 2; i < argv.length; i++) dict.put(argv[i], Boolean.TRUE);
                        }
                    }
                    break;

                case "SREM":
                    if (argv.length >= 3) {
                        RedisObject setObj = db.lookupKey(argv[1]);
                        if (setObj != null && setObj.getType() == RedisObjectConstants.OBJ_TYPE_SET) {
                            if (setObj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
                                IntSet is = (IntSet) setObj.getPtr();
                                for (int i = 2; i < argv.length; i++) {
                                    try {
                                        long v = Long.parseLong(new String(argv[i], StandardCharsets.UTF_8));
                                        is = is.remove(v);
                                    } catch (NumberFormatException ignored) {}
                                }
                                setObj.setPtr(is);
                            } else {
                                Dict dict = (Dict) setObj.getPtr();
                                for (int i = 2; i < argv.length; i++) dict.delete(argv[i]);
                            }
                        }
                    }
                    break;

                // ZSet commands
                case "ZADD":
                    if (argv.length >= 4) {
                        RedisObject zsetObj = getOrCreateZSet(db, argv[1]);
                        // Simple: add score-member pairs (skip flags like NX/XX/GT/LT/CH)
                        int startIdx = 2;
                        // Skip flags
                        while (startIdx < argv.length) {
                            String flag = new String(argv[startIdx], StandardCharsets.UTF_8).toUpperCase();
                            if (flag.equals("NX") || flag.equals("XX") || flag.equals("GT")
                                    || flag.equals("LT") || flag.equals("CH") || flag.equals("INCR")) {
                                startIdx++;
                            } else break;
                        }
                        if (zsetObj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
                            ListPack lp = (ListPack) zsetObj.getPtr();
                            for (int i = startIdx; i + 1 < argv.length; i += 2) {
                                double score = Double.parseDouble(new String(argv[i], StandardCharsets.UTF_8));
                                lp = lp.append(argv[i + 1]);
                                lp = lp.append(String.valueOf(score).getBytes(StandardCharsets.UTF_8));
                            }
                            zsetObj.setPtr(lp);
                        } else {
                            ZSetCommands.ZSetData data = (ZSetCommands.ZSetData) zsetObj.getPtr();
                            for (int i = startIdx; i + 1 < argv.length; i += 2) {
                                double score = Double.parseDouble(new String(argv[i], StandardCharsets.UTF_8));
                                data.zsl.insert(score, argv[i + 1]);
                                data.dict.put(argv[i + 1], score);
                            }
                        }
                    }
                    break;

                case "ZREM":
                    if (argv.length >= 3) {
                        RedisObject zsetObj = db.lookupKey(argv[1]);
                        if (zsetObj != null && zsetObj.getType() == RedisObjectConstants.OBJ_TYPE_ZSET) {
                            if (zsetObj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_SKIPLIST) {
                                ZSetCommands.ZSetData data = (ZSetCommands.ZSetData) zsetObj.getPtr();
                                for (int i = 2; i < argv.length; i++) {
                                    Object scoreObj = data.dict.get(argv[i]);
                                    if (scoreObj instanceof Double) {
                                        data.zsl.delete((Double) scoreObj, argv[i]);
                                        data.dict.delete(argv[i]);
                                    }
                                }
                            }
                        }
                    }
                    break;

                case "FLUSHDB":
                    db.flush();
                    break;

                case "FLUSHALL":
                    // Caller should flush all dbs — we only have current db here
                    db.flush();
                    break;

                default:
                    // Ignore unknown / read-only commands
                    break;
            }
        } catch (Exception e) {
            log.warn("Error replaying AOF command {}: {}", cmd, e.getMessage());
        }
    }

    // ---- Helpers ----

    private static RedisObject strObj(byte[] value) {
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                value);
    }

    private static byte[] getBytes(RedisObject obj) {
        Object ptr = obj.getPtr();
        if (ptr instanceof byte[]) return (byte[]) ptr;
        if (ptr instanceof Long) return String.valueOf((Long) ptr).getBytes(StandardCharsets.UTF_8);
        return ptr.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void incrBy(RedisDb db, byte[] key, long delta) {
        RedisObject obj = db.lookupKey(key);
        long current = 0;
        if (obj != null) {
            try {
                current = Long.parseLong(new String(getBytes(obj), StandardCharsets.UTF_8));
            } catch (NumberFormatException ignored) {}
        }
        long newVal = current + delta;
        db.setKey(key, RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_INT,
                newVal));
    }

    private static RedisObject getOrCreateList(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null || obj.getType() != RedisObjectConstants.OBJ_TYPE_LIST) {
            obj = RedisObject.createObject(
                    RedisObjectConstants.OBJ_TYPE_LIST,
                    RedisObjectConstants.OBJ_ENCODING_QUICKLIST,
                    QuickList.create());
            db.setKey(key, obj);
        }
        return obj;
    }

    private static RedisObject getOrCreateHash(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null || obj.getType() != RedisObjectConstants.OBJ_TYPE_HASH) {
            obj = RedisObject.createObject(
                    RedisObjectConstants.OBJ_TYPE_HASH,
                    RedisObjectConstants.OBJ_ENCODING_HT,
                    Dict.create());
            db.setKey(key, obj);
        }
        // If listpack, upgrade to HT for simplicity
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            Dict dict = Dict.create();
            for (int i = 0; i + 1 < lp.size(); i += 2) {
                dict.put(lp.get(i), lp.get(i + 1));
            }
            obj.setEncoding(RedisObjectConstants.OBJ_ENCODING_HT);
            obj.setPtr(dict);
        }
        return obj;
    }

    private static RedisObject getOrCreateSet(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null || obj.getType() != RedisObjectConstants.OBJ_TYPE_SET) {
            obj = RedisObject.createObject(
                    RedisObjectConstants.OBJ_TYPE_SET,
                    RedisObjectConstants.OBJ_ENCODING_INTSET,
                    IntSet.create());
            db.setKey(key, obj);
        }
        return obj;
    }

    private static RedisObject getOrCreateZSet(RedisDb db, byte[] key) {
        RedisObject obj = db.lookupKey(key);
        if (obj == null || obj.getType() != RedisObjectConstants.OBJ_TYPE_ZSET) {
            obj = RedisObject.createObject(
                    RedisObjectConstants.OBJ_TYPE_ZSET,
                    RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                    ListPack.create());
            db.setKey(key, obj);
        }
        return obj;
    }

    private static Dict intSetToDict(IntSet is) {
        Dict d = Dict.create();
        for (long val : is.toArray()) {
            d.put(String.valueOf(val).getBytes(StandardCharsets.UTF_8), Boolean.TRUE);
        }
        return d;
    }

    private static String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') break;
            if (c != '\r') sb.append((char) c);
        }
        return sb.toString();
    }

    private static void skipLine(DataInputStream in) throws IOException {
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') break;
        }
    }
}
