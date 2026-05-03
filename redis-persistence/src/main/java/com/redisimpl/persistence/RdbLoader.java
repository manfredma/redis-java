package com.redisimpl.persistence;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFException;
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

/**
 * RdbLoader — reads an RDB file and restores database state.
 *
 * <p>Supports RDB version 11 (REDIS0011).
 * Validates CRC64 checksum on load.
 */
public final class RdbLoader {

    private static final Logger log = LoggerFactory.getLogger(RdbLoader.class);

    private final RedisConfig config;

    public RdbLoader(RedisConfig config) {
        this.config = config;
    }

    /**
     * Load the RDB file into the given databases.
     * If the file does not exist, this is a no-op.
     *
     * @throws IOException if the file exists but is corrupt or unreadable
     */
    public void load(RedisDb[] dbs) throws IOException {
        String filePath = config.getRdbFilePath();
        if (!Files.exists(Paths.get(filePath))) {
            log.info("RDB file not found at {}, skipping load", filePath);
            return;
        }

        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        validateCrc(fileBytes);

        DataInputStream in = new DataInputStream(
                new BufferedInputStream(new ByteArrayInputStream(fileBytes)));
        readRdb(in, dbs);
        log.info("RDB loaded from {}", filePath);
    }

    /**
     * Load RDB from a raw byte array (used for replication full sync).
     */
    public void loadFromBytes(byte[] rdbData, RedisDb[] dbs) throws IOException {
        validateCrc(rdbData);
        DataInputStream in = new DataInputStream(
                new BufferedInputStream(new ByteArrayInputStream(rdbData)));
        readRdb(in, dbs);
        log.info("RDB loaded from replication stream: {} bytes", rdbData.length);
    }

    private void validateCrc(byte[] fileBytes) throws IOException {
        if (fileBytes.length < 9 + 9) { // magic + EOF + CRC
            throw new IOException("RDB file too short");
        }
        // CRC covers everything except the last 8 bytes
        long expectedCrc = Crc64.digest(fileBytes, 0, fileBytes.length - 8);
        long actualCrc = Crc64.readLE64(fileBytes, fileBytes.length - 8);
        if (expectedCrc != actualCrc) {
            throw new IOException(String.format(
                    "RDB CRC64 mismatch: expected %016x, got %016x", expectedCrc, actualCrc));
        }
    }

    private void readRdb(DataInputStream in, RedisDb[] dbs) throws IOException {
        // Read magic
        byte[] magic = new byte[9];
        in.readFully(magic);
        String magicStr = new String(magic, StandardCharsets.US_ASCII);
        if (!magicStr.startsWith("REDIS")) {
            throw new IOException("Not an RDB file (bad magic: " + magicStr + ")");
        }

        RedisDb currentDb = dbs[0];
        long expiry = 0;

        while (true) {
            int opcode = in.read();
            if (opcode == -1) throw new IOException("Unexpected EOF in RDB");

            if (opcode == RdbSaver.RDB_OPCODE_EOF) {
                // Skip the 8-byte CRC (already validated)
                break;
            }

            if (opcode == RdbSaver.RDB_OPCODE_SELECTDB) {
                int dbIndex = (int) readRdbLen(in);
                if (dbIndex >= 0 && dbIndex < dbs.length) {
                    currentDb = dbs[dbIndex];
                }
                expiry = 0;
                continue;
            }

            if (opcode == RdbSaver.RDB_OPCODE_RESIZEDB) {
                readRdbLen(in); // key count
                readRdbLen(in); // expire count
                continue;
            }

            if (opcode == RdbSaver.RDB_OPCODE_AUX) {
                readRdbStringBytes(in); // key
                readRdbStringBytes(in); // value
                continue;
            }

            if (opcode == RdbSaver.RDB_OPCODE_EXPIRETIME_MS) {
                expiry = readLE64(in);
                opcode = in.read(); // read the actual type byte
            }

            // opcode is now the type byte
            byte[] key = readRdbStringBytes(in);
            RedisObject obj = readValue(in, opcode);

            // Skip expired keys
            if (expiry > 0 && expiry <= System.currentTimeMillis()) {
                expiry = 0;
                continue;
            }

            currentDb.setKey(key, obj);
            if (expiry > 0) {
                currentDb.setExpiry(key, expiry);
                expiry = 0;
            }
        }
    }

    private RedisObject readValue(DataInputStream in, int type) throws IOException {
        switch (type) {
            case RdbSaver.RDB_TYPE_STRING:
                return readString(in);

            case RdbSaver.RDB_TYPE_LIST:
                return readListPlain(in);

            case RdbSaver.RDB_TYPE_LIST_QUICKLIST2:
                return readListQuicklist2(in);

            case RdbSaver.RDB_TYPE_HASH:
                return readHashHt(in);

            case RdbSaver.RDB_TYPE_HASH_ZIPLIST:
                return readHashListpack(in);

            case RdbSaver.RDB_TYPE_SET:
                return readSetHt(in);

            case RdbSaver.RDB_TYPE_SET_INTSET:
                return readSetIntset(in);

            case RdbSaver.RDB_TYPE_ZSET_2:
                return readZSetSkiplist(in);

            case RdbSaver.RDB_TYPE_ZSET_ZIPLIST:
                return readZSetListpack(in);

            case RdbSaver.RDB_TYPE_STREAM_LISTPACKS:
                return readStream(in);

            default:
                throw new IOException("Unknown RDB type: " + type);
        }
    }

    // ---- Type readers ----

    private RedisObject readString(DataInputStream in) throws IOException {
        byte[] bytes = readRdbStringBytes(in);
        // Try to detect integer encoding
        if (bytes.length <= 20) {
            try {
                long val = Long.parseLong(new String(bytes, StandardCharsets.US_ASCII));
                return RedisObject.createObject(
                        RedisObjectConstants.OBJ_TYPE_STRING,
                        RedisObjectConstants.OBJ_ENCODING_INT,
                        val);
            } catch (NumberFormatException ignored) {}
        }
        // Use Sds as ptr so StringCommands can cast to Sds correctly
        com.redisimpl.core.sds.Sds sds = com.redisimpl.core.sds.Sds.fromBytes(bytes);
        int encoding = bytes.length <= 44
                ? RedisObjectConstants.OBJ_ENCODING_EMBSTR
                : RedisObjectConstants.OBJ_ENCODING_RAW;
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                encoding,
                sds);
    }

    private RedisObject readListPlain(DataInputStream in) throws IOException {
        long count = readRdbLen(in);
        QuickList ql = QuickList.create();
        for (long i = 0; i < count; i++) {
            ql = ql.rpush(readRdbStringBytes(in));
        }
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_LIST,
                RedisObjectConstants.OBJ_ENCODING_QUICKLIST,
                ql);
    }

    private RedisObject readListQuicklist2(DataInputStream in) throws IOException {
        long count = readRdbLen(in);
        QuickList ql = QuickList.create();
        for (long i = 0; i < count; i++) {
            byte[] elem = readRdbStringBytes(in);
            readRdbLen(in); // container type (PLAIN=1, PACKED=2)
            ql = ql.rpush(elem);
        }
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_LIST,
                RedisObjectConstants.OBJ_ENCODING_QUICKLIST,
                ql);
    }

    private RedisObject readHashHt(DataInputStream in) throws IOException {
        long count = readRdbLen(in);
        Dict dict = Dict.create();
        for (long i = 0; i < count; i++) {
            byte[] field = readRdbStringBytes(in);
            byte[] value = readRdbStringBytes(in);
            dict.put(field, value);
        }
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_HASH,
                RedisObjectConstants.OBJ_ENCODING_HT,
                dict);
    }

    private RedisObject readHashListpack(DataInputStream in) throws IOException {
        long count = readRdbLen(in);
        ListPack lp = ListPack.create();
        for (long i = 0; i < count; i++) {
            lp = lp.append(readRdbStringBytes(in)); // field
            lp = lp.append(readRdbStringBytes(in)); // value
        }
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_HASH,
                RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                lp);
    }

    private RedisObject readSetHt(DataInputStream in) throws IOException {
        long count = readRdbLen(in);
        Dict dict = Dict.create();
        for (long i = 0; i < count; i++) {
            byte[] member = readRdbStringBytes(in);
            dict.put(member, Boolean.TRUE);
        }
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_SET,
                RedisObjectConstants.OBJ_ENCODING_HT,
                dict);
    }

    private RedisObject readSetIntset(DataInputStream in) throws IOException {
        byte[] intsetBytes = readRdbStringBytes(in);
        IntSet is = deserializeIntSet(intsetBytes);
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_SET,
                RedisObjectConstants.OBJ_ENCODING_INTSET,
                is);
    }

    private RedisObject readZSetSkiplist(DataInputStream in) throws IOException {
        long count = readRdbLen(in);
        ZSetCommands.ZSetData data = new ZSetCommands.ZSetData();
        for (long i = 0; i < count; i++) {
            byte[] member = readRdbStringBytes(in);
            double score = readDouble(in);
            data.zsl.insert(score, member);
            data.dict.put(member, score);
        }
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_ZSET,
                RedisObjectConstants.OBJ_ENCODING_SKIPLIST,
                data);
    }

    private RedisObject readZSetListpack(DataInputStream in) throws IOException {
        long count = readRdbLen(in);
        ListPack lp = ListPack.create();
        for (long i = 0; i < count; i++) {
            lp = lp.append(readRdbStringBytes(in)); // member
            lp = lp.append(readRdbStringBytes(in)); // score string
        }
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_ZSET,
                RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                lp);
    }

    /**
     * Read a Stream object from RDB.
     * Mirrors the RDB_TYPE_STREAM_LISTPACKS load path in rdb.c.
     *
     * Our simplified format (written by writeStream):
     *   N_listpacks → for each: id_bytes + fields_bytes
     *   length + last_ms + last_seq + first_ms + first_seq + max_del_ms + max_del_seq + entries_added
     *   N_groups → for each: name + last_ms + last_seq + entries_read + pel_count + N_consumers
     *     each consumer: name + seen_time + active_time + pel_count
     */
    @SuppressWarnings("unchecked")
    private RedisObject readStream(DataInputStream in) throws IOException {
        com.redisimpl.server.commands.stream.StreamObject stream =
                new com.redisimpl.server.commands.stream.StreamObject();

        long nEntries = readRdbLen(in);
        for (long i = 0; i < nEntries; i++) {
            byte[] idBytes = readRdbStringBytes(in);
            byte[] fieldsBytes = readRdbStringBytes(in);
            String id = new String(idBytes, java.nio.charset.StandardCharsets.UTF_8);

            java.util.Map<String, String> fields = new java.util.LinkedHashMap<>();
            java.io.DataInputStream fis = new java.io.DataInputStream(
                    new java.io.ByteArrayInputStream(fieldsBytes));
            int fieldCount = fis.read() & 0xFF;
            for (int fi = 0; fi < fieldCount; fi++) {
                long flen = readRdbLen(fis);
                byte[] fb = new byte[(int) flen]; fis.readFully(fb);
                long vlen = readRdbLen(fis);
                byte[] vb = new byte[(int) vlen]; fis.readFully(vb);
                fields.put(new String(fb, java.nio.charset.StandardCharsets.UTF_8),
                           new String(vb, java.nio.charset.StandardCharsets.UTF_8));
            }
            stream.addRdb(id, fields);
        }

        /* length */        readRdbLen(in);
        long lastMs  =      readRdbLen(in);
        long lastSeq =      readRdbLen(in);
        /* first_id */      readRdbLen(in); readRdbLen(in);
        /* max_del */       readRdbLen(in); readRdbLen(in);
        /* entriesAdded */  readRdbLen(in);
        stream.setLastId(lastMs, lastSeq);

        long nGroups = readRdbLen(in);
        for (long g = 0; g < nGroups; g++) {
            String groupName = new String(readRdbStringBytes(in),
                    java.nio.charset.StandardCharsets.UTF_8);
            long lastDelMs  = readRdbLen(in);
            long lastDelSeq = readRdbLen(in);
            /* entries_read */ readRdbLen(in);
            /* pel_count */    readRdbLen(in);
            stream.createGroup(groupName, lastDelMs + "-" + lastDelSeq);

            long nConsumers = readRdbLen(in);
            for (long c = 0; c < nConsumers; c++) {
                String consName = new String(readRdbStringBytes(in),
                        java.nio.charset.StandardCharsets.UTF_8);
                /* seen_time */   readRdbLen(in);
                /* active_time */ readRdbLen(in);
                /* pel_count */   readRdbLen(in);
                stream.createGroup(groupName, lastDelMs + "-" + lastDelSeq)
                      .getOrCreateConsumer(consName);
            }
        }

        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STREAM,
                RedisObjectConstants.OBJ_ENCODING_STREAM,
                stream);
    }

    // ---- Low-level decoding helpers ----

    /**
     * Read a length-encoded integer from the stream.
     * Also handles special encodings (INT8/16/32, LZF) — returns the length value.
     * For special encodings, returns a negative sentinel; use readRdbStringBytes() instead.
     */
    static long readRdbLen(DataInputStream in) throws IOException {
        int first = in.read();
        if (first == -1) throw new EOFException("EOF reading RDB length");
        int type = (first & 0xC0) >> 6;
        switch (type) {
            case 0: // 6-bit length
                return first & 0x3F;
            case 1: // 14-bit length
                int second = in.read();
                return ((long) (first & 0x3F) << 8) | (second & 0xFF);
            case 2: // 32-bit length (big-endian)
                return ((long) (in.read() & 0xFF) << 24)
                        | ((long) (in.read() & 0xFF) << 16)
                        | ((long) (in.read() & 0xFF) << 8)
                        | (in.read() & 0xFF);
            case 3: // Special encoding
                return -(first & 0x3F) - 1; // negative sentinel
            default:
                throw new IOException("Invalid RDB length encoding: " + first);
        }
    }

    /**
     * Read an RDB-encoded string as raw bytes.
     * Handles: raw bytes, INT8/16/32, LZF.
     */
    static byte[] readRdbStringBytes(DataInputStream in) throws IOException {
        int first = in.read();
        if (first == -1) throw new EOFException("EOF reading RDB string");
        int type = (first & 0xC0) >> 6;

        if (type == 3) {
            // Special encoding
            int enc = first & 0x3F;
            switch (enc) {
                case RdbSaver.RDB_ENC_INT8: {
                    int val = in.read();
                    return String.valueOf((byte) val).getBytes(StandardCharsets.US_ASCII);
                }
                case RdbSaver.RDB_ENC_INT16: {
                    int lo = in.read();
                    int hi = in.read();
                    short val = (short) (lo | (hi << 8));
                    return String.valueOf(val).getBytes(StandardCharsets.US_ASCII);
                }
                case RdbSaver.RDB_ENC_INT32: {
                    int b0 = in.read(), b1 = in.read(), b2 = in.read(), b3 = in.read();
                    int val = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
                    return String.valueOf(val).getBytes(StandardCharsets.US_ASCII);
                }
                case RdbSaver.RDB_ENC_LZF: {
                    long compressedLen = readRdbLenFromByte(in, 0);
                    long originalLen = readRdbLenFromByte(in, 0); // stored but not needed by LZFDecoder
                    byte[] compressed = new byte[(int) compressedLen];
                    in.readFully(compressed);
                    try {
                        return LZFDecoder.decode(compressed);
                    } catch (LZFException e) {
                        throw new IOException("LZF decompression failed", e);
                    }
                }
                default:
                    throw new IOException("Unknown RDB string encoding: " + enc);
            }
        }

        // Regular length-prefixed string
        long len;
        switch (type) {
            case 0:
                len = first & 0x3F;
                break;
            case 1:
                len = ((long) (first & 0x3F) << 8) | (in.read() & 0xFF);
                break;
            case 2:
                len = ((long) (in.read() & 0xFF) << 24)
                        | ((long) (in.read() & 0xFF) << 16)
                        | ((long) (in.read() & 0xFF) << 8)
                        | (in.read() & 0xFF);
                break;
            default:
                throw new IOException("Unexpected length type: " + type);
        }

        byte[] bytes = new byte[(int) len];
        in.readFully(bytes);
        return bytes;
    }

    /** Read a length from stream starting with the already-read first byte (for LZF). */
    private static long readRdbLenFromByte(DataInputStream in, int firstByte) throws IOException {
        // Re-read properly
        int first = in.read();
        if (first == -1) throw new EOFException();
        int type = (first & 0xC0) >> 6;
        switch (type) {
            case 0: return first & 0x3F;
            case 1: return ((long) (first & 0x3F) << 8) | (in.read() & 0xFF);
            case 2:
                return ((long) (in.read() & 0xFF) << 24)
                        | ((long) (in.read() & 0xFF) << 16)
                        | ((long) (in.read() & 0xFF) << 8)
                        | (in.read() & 0xFF);
            default:
                throw new IOException("Bad length in LZF block: " + first);
        }
    }

    private static long readLE64(DataInputStream in) throws IOException {
        long val = 0;
        for (int i = 0; i < 8; i++) {
            val |= ((long) (in.read() & 0xFF)) << (8 * i);
        }
        return val;
    }

    private static double readDouble(DataInputStream in) throws IOException {
        long bits = readLE64(in);
        return Double.longBitsToDouble(bits);
    }

    private static IntSet deserializeIntSet(byte[] data) {
        if (data.length < 8) return IntSet.create();
        int enc = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8)
                | ((data[2] & 0xFF) << 16) | ((data[3] & 0xFF) << 24);
        int len = (data[4] & 0xFF) | ((data[5] & 0xFF) << 8)
                | ((data[6] & 0xFF) << 16) | ((data[7] & 0xFF) << 24);

        IntSet is = IntSet.create();
        for (int i = 0; i < len; i++) {
            int offset = 8 + i * enc;
            long val = 0;
            for (int b = 0; b < enc; b++) {
                val |= ((long) (data[offset + b] & 0xFF)) << (8 * b);
            }
            // Sign-extend
            if (enc == 2) val = (short) val;
            else if (enc == 4) val = (int) val;
            is = is.add(val);
        }
        return is;
    }
}
