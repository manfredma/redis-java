package com.redisimpl.persistence;

import com.ning.compress.lzf.LZFEncoder;
import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.intset.IntSet;
import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.quicklist.QuickList;
import com.redisimpl.core.zskiplist.ZSkipList;
import com.redisimpl.core.zskiplist.ZSkipListNode;
import com.redisimpl.server.commands.zset.ZSetCommands;
import com.redisimpl.server.db.RedisDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RdbSaver — writes Redis databases to an RDB file (version 11).
 *
 * <p>Format: REDIS0011 [db-sections] 0xFF [8-byte CRC64 LE]
 *
 * <p>Each db-section:
 * <pre>
 *   0xFE [db-index as length-encoded int]
 *   0xFB [key-count] [expire-count]  (RDB_OPCODE_RESIZEDB)
 *   [key-value-pairs...]
 * </pre>
 *
 * <p>Each key-value pair:
 * <pre>
 *   [0xFC [8-byte expiry ms LE]]?   (optional expiry)
 *   [type-byte]
 *   [key as rdb-string]
 *   [value encoded by type]
 * </pre>
 */
public final class RdbSaver {

    private static final Logger log = LoggerFactory.getLogger(RdbSaver.class);

    // ---- RDB opcodes ----
    static final int RDB_OPCODE_EOF         = 0xFF;
    static final int RDB_OPCODE_SELECTDB    = 0xFE;
    static final int RDB_OPCODE_EXPIRETIME_MS = 0xFC;
    static final int RDB_OPCODE_RESIZEDB    = 0xFB;
    static final int RDB_OPCODE_AUX         = 0xFA;

    // ---- RDB type bytes ----
    static final int RDB_TYPE_STRING        = 0;
    static final int RDB_TYPE_LIST          = 1;
    static final int RDB_TYPE_SET           = 2;
    static final int RDB_TYPE_ZSET_2        = 5;  // zset v2 (double as binary)
    static final int RDB_TYPE_HASH          = 4;
    static final int RDB_TYPE_SET_INTSET    = 11;
    static final int RDB_TYPE_ZSET_ZIPLIST  = 12; // listpack encoded zset
    static final int RDB_TYPE_HASH_ZIPLIST  = 13; // listpack encoded hash
    static final int RDB_TYPE_LIST_QUICKLIST2   = 18;
    static final int RDB_TYPE_STREAM_LISTPACKS  = 15; // Stream (basic, used for our impl)

    // ---- RDB string encoding special types ----
    static final int RDB_ENC_INT8           = 0;
    static final int RDB_ENC_INT16          = 1;
    static final int RDB_ENC_INT32          = 2;
    static final int RDB_ENC_LZF            = 3;

    // ---- LZF compression threshold ----
    private static final int LZF_COMPRESS_THRESHOLD = 20;

    private final RedisConfig config;
    private final AtomicLong lastSaveTime = new AtomicLong(0);
    private final AtomicBoolean bgSaveInProgress = new AtomicBoolean(false);

    public RdbSaver(RedisConfig config) {
        this.config = config;
    }

    // ---- Public API ----

    /**
     * Synchronously save all databases to the RDB file.
     */
    public void save(RedisDb[] dbs) throws IOException {
        String filePath = config.getRdbFilePath();
        String tmpPath = filePath + ".tmp." + System.currentTimeMillis();

        try (CrcOutputStream out = new CrcOutputStream(new BufferedOutputStream(
                new FileOutputStream(tmpPath)))) {
            writeRdb(out, dbs);
        }

        // Atomic rename
        Files.move(Paths.get(tmpPath), Paths.get(filePath), StandardCopyOption.REPLACE_EXISTING);
        lastSaveTime.set(System.currentTimeMillis() / 1000);
        log.info("RDB saved to {}", filePath);
    }

    /**
     * Asynchronously save databases in a background thread.
     */
    public void bgSave(final RedisDb[] dbs) {
        if (bgSaveInProgress.compareAndSet(false, true)) {
            Thread t = new Thread(() -> {
                try {
                    save(dbs);
                } catch (IOException e) {
                    log.error("BGSAVE failed", e);
                } finally {
                    bgSaveInProgress.set(false);
                }
            }, "redis-bgsave");
            t.setDaemon(true);
            t.start();
        }
    }

    public boolean isBgSaveInProgress() {
        return bgSaveInProgress.get();
    }

    public long getLastSaveTime() {
        return lastSaveTime.get();
    }

    /**
     * Serialize databases to an in-memory stream (used for replication full sync).
     */
    public void saveToStream(OutputStream outputStream, RedisDb[] dbs) throws IOException {
        try (CrcOutputStream out = new CrcOutputStream(new BufferedOutputStream(outputStream))) {
            writeRdb(out, dbs);
        }
    }

    // ---- Write logic ----

    private void writeRdb(CrcOutputStream out, RedisDb[] dbs) throws IOException {
        // Magic + version
        out.write("REDIS0011".getBytes(StandardCharsets.US_ASCII));

        // AUX fields
        writeAux(out, "redis-ver", "7.0.0-java");
        writeAux(out, "redis-bits", "64");

        // Databases
        for (RedisDb db : dbs) {
            if (db.dbSize() == 0) continue;
            writeDb(out, db);
        }

        // EOF
        out.write(RDB_OPCODE_EOF);

        // CRC64 checksum
        long crc = out.getCrc();
        byte[] crcBytes = new byte[8];
        Crc64.writeLE64(crc, crcBytes, 0);
        // Write raw (no CRC update for the checksum itself)
        out.writeRaw(crcBytes);
    }

    private void writeAux(CrcOutputStream out, String key, String value) throws IOException {
        out.write(RDB_OPCODE_AUX);
        writeRdbString(out, key.getBytes(StandardCharsets.UTF_8));
        writeRdbString(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private void writeDb(CrcOutputStream out, RedisDb db) throws IOException {
        // SELECTDB opcode
        out.write(RDB_OPCODE_SELECTDB);
        writeRdbLen(out, db.getId());

        // RESIZEDB opcode
        out.write(RDB_OPCODE_RESIZEDB);
        writeRdbLen(out, db.dbSize());
        writeRdbLen(out, 0); // expire count (approximate)

        // Key-value pairs
        for (Dict.Entry entry : db.getDict()) {
            byte[] key = entry.getKey();
            RedisObject obj = (RedisObject) entry.getValue();

            // Write expiry if present
            long expiry = db.getRawExpiry(key);
            if (expiry > 0) {
                out.write(RDB_OPCODE_EXPIRETIME_MS);
                writeLE64(out, expiry);
            }

            // Write type + key + value
            writeKeyValue(out, key, obj);
        }
    }

    private void writeKeyValue(CrcOutputStream out, byte[] key, RedisObject obj) throws IOException {
        int type = obj.getType();
        int encoding = obj.getEncoding();

        switch (type) {
            case RedisObjectConstants.OBJ_TYPE_STRING:
                out.write(RDB_TYPE_STRING);
                writeRdbString(out, key);
                writeStringObject(out, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_LIST:
                writeList(out, key, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_HASH:
                writeHash(out, key, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_SET:
                writeSet(out, key, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_ZSET:
                writeZSet(out, key, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_STREAM:
                writeStream(out, key, obj);
                break;

            default:
                log.warn("Unknown object type {}, skipping key", type);
        }
    }

    // ---- String ----

    private void writeStringObject(CrcOutputStream out, RedisObject obj) throws IOException {
        int encoding = obj.getEncoding();
        if (encoding == RedisObjectConstants.OBJ_ENCODING_INT) {
            long val = (Long) obj.getPtr();
            writeRdbIntegerEncoding(out, val);
        } else {
            byte[] bytes;
            if (obj.getPtr() instanceof com.redisimpl.core.sds.Sds) {
                bytes = ((com.redisimpl.core.sds.Sds) obj.getPtr()).toBytes();
            } else if (obj.getPtr() instanceof byte[]) {
                bytes = (byte[]) obj.getPtr();
            } else {
                bytes = obj.getPtr().toString().getBytes(StandardCharsets.UTF_8);
            }
            writeRdbString(out, bytes);
        }
    }

    // ---- List ----

    private void writeList(CrcOutputStream out, byte[] key, RedisObject obj) throws IOException {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_QUICKLIST) {
            out.write(RDB_TYPE_LIST_QUICKLIST2);
            writeRdbString(out, key);
            QuickList ql = (QuickList) obj.getPtr();
            long qlSize = ql.llen();
            writeRdbLen(out, qlSize);
            for (long i = 0; i < qlSize; i++) {
                byte[] elem = ql.index(i);
                writeRdbString(out, elem);
                writeRdbLen(out, 1); // container: PLAIN=1
            }
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            // listpack-encoded list: serialize as quicklist with single listpack node
            out.write(RDB_TYPE_LIST_QUICKLIST2);
            writeRdbString(out, key);
            ListPack lp = (ListPack) obj.getPtr();
            writeRdbLen(out, lp.size());
            for (int i = 0; i < lp.size(); i++) {
                writeRdbString(out, lp.get(i));
                writeRdbLen(out, 1);
            }
        }
    }

    // ---- Hash ----

    private void writeHash(CrcOutputStream out, byte[] key, RedisObject obj) throws IOException {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            out.write(RDB_TYPE_HASH_ZIPLIST);
            writeRdbString(out, key);
            ListPack lp = (ListPack) obj.getPtr();
            // Write as field-value pairs count then entries
            writeRdbLen(out, lp.size() / 2);
            for (int i = 0; i < lp.size(); i += 2) {
                writeRdbString(out, lp.get(i));     // field
                writeRdbString(out, lp.get(i + 1)); // value
            }
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_HT) {
            out.write(RDB_TYPE_HASH);
            writeRdbString(out, key);
            Dict dict = (Dict) obj.getPtr();
            writeRdbLen(out, dict.size());
            for (Dict.Entry e : dict) {
                writeRdbString(out, e.getKey());
                writeRdbString(out, (byte[]) e.getValue());
            }
        }
    }

    // ---- Set ----

    private void writeSet(CrcOutputStream out, byte[] key, RedisObject obj) throws IOException {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
            out.write(RDB_TYPE_SET_INTSET);
            writeRdbString(out, key);
            IntSet is = (IntSet) obj.getPtr();
            // Serialize intset as raw bytes
            byte[] intsetBytes = serializeIntSet(is);
            writeRdbString(out, intsetBytes);
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_HT
                || obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            out.write(RDB_TYPE_SET);
            writeRdbString(out, key);
            Dict dict = (Dict) obj.getPtr();
            writeRdbLen(out, dict.size());
            for (Dict.Entry e : dict) {
                writeRdbString(out, e.getKey());
            }
        }
    }

    // ---- ZSet ----

    private void writeZSet(CrcOutputStream out, byte[] key, RedisObject obj) throws IOException {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            out.write(RDB_TYPE_ZSET_ZIPLIST);
            writeRdbString(out, key);
            ListPack lp = (ListPack) obj.getPtr();
            writeRdbLen(out, lp.size() / 2);
            for (int i = 0; i < lp.size(); i += 2) {
                writeRdbString(out, lp.get(i));     // member
                writeRdbString(out, lp.get(i + 1)); // score as string
            }
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_SKIPLIST) {
            out.write(RDB_TYPE_ZSET_2);
            writeRdbString(out, key);
            ZSetCommands.ZSetData data = (ZSetCommands.ZSetData) obj.getPtr();
            writeRdbLen(out, data.zsl.length());
            // Iterate skiplist in order
            ZSkipListNode node = data.zsl.getHeader().getLevels()[0].forward;
            while (node != null) {
                writeRdbString(out, node.getEle());
                writeDouble(out, node.getScore());
                node = node.getLevels()[0].forward;
            }
        }
    }

    // ---- Stream ----

    /**
     * Serialize a Stream object to RDB using RDB_TYPE_STREAM_LISTPACKS format.
     *
     * Mirrors the OBJ_STREAM case in rdbSaveObjectLen/rdbSaveObject() in rdb.c.
     *
     * Format (RDB_TYPE_STREAM_LISTPACKS = 15):
     *   type_byte(1) + key_string
     *   N_listpacks(len)
     *   for each entry: entry_key_str + listpack_bytes_str
     *   total_length(len)
     *   last_id_ms(len) + last_id_seq(len)
     *   first_id_ms(len) + first_id_seq(len)
     *   max_deleted_ms(len) + max_deleted_seq(len)
     *   entries_added(len)
     *   N_groups(len)
     *   for each group:
     *     group_name_str
     *     last_delivered_ms(len) + last_delivered_seq(len)
     *     entries_read(len)
     *     pel_count(len) = 0  (simplified: no PEL serialization)
     *     N_consumers(len) = 0
     */
    @SuppressWarnings("unchecked")
    private void writeStream(CrcOutputStream out, byte[] key, RedisObject obj) throws IOException {
        com.redisimpl.server.commands.stream.StreamObject stream =
                (com.redisimpl.server.commands.stream.StreamObject) obj.getPtr();
        if (stream == null) return;

        out.write(RDB_TYPE_STREAM_LISTPACKS);
        writeRdbString(out, key);

        // Serialize all entries as individual "listpack" nodes
        // In our simplified implementation each entry is one node.
        // We serialize all entries as a single "master node".
        java.util.List<com.redisimpl.server.commands.stream.StreamEntry> entries =
                stream.range("-", "+", Integer.MAX_VALUE);

        // N listpacks — we use 1 node containing all entries serialized as RESP
        if (entries.isEmpty()) {
            writeRdbLen(out, 0); // 0 listpacks
        } else {
            writeRdbLen(out, entries.size()); // one "node" per entry for simplicity
            for (com.redisimpl.server.commands.stream.StreamEntry entry : entries) {
                // key: entry ID as bytes
                byte[] idBytes = entry.getId().getBytes(StandardCharsets.UTF_8);
                writeRdbString(out, idBytes);
                // value: serialized fields as a listpack-like byte sequence
                // We encode fields as [count, field1, val1, field2, val2, ...]
                java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
                bos.write((byte) entry.getFields().size()); // field count (1 byte)
                for (java.util.Map.Entry<String, String> f : entry.getFields().entrySet()) {
                    byte[] fb = f.getKey().getBytes(StandardCharsets.UTF_8);
                    byte[] vb = f.getValue().getBytes(StandardCharsets.UTF_8);
                    writeRdbLen(bos, fb.length); bos.write(fb);
                    writeRdbLen(bos, vb.length); bos.write(vb);
                }
                writeRdbString(out, bos.toByteArray());
            }
        }

        // stream metadata
        writeRdbLen(out, stream.size());                        // total length
        writeRdbLen(out, stream.getLastMillis());               // last_id.ms
        writeRdbLen(out, stream.getLastSeq());                  // last_id.seq
        // first_id
        String firstId = entries.isEmpty() ? "0-0" : entries.get(0).getId();
        String[] fParts = firstId.split("-");
        writeRdbLen(out, Long.parseLong(fParts[0]));
        writeRdbLen(out, Long.parseLong(fParts[1]));
        // max_deleted_entry_id (0-0 = none)
        writeRdbLen(out, 0);
        writeRdbLen(out, 0);
        // entries_added
        writeRdbLen(out, stream.size());

        // Consumer groups
        java.util.Map<String, com.redisimpl.server.commands.stream.StreamConsumerGroup> groups =
                stream.getGroups();
        writeRdbLen(out, groups.size());
        for (com.redisimpl.server.commands.stream.StreamConsumerGroup g : groups.values()) {
            writeRdbString(out, g.getName().getBytes(StandardCharsets.UTF_8));
            writeRdbLen(out, g.getLastDeliveredMillis()); // last_delivered_id.ms
            writeRdbLen(out, g.getLastDeliveredSeq());    // last_delivered_id.seq
            writeRdbLen(out, 0);  // entries_read
            writeRdbLen(out, 0);  // PEL count (simplified)
            writeRdbLen(out, g.getConsumers().size()); // consumer count
            for (com.redisimpl.server.commands.stream.StreamConsumerGroup.StreamConsumer c
                    : g.getConsumers().values()) {
                writeRdbString(out, c.getName().getBytes(StandardCharsets.UTF_8));
                writeRdbLen(out, c.getSeenTime()); // seen-time
                writeRdbLen(out, 0); // active-time
                writeRdbLen(out, 0); // consumer PEL count
            }
        }
    }

    // ---- Low-level encoding helpers ----

    /**
     * Write a length-encoded integer (RDB length encoding).
     * - 0-63: 1 byte (00xxxxxx)
     * - 64-16383: 2 bytes (01xxxxxx xxxxxxxx)
     * - 16384+: 5 bytes (10000000 + 4 bytes BE)
     */
    static void writeRdbLen(OutputStream out, long len) throws IOException {
        if (len < 0) throw new IOException("Negative length: " + len);
        if (len <= 63) {
            out.write((int) len);
        } else if (len <= 16383) {
            out.write((int) (0x40 | (len >> 8)));
            out.write((int) (len & 0xFF));
        } else {
            out.write(0x80);
            out.write((int) ((len >> 24) & 0xFF));
            out.write((int) ((len >> 16) & 0xFF));
            out.write((int) ((len >> 8) & 0xFF));
            out.write((int) (len & 0xFF));
        }
    }

    /**
     * Write a string with RDB encoding:
     * - Try integer encoding first (INT8/16/32)
     * - Try LZF compression for long strings
     * - Fall back to raw bytes with length prefix
     */
    static void writeRdbString(OutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) bytes = new byte[0];

        // Try integer encoding
        if (bytes.length <= 20) {
            try {
                long val = Long.parseLong(new String(bytes, StandardCharsets.US_ASCII));
                if (writeRdbIntegerEncoding(out, val)) return;
            } catch (NumberFormatException ignored) {}
        }

        // Try LZF compression for strings >= threshold
        if (bytes.length >= LZF_COMPRESS_THRESHOLD) {
            try {
                byte[] compressed = LZFEncoder.encode(bytes);
                if (compressed.length < bytes.length) {
                    // Write LZF encoded: 0xC0|3, compressed_len (lzf stream), original_len, lzf_stream
                    // We store the LZF stream length and original length, then the LZF stream bytes
                    out.write(0xC0 | RDB_ENC_LZF);
                    writeRdbLen(out, compressed.length);
                    writeRdbLen(out, bytes.length);
                    out.write(compressed);
                    return;
                }
            } catch (Exception ignored) {}
        }

        // Raw string
        writeRdbLen(out, bytes.length);
        out.write(bytes);
    }

    /**
     * Try to write an integer using RDB integer encoding.
     * Returns true if successful (value fits in INT8/16/32).
     */
    static boolean writeRdbIntegerEncoding(OutputStream out, long val) throws IOException {
        if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE) {
            out.write(0xC0 | RDB_ENC_INT8);
            out.write((int) (val & 0xFF));
            return true;
        } else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
            out.write(0xC0 | RDB_ENC_INT16);
            out.write((int) (val & 0xFF));
            out.write((int) ((val >> 8) & 0xFF));
            return true;
        } else if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
            out.write(0xC0 | RDB_ENC_INT32);
            out.write((int) (val & 0xFF));
            out.write((int) ((val >> 8) & 0xFF));
            out.write((int) ((val >> 16) & 0xFF));
            out.write((int) ((val >> 24) & 0xFF));
            return true;
        }
        return false;
    }

    private static void writeLE64(OutputStream out, long val) throws IOException {
        for (int i = 0; i < 8; i++) {
            out.write((int) (val & 0xFF));
            val >>>= 8;
        }
    }

    private static void writeDouble(OutputStream out, double score) throws IOException {
        long bits = Double.doubleToLongBits(score);
        writeLE64(out, bits);
    }

    /**
     * Serialize an IntSet to its binary representation.
     * Format: [4-byte encoding LE] [4-byte length LE] [elements...]
     */
    private static byte[] serializeIntSet(IntSet is) {
        int enc = is.getEncoding();
        int len = is.getLength();
        byte[] out = new byte[8 + len * enc];
        // encoding (LE)
        out[0] = (byte) (enc & 0xFF);
        out[1] = (byte) ((enc >> 8) & 0xFF);
        out[2] = (byte) ((enc >> 16) & 0xFF);
        out[3] = (byte) ((enc >> 24) & 0xFF);
        // length (LE)
        out[4] = (byte) (len & 0xFF);
        out[5] = (byte) ((len >> 8) & 0xFF);
        out[6] = (byte) ((len >> 16) & 0xFF);
        out[7] = (byte) ((len >> 24) & 0xFF);
        // elements
        byte[] contents = is.getContents();
        System.arraycopy(contents, 0, out, 8, contents.length);
        return out;
    }

    // ---- CRC-tracking output stream ----

    /**
     * An OutputStream that tracks CRC64 of all written bytes,
     * with a separate writeRaw() that bypasses CRC tracking (for the checksum itself).
     */
    static final class CrcOutputStream extends FilterOutputStream {
        private long crc = 0L;

        CrcOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            byte[] tmp = {(byte) b};
            crc = Crc64.digest(crc, tmp, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            crc = Crc64.digest(crc, b, off, len);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        /** Write bytes without updating CRC (used for the checksum field itself). */
        void writeRaw(byte[] b) throws IOException {
            out.write(b);
        }

        long getCrc() { return crc; }
    }
}
