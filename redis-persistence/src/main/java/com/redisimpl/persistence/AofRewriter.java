package com.redisimpl.persistence;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.intset.IntSet;
import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.quicklist.QuickList;
import com.redisimpl.core.zskiplist.ZSkipListNode;
import com.redisimpl.server.commands.zset.ZSetCommands;
import com.redisimpl.server.db.RedisDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AofRewriter — performs AOF rewrite (BGREWRITEAOF).
 *
 * <p>The rewrite process:
 * <ol>
 *   <li>If {@code aof-use-rdb-preamble yes}: write an RDB snapshot first</li>
 *   <li>Then write current database state as AOF commands</li>
 *   <li>Append any incremental commands buffered during the rewrite</li>
 *   <li>Atomically replace the old AOF file</li>
 * </ol>
 *
 * <p>Incremental commands can be buffered via {@link #appendToBuffer} while
 * a rewrite is in progress; they will be appended after the rewrite completes.
 */
public final class AofRewriter {

    private static final Logger log = LoggerFactory.getLogger(AofRewriter.class);

    private final RedisConfig config;
    private final AtomicBoolean rewriteInProgress = new AtomicBoolean(false);

    /** Buffer for commands written during an active rewrite. */
    private final List<byte[]> incrementalBuffer = new CopyOnWriteArrayList<>();

    public AofRewriter(RedisConfig config) {
        this.config = config;
    }

    /**
     * Buffer a command to be appended after the current rewrite completes.
     * Commands are always buffered; the buffer is consumed when the rewrite finishes.
     *
     * @param dbIndex the database index
     * @param argv    the command arguments
     */
    public void appendToBuffer(int dbIndex, byte[][] argv) {
        incrementalBuffer.add(encodeRespCommand(dbIndex, argv));
    }

    /**
     * Start an asynchronous AOF rewrite in a background thread.
     *
     * @param dbs snapshot of the current database state
     */
    public void bgRewrite(final RedisDb[] dbs) {
        if (!rewriteInProgress.compareAndSet(false, true)) {
            log.warn("BGREWRITEAOF already in progress");
            return;
        }
        // Note: do NOT clear incrementalBuffer here — commands buffered before bgRewrite
        // should also be included in the output.

        Thread t = new Thread(() -> {
            try {
                doRewrite(dbs);
            } catch (IOException e) {
                log.error("BGREWRITEAOF failed", e);
            } finally {
                rewriteInProgress.set(false);
            }
        }, "redis-aof-rewrite");
        t.setDaemon(true);
        t.start();
    }

    public boolean isRewriteInProgress() {
        return rewriteInProgress.get();
    }

    // ---- Rewrite logic ----

    private void doRewrite(RedisDb[] dbs) throws IOException {
        String aofPath = config.getAofFilePath();
        String tmpPath = aofPath + ".rewrite." + System.currentTimeMillis();

        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tmpPath))) {
            if (config.isAofUseRdbPreamble()) {
                writeRdbPreamble(out, dbs);
            } else {
                writeAofBody(out, dbs);
            }

            // Append incremental buffer
            for (byte[] chunk : incrementalBuffer) {
                out.write(chunk);
            }
        }

        // Atomic rename
        Files.move(Paths.get(tmpPath), Paths.get(aofPath), StandardCopyOption.REPLACE_EXISTING);
        log.info("BGREWRITEAOF completed: {}", aofPath);
    }

    /**
     * Write an RDB snapshot as the preamble of the AOF file.
     */
    private void writeRdbPreamble(OutputStream out, RedisDb[] dbs) throws IOException {
        // Write RDB to a temp file, then copy its bytes into the AOF stream
        String tmpRdb = config.getRdbFilePath() + ".aof-preamble." + System.currentTimeMillis();
        RedisConfig tmpConfig = new RedisConfig();
        String rdbPath = config.getRdbFilePath();
        int lastSlash = Math.max(rdbPath.lastIndexOf('/'), rdbPath.lastIndexOf('\\'));
        if (lastSlash >= 0) {
            tmpConfig.setDir(rdbPath.substring(0, lastSlash));
            tmpConfig.setDbfilename(rdbPath.substring(lastSlash + 1) + ".aof-preamble." + System.currentTimeMillis());
        } else {
            tmpConfig.setDir("./");
            tmpConfig.setDbfilename(rdbPath + ".aof-preamble." + System.currentTimeMillis());
        }

        RdbSaver saver = new RdbSaver(tmpConfig);
        saver.save(dbs);

        byte[] rdbBytes = Files.readAllBytes(Paths.get(tmpConfig.getRdbFilePath()));
        out.write(rdbBytes);

        // Clean up temp RDB
        new File(tmpConfig.getRdbFilePath()).delete();
    }

    /**
     * Write database state as AOF commands.
     */
    private void writeAofBody(OutputStream out, RedisDb[] dbs) throws IOException {
        for (RedisDb db : dbs) {
            if (db.dbSize() == 0) continue;

            // SELECT dbIndex
            if (db.getId() != 0) {
                out.write(encodeRespCommand(new byte[][]{
                        "SELECT".getBytes(StandardCharsets.UTF_8),
                        String.valueOf(db.getId()).getBytes(StandardCharsets.UTF_8)
                }));
            }

            for (Dict.Entry entry : db.getDict()) {
                byte[] key = entry.getKey();
                RedisObject obj = (RedisObject) entry.getValue();

                // Skip expired keys
                long expiry = db.getRawExpiry(key);
                if (expiry > 0 && expiry <= System.currentTimeMillis()) continue;

                // Write the key as a command
                writeKeyAsAofCommand(out, key, obj);

                // Write expiry if present
                if (expiry > 0) {
                    out.write(encodeRespCommand(new byte[][]{
                            "PEXPIREAT".getBytes(StandardCharsets.UTF_8),
                            key,
                            String.valueOf(expiry).getBytes(StandardCharsets.UTF_8)
                    }));
                }
            }
        }
    }

    private void writeKeyAsAofCommand(OutputStream out, byte[] key, RedisObject obj) throws IOException {
        switch (obj.getType()) {
            case RedisObjectConstants.OBJ_TYPE_STRING:
                out.write(encodeRespCommand(new byte[][]{
                        "SET".getBytes(StandardCharsets.UTF_8),
                        key,
                        getStringBytes(obj)
                }));
                break;

            case RedisObjectConstants.OBJ_TYPE_LIST:
                writeListCommand(out, key, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_HASH:
                writeHashCommand(out, key, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_SET:
                writeSetCommand(out, key, obj);
                break;

            case RedisObjectConstants.OBJ_TYPE_ZSET:
                writeZSetCommand(out, key, obj);
                break;

            default:
                log.warn("Unknown type {} for key {}, skipping", obj.getType(), new String(key));
        }
    }

    private void writeListCommand(OutputStream out, byte[] key, RedisObject obj) throws IOException {
        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_QUICKLIST) {
            QuickList ql = (QuickList) obj.getPtr();
            long size = ql.llen();
            if (size == 0) return;
            // Build RPUSH key e1 e2 ... (batch up to 64 elements)
            List<byte[]> args = new ArrayList<>();
            args.add("RPUSH".getBytes(StandardCharsets.UTF_8));
            args.add(key);
            for (long i = 0; i < size; i++) {
                args.add(ql.index(i));
                if (args.size() >= 66) { // 64 elements + RPUSH + key
                    out.write(encodeRespCommand(args.toArray(new byte[0][])));
                    args.clear();
                    args.add("RPUSH".getBytes(StandardCharsets.UTF_8));
                    args.add(key);
                }
            }
            if (args.size() > 2) {
                out.write(encodeRespCommand(args.toArray(new byte[0][])));
            }
        }
    }

    private void writeHashCommand(OutputStream out, byte[] key, RedisObject obj) throws IOException {
        List<byte[]> args = new ArrayList<>();
        args.add("HSET".getBytes(StandardCharsets.UTF_8));
        args.add(key);

        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_HT) {
            Dict dict = (Dict) obj.getPtr();
            for (Dict.Entry e : dict) {
                args.add(e.getKey());
                args.add((byte[]) e.getValue());
            }
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i + 1 < lp.size(); i += 2) {
                args.add(lp.get(i));
                args.add(lp.get(i + 1));
            }
        }

        if (args.size() > 2) {
            out.write(encodeRespCommand(args.toArray(new byte[0][])));
        }
    }

    private void writeSetCommand(OutputStream out, byte[] key, RedisObject obj) throws IOException {
        List<byte[]> args = new ArrayList<>();
        args.add("SADD".getBytes(StandardCharsets.UTF_8));
        args.add(key);

        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_INTSET) {
            IntSet is = (IntSet) obj.getPtr();
            for (long val : is.toArray()) {
                args.add(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
            }
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_HT) {
            Dict dict = (Dict) obj.getPtr();
            for (Dict.Entry e : dict) {
                args.add(e.getKey());
            }
        }

        if (args.size() > 2) {
            out.write(encodeRespCommand(args.toArray(new byte[0][])));
        }
    }

    private void writeZSetCommand(OutputStream out, byte[] key, RedisObject obj) throws IOException {
        List<byte[]> args = new ArrayList<>();
        args.add("ZADD".getBytes(StandardCharsets.UTF_8));
        args.add(key);

        if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_LISTPACK) {
            ListPack lp = (ListPack) obj.getPtr();
            for (int i = 0; i + 1 < lp.size(); i += 2) {
                args.add(lp.get(i + 1)); // score
                args.add(lp.get(i));     // member
            }
        } else if (obj.getEncoding() == RedisObjectConstants.OBJ_ENCODING_SKIPLIST) {
            ZSetCommands.ZSetData data = (ZSetCommands.ZSetData) obj.getPtr();
            ZSkipListNode node = data.zsl.getHeader().getLevels()[0].forward;
            while (node != null) {
                args.add(String.valueOf(node.getScore()).getBytes(StandardCharsets.UTF_8));
                args.add(node.getEle());
                node = node.getLevels()[0].forward;
            }
        }

        if (args.size() > 2) {
            out.write(encodeRespCommand(args.toArray(new byte[0][])));
        }
    }

    // ---- RESP encoding ----

    /**
     * Encode a command with db-index SELECT prefix as raw bytes.
     */
    private static byte[] encodeRespCommand(int dbIndex, byte[][] argv) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (dbIndex != 0) {
            byte[] selectCmd = encodeRespCommand(new byte[][]{
                    "SELECT".getBytes(StandardCharsets.UTF_8),
                    String.valueOf(dbIndex).getBytes(StandardCharsets.UTF_8)
            });
            baos.write(selectCmd, 0, selectCmd.length);
        }
        byte[] cmd = encodeRespCommand(argv);
        baos.write(cmd, 0, cmd.length);
        return baos.toByteArray();
    }

    /**
     * Encode a command as a RESP array.
     */
    static byte[] encodeRespCommand(byte[][] argv) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(('*' + String.valueOf(argv.length) + "\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            for (byte[] arg : argv) {
                baos.write(('$' + String.valueOf(arg.length) + "\r\n")
                        .getBytes(StandardCharsets.US_ASCII));
                baos.write(arg);
                baos.write('\r');
                baos.write('\n');
            }
        } catch (IOException e) {
            // ByteArrayOutputStream never throws
        }
        return baos.toByteArray();
    }

    private static byte[] getStringBytes(RedisObject obj) {
        Object ptr = obj.getPtr();
        if (ptr instanceof byte[]) return (byte[]) ptr;
        if (ptr instanceof Long) return String.valueOf((Long) ptr).getBytes(StandardCharsets.UTF_8);
        return ptr.toString().getBytes(StandardCharsets.UTF_8);
    }
}
