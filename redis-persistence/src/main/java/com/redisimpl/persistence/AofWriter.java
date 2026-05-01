package com.redisimpl.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AofWriter — appends write commands to the AOF file in RESP format.
 *
 * <p>Supports three fsync strategies:
 * <ul>
 *   <li>{@code always} — fsync after every write (safest, slowest)</li>
 *   <li>{@code everysec} — fsync in background every second (default)</li>
 *   <li>{@code no} — let OS decide when to flush (fastest, least safe)</li>
 * </ul>
 *
 * <p>Thread safety: {@link #appendCommand} may be called from any thread.
 * The underlying FileOutputStream is synchronized.
 */
public final class AofWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AofWriter.class);

    private final RedisConfig config;
    private final FileOutputStream fos;
    private final BufferedOutputStream bos;
    private final Object writeLock = new Object();

    /** Currently selected database (to avoid redundant SELECT commands). */
    private final AtomicInteger currentDb = new AtomicInteger(-1);

    /** Background fsync scheduler (used for everysec strategy). */
    private ScheduledExecutorService fsyncScheduler;

    public AofWriter(RedisConfig config) throws IOException {
        this.config = config;
        String path = config.getAofFilePath();
        // Ensure parent directory exists
        File dir = new File(path).getParentFile();
        if (dir != null && !dir.exists()) dir.mkdirs();

        this.fos = new FileOutputStream(path, true); // append mode
        this.bos = new BufferedOutputStream(fos);

        if ("everysec".equalsIgnoreCase(config.getAppendfsync())) {
            startEverysecFsync();
        }
    }

    private void startEverysecFsync() {
        fsyncScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "redis-aof-fsync");
            t.setDaemon(true);
            return t;
        });
        fsyncScheduler.scheduleAtFixedRate(() -> {
            try {
                synchronized (writeLock) {
                    bos.flush();
                    fos.getFD().sync();
                }
            } catch (IOException e) {
                log.error("AOF everysec fsync failed", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Append a write command to the AOF file.
     *
     * @param dbIndex the database index the command was executed in
     * @param argv    the command arguments (argv[0] is the command name)
     */
    public void appendCommand(int dbIndex, byte[][] argv) throws IOException {
        synchronized (writeLock) {
            // Emit SELECT if database changed
            if (dbIndex != currentDb.get()) {
                if (dbIndex != 0 || currentDb.get() != -1) {
                    // Only emit SELECT for non-zero db, or if we previously selected a different db
                    if (dbIndex != 0) {
                        writeRespCommand(new byte[][]{
                                "SELECT".getBytes(StandardCharsets.UTF_8),
                                String.valueOf(dbIndex).getBytes(StandardCharsets.UTF_8)
                        });
                    }
                }
                currentDb.set(dbIndex);
            }

            writeRespCommand(argv);

            String fsync = config.getAppendfsync();
            if ("always".equalsIgnoreCase(fsync)) {
                bos.flush();
                fos.getFD().sync();
            }
            // "everysec": flushed by background thread
            // "no": OS decides
        }
    }

    /**
     * Write a command as a RESP array to the underlying stream.
     */
    private void writeRespCommand(byte[][] argv) throws IOException {
        // *<argc>\r\n
        bos.write(('*' + String.valueOf(argv.length) + "\r\n")
                .getBytes(StandardCharsets.US_ASCII));
        for (byte[] arg : argv) {
            // $<len>\r\n<data>\r\n
            bos.write(('$' + String.valueOf(arg.length) + "\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            bos.write(arg);
            bos.write('\r');
            bos.write('\n');
        }
    }

    /**
     * Flush any buffered data to the OS.
     */
    public void flush() throws IOException {
        synchronized (writeLock) {
            bos.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (fsyncScheduler != null) {
            fsyncScheduler.shutdown();
        }
        synchronized (writeLock) {
            bos.flush();
            fos.close();
        }
    }
}
