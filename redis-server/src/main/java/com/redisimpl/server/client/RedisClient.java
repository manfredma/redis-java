package com.redisimpl.server.client;

import com.redisimpl.core.sds.Sds;
import lombok.Getter;
import lombok.Setter;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * RedisClient — Java port of Redis's client structure in server.h.
 *
 * <p>Represents a connected client with its own query buffer, argument vector,
 * and output buffers.
 */
@Getter
@Setter
public final class RedisClient {

    /** Fixed output buffer size (16KB, same as Redis) */
    public static final int REDIS_REPLY_CHUNK_BYTES = 16 * 1024;

    // ---- Client flags ----
    public static final int CLIENT_SLAVE    = 1;
    public static final int CLIENT_MASTER   = 2;
    public static final int CLIENT_MONITOR  = 4;
    public static final int CLIENT_MULTI    = 8;
    public static final int CLIENT_BLOCKED  = 16;
    public static final int CLIENT_CLOSE_AFTER_REPLY = 32;
    public static final int CLIENT_UNBLOCKED = 64;
    public static final int CLIENT_CLOSE_ASAP = 128;

    /** File descriptor (or virtual fd) */
    private final int fd;

    /** Underlying socket channel (null for fake/test clients) */
    private SocketChannel channel;

    /** Input query buffer */
    private Sds querybuf;

    /** Current command argument count */
    private int argc;

    /** Current command argument vector */
    private byte[][] argv;

    /** Current command (set by command lookup) */
    private Object cmd; // will be CommandEntry once command system is built

    /** Client flags */
    private int flags;

    /** Current database index */
    private int db;

    /** Last interaction timestamp (ms) */
    private long lastInteraction;

    // ---- Output buffers ----

    /** Fixed output buffer (16KB) */
    private final byte[] buf;

    /** Current write position in buf */
    private int bufpos;

    /** Dynamic reply list (used when buf is full) */
    private final List<byte[]> reply;

    /** Total bytes in dynamic reply list */
    private long replyBytes;

    public RedisClient(int fd) {
        this.fd = fd;
        this.querybuf = Sds.empty();
        this.argc = 0;
        this.argv = null;
        this.flags = 0;
        this.db = 0;
        this.lastInteraction = System.currentTimeMillis();
        this.buf = new byte[REDIS_REPLY_CHUNK_BYTES];
        this.bufpos = 0;
        this.reply = new ArrayList<>();
        this.replyBytes = 0;
    }

    // ---- Query buffer ----

    public void appendQueryBuf(byte[] data) {
        this.querybuf = this.querybuf.append(data);
        this.lastInteraction = System.currentTimeMillis();
    }

    public void resetQueryBuf() {
        this.querybuf = Sds.empty();
    }

    public void consumeQueryBuf(int len) {
        if (len >= querybuf.length()) {
            resetQueryBuf();
        } else {
            this.querybuf = querybuf.sdsrange(len, -1);
        }
    }

    // ---- Argument vector ----

    public void setArgv(byte[][] argv) {
        this.argv = argv;
        this.argc = argv != null ? argv.length : 0;
    }

    // ---- Output buffers ----

    /**
     * Try to write data to the fixed output buffer.
     * Returns true if successful, false if the buffer doesn't have enough space.
     */
    public boolean addReplyToFixedBuf(byte[] data) {
        if (bufpos + data.length > REDIS_REPLY_CHUNK_BYTES) {
            return false;
        }
        System.arraycopy(data, 0, buf, bufpos, data.length);
        bufpos += data.length;
        return true;
    }

    /**
     * Add data to the dynamic reply list.
     */
    public void addReplyToReplyList(byte[] data) {
        reply.add(data.clone());
        replyBytes += data.length;
    }

    /**
     * Add a reply: first try fixed buf, then overflow to reply list.
     */
    public void addReply(byte[] data) {
        if (!addReplyToFixedBuf(data)) {
            addReplyToReplyList(data);
        }
    }

    /**
     * Reset output buffers.
     */
    public void resetOutputBuffers() {
        bufpos = 0;
        reply.clear();
        replyBytes = 0;
    }

    /**
     * Get all pending output as a single byte array (for testing).
     */
    public byte[] getPendingOutput() {
        int totalLen = bufpos + (int) replyBytes;
        byte[] out = new byte[totalLen];
        System.arraycopy(buf, 0, out, 0, bufpos);
        int offset = bufpos;
        for (byte[] chunk : reply) {
            System.arraycopy(chunk, 0, out, offset, chunk.length);
            offset += chunk.length;
        }
        return out;
    }

    public boolean hasPendingOutput() {
        return bufpos > 0 || !reply.isEmpty();
    }

    @Override
    public String toString() {
        return "RedisClient{fd=" + fd + ", db=" + db + ", flags=" + flags + "}";
    }
}
