package com.redisimpl.replication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Replication backlog — circular buffer for partial resynchronization.
 * Mirrors Redis's repl_backlog in replication.c.
 *
 * The backlog stores the last N bytes of commands sent to replicas.
 * When a replica reconnects, it can request partial resync from a given offset
 * if that offset is still in the backlog.
 */
public final class ReplicationBacklog {

    private final byte[] buffer;
    private final int capacity;

    /** Total bytes ever written to the backlog */
    private final AtomicLong masterOffset = new AtomicLong(0);

    /** Index into buffer where next write goes */
    private int writePos = 0;

    /** Whether the buffer has been filled at least once (wrapped) */
    private volatile boolean wrapped = false;

    public ReplicationBacklog(int capacity) {
        this.capacity = capacity;
        this.buffer = new byte[capacity];
    }

    /**
     * Append data to the backlog (called after each write command is propagated).
     */
    public synchronized void append(byte[] data) {
        if (data == null || data.length == 0) return;

        int len = data.length;
        masterOffset.addAndGet(len);

        if (len >= capacity) {
            // Data larger than backlog: just keep the last `capacity` bytes
            System.arraycopy(data, len - capacity, buffer, 0, capacity);
            writePos = 0;
            wrapped = true;
            return;
        }

        int end = writePos + len;
        if (end <= capacity) {
            System.arraycopy(data, 0, buffer, writePos, len);
            writePos = end % capacity;
        } else {
            // Wrap around
            int firstPart = capacity - writePos;
            System.arraycopy(data, 0, buffer, writePos, firstPart);
            System.arraycopy(data, firstPart, buffer, 0, len - firstPart);
            writePos = len - firstPart;
            wrapped = true;
        }
    }

    /**
     * Get data from offset to current master offset (up to maxLen bytes).
     * Returns null if the offset is not in the backlog.
     */
    public synchronized byte[] getFrom(long fromOffset, int maxLen) {
        long currentOffset = masterOffset.get();
        if (fromOffset > currentOffset) return null;
        if (!canPartialResync(fromOffset)) return null;

        long available = currentOffset - fromOffset;
        int toRead = (int) Math.min(available, maxLen);
        if (toRead <= 0) return new byte[0];

        byte[] result = new byte[toRead];

        // Calculate read position in circular buffer
        long backlogStart = currentOffset - (wrapped ? capacity : writePos);
        long relativeOffset = fromOffset - backlogStart;
        int readPos = (int) (relativeOffset % capacity);

        int firstPart = Math.min(toRead, capacity - readPos);
        System.arraycopy(buffer, readPos, result, 0, firstPart);
        if (firstPart < toRead) {
            System.arraycopy(buffer, 0, result, firstPart, toRead - firstPart);
        }
        return result;
    }

    /**
     * Returns true if the given offset is within the backlog range.
     */
    public synchronized boolean canPartialResync(long offset) {
        long currentOffset = masterOffset.get();
        if (offset > currentOffset) return false;

        long backlogStart = currentOffset - (wrapped ? capacity : writePos);
        return offset >= backlogStart;
    }

    public long getMasterOffset() {
        return masterOffset.get();
    }

    public int getCapacity() {
        return capacity;
    }
}
