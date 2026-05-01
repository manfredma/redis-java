package com.redisimpl.core.intset;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Integer Set — Java port of Redis's intset.c.
 *
 * <p>Stores a sorted set of unique integers in a compact byte array.
 * Supports three encodings: INT16, INT32, INT64.
 * Automatically upgrades encoding when a value exceeds the current range.
 *
 * <p>Immutable semantics: all mutating operations return a new IntSet.
 */
@Getter
public final class IntSet {

    /** Encoding constants (bytes per element) */
    public static final int INTSET_ENC_INT16 = 2;
    public static final int INTSET_ENC_INT32 = 4;
    public static final int INTSET_ENC_INT64 = 8;

    /** Max entries before conversion to HT */
    public static final int INTSET_MAX_ENTRIES = 512;

    /** Encoding in use (2, 4, or 8 bytes per element) */
    private final int encoding;

    /** Number of elements */
    private final int length;

    /**
     * Compact storage. Elements are stored in little-endian byte order,
     * sorted in ascending order.
     */
    private final byte[] contents;

    private IntSet(int encoding, int length, byte[] contents) {
        this.encoding = encoding;
        this.length = length;
        this.contents = contents;
    }

    // ---- Factory ----

    public static IntSet create() {
        return new IntSet(INTSET_ENC_INT16, 0, new byte[0]);
    }

    // ---- Core operations ----

    /**
     * Add a value to the set. Returns a new IntSet.
     * If value already exists, returns this unchanged.
     * Upgrades encoding if necessary.
     */
    public IntSet add(long value) {
        int requiredEncoding = valueEncoding(value);
        IntSet set = this;

        // Upgrade if necessary
        if (requiredEncoding > set.encoding) {
            set = set.upgrade(requiredEncoding);
        }

        // Binary search for insertion point
        int pos = set.search(value);
        if (pos >= 0) {
            // Already exists
            return set;
        }
        int insertPos = -(pos + 1);

        // Create new contents with one extra slot
        int newLength = set.length + 1;
        byte[] newContents = new byte[newLength * set.encoding];

        // Copy elements before insertPos
        if (insertPos > 0) {
            System.arraycopy(set.contents, 0, newContents, 0, insertPos * set.encoding);
        }
        // Write the new value
        writeValue(newContents, insertPos, set.encoding, value);
        // Copy elements after insertPos
        if (insertPos < set.length) {
            System.arraycopy(set.contents, insertPos * set.encoding,
                    newContents, (insertPos + 1) * set.encoding,
                    (set.length - insertPos) * set.encoding);
        }

        return new IntSet(set.encoding, newLength, newContents);
    }

    /**
     * Remove a value from the set. Returns a new IntSet.
     * If value does not exist, returns this unchanged.
     */
    public IntSet remove(long value) {
        // If value requires higher encoding, it can't be in this set
        if (valueEncoding(value) > encoding) return this;

        int pos = search(value);
        if (pos < 0) return this; // not found

        int newLength = length - 1;
        byte[] newContents = new byte[newLength * encoding];

        // Copy elements before pos
        if (pos > 0) {
            System.arraycopy(contents, 0, newContents, 0, pos * encoding);
        }
        // Copy elements after pos
        if (pos < newLength) {
            System.arraycopy(contents, (pos + 1) * encoding,
                    newContents, pos * encoding,
                    (newLength - pos) * encoding);
        }

        return new IntSet(encoding, newLength, newContents);
    }

    /**
     * Check if value exists in the set.
     */
    public boolean contains(long value) {
        if (valueEncoding(value) > encoding) return false;
        return search(value) >= 0;
    }

    /**
     * Return all elements as a sorted long array.
     */
    public long[] toArray() {
        long[] result = new long[length];
        for (int i = 0; i < length; i++) {
            result[i] = readValue(i);
        }
        return result;
    }

    // ---- Internal helpers ----

    /**
     * Binary search. Returns index if found, -(insertionPoint+1) if not found.
     */
    private int search(long value) {
        if (length == 0) return -1;
        int lo = 0, hi = length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long midVal = readValue(mid);
            if (midVal == value) return mid;
            if (midVal < value) lo = mid + 1;
            else hi = mid - 1;
        }
        return -(lo + 1);
    }

    /**
     * Read element at index {@code i} using current encoding.
     */
    private long readValue(int i) {
        int offset = i * encoding;
        ByteBuffer buf = ByteBuffer.wrap(contents, offset, encoding).order(ByteOrder.LITTLE_ENDIAN);
        switch (encoding) {
            case INTSET_ENC_INT16: return buf.getShort();
            case INTSET_ENC_INT32: return buf.getInt();
            case INTSET_ENC_INT64: return buf.getLong();
            default: throw new IllegalStateException("Unknown encoding: " + encoding);
        }
    }

    /**
     * Write value at position {@code i} into {@code dest} using {@code enc}.
     */
    private static void writeValue(byte[] dest, int i, int enc, long value) {
        int offset = i * enc;
        ByteBuffer buf = ByteBuffer.wrap(dest, offset, enc).order(ByteOrder.LITTLE_ENDIAN);
        switch (enc) {
            case INTSET_ENC_INT16: buf.putShort((short) value); break;
            case INTSET_ENC_INT32: buf.putInt((int) value); break;
            case INTSET_ENC_INT64: buf.putLong(value); break;
            default: throw new IllegalStateException("Unknown encoding: " + enc);
        }
    }

    /**
     * Upgrade encoding to {@code newEncoding}, preserving all elements.
     */
    public IntSet upgrade(int newEncoding) {
        byte[] newContents = new byte[length * newEncoding];
        for (int i = 0; i < length; i++) {
            long val = readValue(i);
            writeValue(newContents, i, newEncoding, val);
        }
        return new IntSet(newEncoding, length, newContents);
    }

    /**
     * Return the minimum encoding required to store {@code value}.
     */
    private static int valueEncoding(long value) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) return INTSET_ENC_INT16;
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) return INTSET_ENC_INT32;
        return INTSET_ENC_INT64;
    }

    /** Returns the number of elements (same as {@code getLength()}). */
    public int length() {
        return length;
    }

    @Override
    public String toString() {
        return "IntSet{encoding=" + encoding + ", length=" + length + "}";
    }
}
