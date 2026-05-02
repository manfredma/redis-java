package com.redisimpl.core.sds;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Simple Dynamic String — Java port of Redis sds.c.
 *
 * Encoding type selection mirrors sdsReqType():
 *   TYPE_5:  len <  32   (no alloc field; len stored in 5 high bits of flags)
 *   TYPE_8:  len <= 255  (uint8 len/alloc)
 *   TYPE_16: len <= 65535
 *   TYPE_32: len <= 4GB-1
 *   TYPE_64: larger
 *
 * Memory growth strategy mirrors sdsMakeRoomFor() with greedy=1:
 *   newLen < SDS_MAX_PREALLOC (1MB) → alloc = newLen * 2
 *   newLen >= SDS_MAX_PREALLOC      → alloc = newLen + SDS_MAX_PREALLOC
 *   If appending, use TYPE_8 minimum (not TYPE_5) to preserve alloc field.
 *
 * Java implementation is immutable (each mutation returns a new Sds).
 */
public final class Sds {

    // ---- Encoding types (mirrors SDS_TYPE_* in sds.h) ----
    public static final int SDS_TYPE_5  = 0; // len < 32
    public static final int SDS_TYPE_8  = 1; // len <= 0xFF
    public static final int SDS_TYPE_16 = 2; // len <= 0xFFFF
    public static final int SDS_TYPE_32 = 3; // len <= 0xFFFFFFFFL
    public static final int SDS_TYPE_64 = 4; // larger

    private static final long SDS_MAX_PREALLOC = 1024 * 1024L; // 1 MB

    // Maximum len for each type (mirrors sdsTypeMaxSize())
    private static final long TYPE5_MAX  = (1L << 5) - 1;   // 31
    private static final long TYPE8_MAX  = (1L << 8) - 1;   // 255
    private static final long TYPE16_MAX = (1L << 16) - 1;  // 65535
    private static final long TYPE32_MAX = (1L << 32) - 1;  // 4294967295

    /** Content bytes. Only [0, len) are valid. */
    private final byte[] buf;
    /** Number of used bytes. */
    private final int len;
    /** Allocated capacity (== buf.length). */
    private final int alloc;
    /** Encoding type (SDS_TYPE_*). */
    private final int type;

    private Sds(byte[] buf, int len) {
        this.buf   = buf;
        this.len   = len;
        this.alloc = buf.length;
        this.type  = sdsReqType(len);
    }

    // ---- Factory methods ----

    public static Sds empty() {
        return new Sds(new byte[0], 0);
    }

    public static Sds fromString(String s) {
        if (s == null || s.isEmpty()) return empty();
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new Sds(bytes.clone(), bytes.length);
    }

    public static Sds fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return empty();
        return new Sds(bytes.clone(), bytes.length);
    }

    // ---- Encoding type selection (mirrors sdsReqType()) ----

    /**
     * Returns the SDS encoding type for a string of the given length.
     * Mirrors sdsReqType() in sds.c exactly.
     */
    public static int sdsReqType(long len) {
        if (len < (1L << 5))  return SDS_TYPE_5;
        if (len <= TYPE8_MAX) return SDS_TYPE_8;
        if (len <= TYPE16_MAX) return SDS_TYPE_16;
        if (len <= TYPE32_MAX) return SDS_TYPE_32;
        return SDS_TYPE_64;
    }

    // ---- Core operations ----

    /**
     * Append bytes, returning a new Sds.
     * Mirrors sdsMakeRoomFor(greedy=1) + sdscatlen().
     * When appending, TYPE_5 is never used (no alloc field) → minimum TYPE_8.
     */
    public Sds append(byte[] addition) {
        if (addition == null || addition.length == 0) return this;
        long newLen = (long) this.len + addition.length;
        long newAlloc = greedyAlloc(newLen);
        byte[] newBuf = new byte[(int) newAlloc];
        System.arraycopy(this.buf, 0, newBuf, 0, this.len);
        System.arraycopy(addition, 0, newBuf, this.len, addition.length);
        return new Sds(newBuf, (int) newLen);
    }

    public Sds appendStr(String s) {
        if (s == null || s.isEmpty()) return this;
        return append(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Grow to newLen bytes, zero-filling the added region.
     * Mirrors sdsgrowzero().
     */
    public Sds sdsgrowzero(int newLen) {
        if (newLen <= this.len) return this;
        byte[] newBuf = Arrays.copyOf(this.buf, newLen);
        // Arrays.copyOf zero-fills the new portion
        return new Sds(newBuf, newLen);
    }

    /**
     * Return sub-range [start, end] (inclusive, supports negative indices).
     * Mirrors sdsrange().
     */
    public Sds sdsrange(int start, int end) {
        if (len == 0) return empty();
        if (start < 0) start = Math.max(len + start, 0);
        if (end < 0)   end   = len + end;
        if (start > end || start >= len) return empty();
        end = Math.min(end, len - 1);
        int rangeLen = end - start + 1;
        byte[] newBuf = new byte[rangeLen];
        System.arraycopy(buf, start, newBuf, 0, rangeLen);
        return new Sds(newBuf, rangeLen);
    }

    /**
     * Grow allocation to hold addlen extra bytes without copying content.
     * Mirrors sdsMakeRoomForNonGreedy() — exact size, no doubling.
     */
    public Sds makeRoomFor(int addLen) {
        if (this.alloc - this.len >= addLen) return this;
        int newLen = this.len + addLen;
        byte[] newBuf = new byte[newLen];
        System.arraycopy(this.buf, 0, newBuf, 0, this.len);
        return new Sds(newBuf, this.len);
    }

    // ---- Greedy allocation (mirrors _sdsMakeRoomFor greedy=1) ----

    private static long greedyAlloc(long newLen) {
        // Don't use TYPE_5 when making room (can't track alloc)
        long alloc = newLen;
        if (alloc < SDS_MAX_PREALLOC) {
            alloc *= 2;
        } else {
            alloc += SDS_MAX_PREALLOC;
        }
        return alloc;
    }

    // ---- Accessors ----

    public int getLen()   { return len; }
    public int getAlloc() { return alloc; }
    public int getType()  { return type; }

    public String toStr() {
        return new String(buf, 0, len, StandardCharsets.UTF_8);
    }

    public byte[] toBytes() {
        return Arrays.copyOf(buf, len);
    }

    public boolean isEmpty() { return len == 0; }

    public int length() { return len; }

    // ---- Comparison (mirrors sdscmp()) ----

    public int compareTo(Sds other) {
        int minLen = Math.min(this.len, other.len);
        for (int i = 0; i < minLen; i++) {
            int diff = (this.buf[i] & 0xFF) - (other.buf[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return this.len - other.len;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Sds)) return false;
        Sds other = (Sds) o;
        if (this.len != other.len) return false;
        for (int i = 0; i < len; i++) {
            if (this.buf[i] != other.buf[i]) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i = 0; i < len; i++) result = 31 * result + buf[i];
        return result;
    }

    @Override
    public String toString() {
        return "Sds{len=" + len + ", alloc=" + alloc
                + ", type=" + type + ", content=\"" + toStr() + "\"}";
    }
}
