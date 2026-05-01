package com.redisimpl.core.sds;

import lombok.Getter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Simple Dynamic String — Java port of Redis's sds.
 *
 * <p>Mirrors the semantics of Redis's sds.c:
 * <ul>
 *   <li>Tracks {@code len} (used bytes) and {@code alloc} (allocated capacity).</li>
 *   <li>Append doubles capacity when {@code len < 1 MB}; adds 1 MB otherwise.</li>
 *   <li>Immutable semantics: mutating operations return a new {@code Sds}.</li>
 * </ul>
 */
@Getter
public final class Sds {

    private static final int SDS_MAX_PREALLOC = 1024 * 1024; // 1 MB

    /** Actual content bytes. Length is {@code len}, not {@code buf.length}. */
    private final byte[] buf;

    /** Number of used bytes. */
    private final int len;

    /** Allocated capacity (== buf.length). */
    private final int alloc;

    private Sds(byte[] buf, int len) {
        this.buf = buf;
        this.len = len;
        this.alloc = buf.length;
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

    // ---- Core operations ----

    /**
     * Append bytes to this Sds, returning a new Sds.
     * Growth strategy: if current len < 1MB, new alloc = (len+addLen)*2;
     * otherwise new alloc = len + addLen + 1MB.
     */
    public Sds append(byte[] addition) {
        if (addition == null || addition.length == 0) return this;
        int newLen = this.len + addition.length;
        int newAlloc;
        if (newLen < SDS_MAX_PREALLOC) {
            newAlloc = newLen * 2;
        } else {
            newAlloc = newLen + SDS_MAX_PREALLOC;
        }
        byte[] newBuf = new byte[newAlloc];
        System.arraycopy(this.buf, 0, newBuf, 0, this.len);
        System.arraycopy(addition, 0, newBuf, this.len, addition.length);
        return new Sds(newBuf, newLen);
    }

    /**
     * Append a String to this Sds, returning a new Sds.
     */
    public Sds appendStr(String s) {
        if (s == null || s.isEmpty()) return this;
        return append(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Extend this Sds to {@code newLen} bytes, filling extra bytes with zero.
     * If {@code newLen <= len}, returns this unchanged.
     */
    public Sds sdsgrowzero(int newLen) {
        if (newLen <= this.len) return this;
        byte[] newBuf = Arrays.copyOf(this.buf, newLen);
        // Arrays.copyOf already zero-fills the new portion
        return new Sds(newBuf, newLen);
    }

    /**
     * Return a sub-range [start, end] (inclusive, supports negative indices).
     * Follows Redis sdsrange semantics.
     */
    public Sds sdsrange(int start, int end) {
        if (len == 0) return empty();
        // Normalize negative indices
        if (start < 0) start = Math.max(len + start, 0);
        if (end < 0)   end   = len + end;
        // Clamp
        if (start > end || start >= len) return empty();
        end = Math.min(end, len - 1);
        int rangeLen = end - start + 1;
        byte[] newBuf = new byte[rangeLen];
        System.arraycopy(buf, start, newBuf, 0, rangeLen);
        return new Sds(newBuf, rangeLen);
    }

    // ---- Accessors ----

    public String toStr() {
        return new String(buf, 0, len, StandardCharsets.UTF_8);
    }

    public byte[] toBytes() {
        return Arrays.copyOf(buf, len);
    }

    public boolean isEmpty() {
        return len == 0;
    }

    /** Returns the number of used bytes (same as {@code getLen()}). */
    public int length() {
        return len;
    }

    // ---- Comparison ----

    /**
     * Lexicographic byte comparison (like Redis sdscmp).
     */
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
        for (int i = 0; i < len; i++) {
            result = 31 * result + buf[i];
        }
        return result;
    }

    @Override
    public String toString() {
        return "Sds{len=" + len + ", alloc=" + alloc + ", content=\"" + toStr() + "\"}";
    }
}
