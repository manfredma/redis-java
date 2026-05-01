package com.redisimpl.persistence;

/**
 * CRC-64/Jones implementation matching Redis's crc64.c.
 *
 * <p>Uses the Jones reflected polynomial: {@code 0xad93d23594c935a9}.
 * This is the same CRC-64 used by Redis to checksum RDB files.
 *
 * <p>Algorithm: CRC-64/Jones
 * <ul>
 *   <li>Width: 64</li>
 *   <li>Poly: 0xad93d23594c935a9 (reflected)</li>
 *   <li>Init: 0x0000000000000000</li>
 *   <li>RefIn: true</li>
 *   <li>RefOut: true</li>
 *   <li>XorOut: 0x0000000000000000</li>
 * </ul>
 */
public final class Crc64 {

    private Crc64() {}

    /** Reflected polynomial used by Redis. */
    private static final long POLY = 0xad93d23594c935a9L;

    /** Precomputed lookup table (256 entries). */
    private static final long[] TABLE = buildTable();

    private static long[] buildTable() {
        long[] table = new long[256];
        for (int i = 0; i < 256; i++) {
            long crc = i;
            for (int j = 0; j < 8; j++) {
                if ((crc & 1L) != 0) {
                    crc = (crc >>> 1) ^ POLY;
                } else {
                    crc >>>= 1;
                }
            }
            table[i] = crc;
        }
        return table;
    }

    /**
     * Compute CRC-64 over the given byte array.
     *
     * @param data input bytes
     * @return 64-bit CRC value
     */
    public static long digest(byte[] data) {
        return digest(0L, data, 0, data.length);
    }

    /**
     * Compute CRC-64 over a slice of a byte array (starting CRC = 0).
     *
     * @param data input bytes
     * @param off  offset into data
     * @param len  number of bytes to process
     * @return 64-bit CRC value
     */
    public static long digest(byte[] data, int off, int len) {
        return digest(0L, data, off, len);
    }

    /**
     * Update an existing CRC-64 with additional bytes.
     *
     * @param crc  current CRC value (start with 0)
     * @param data input bytes
     * @param off  offset into data
     * @param len  number of bytes to process
     * @return updated CRC value
     */
    public static long digest(long crc, byte[] data, int off, int len) {
        for (int i = off; i < off + len; i++) {
            int idx = (int) ((crc ^ (data[i] & 0xFFL)) & 0xFF);
            crc = TABLE[idx] ^ (crc >>> 8);
        }
        return crc;
    }

    /**
     * Write a 64-bit CRC value as 8 little-endian bytes into {@code out}
     * starting at offset {@code off}.
     */
    public static void writeLE64(long value, byte[] out, int off) {
        for (int i = 0; i < 8; i++) {
            out[off + i] = (byte) (value & 0xFF);
            value >>>= 8;
        }
    }

    /**
     * Read a 64-bit little-endian value from {@code data} at offset {@code off}.
     */
    public static long readLE64(byte[] data, int off) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (data[off + i] & 0xFFL);
        }
        return v;
    }
}
