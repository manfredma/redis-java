package com.redisimpl.cluster;

/**
 * CRC16 implementation for Redis Cluster key slot calculation.
 * Uses the CRC-16-CCITT polynomial (0x1021), as specified in Redis Cluster spec.
 */
public final class CRC16 {

    private static final int[] TABLE = buildTable();

    private static int[] buildTable() {
        int[] t = new int[256];
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
            }
            t[i] = crc & 0xFFFF;
        }
        return t;
    }

    public static int crc16(byte[] data) {
        return crc16(data, 0, data.length);
    }

    public static int crc16(byte[] data, int offset, int length) {
        int crc = 0;
        for (int i = offset; i < offset + length; i++) {
            crc = ((crc << 8) ^ TABLE[((crc >> 8) ^ (data[i] & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }

    /**
     * Calculate the hash slot for a key.
     * If the key contains "{...}", only the content inside braces is hashed (hash tags).
     */
    public static int keyHashSlot(byte[] key) {
        int s = -1, e = -1;
        for (int i = 0; i < key.length; i++) {
            if (key[i] == '{' && s == -1) s = i;
            else if (key[i] == '}' && s != -1) { e = i; break; }
        }
        if (s != -1 && e != -1 && e > s + 1) {
            return crc16(key, s + 1, e - s - 1) % 16384;
        }
        return crc16(key) % 16384;
    }
}
