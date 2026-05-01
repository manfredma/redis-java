package com.redisimpl.server.commands.hyperloglog;

/**
 * HyperLogLog implementation with 16384 buckets (2^14).
 * Uses the MurmurHash64A hash function.
 * Precision: ~0.81% standard error.
 *
 * Storage: a byte[] of 16384 bytes, one register per byte (6-bit max value = 63).
 * A special 4-byte header "HYLL" is prepended for identification.
 */
public final class HyperLogLog {

    public static final int HLL_REGISTERS = 16384;       // 2^14
    public static final int HLL_BITS = 14;
    public static final double HLL_ALPHA = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS);
    public static final byte[] HLL_MAGIC = {'H', 'Y', 'L', 'L'};

    /** Create a new empty HLL register array (16384 bytes, all zeros). */
    public static byte[] create() {
        byte[] regs = new byte[HLL_REGISTERS + HLL_MAGIC.length];
        System.arraycopy(HLL_MAGIC, 0, regs, 0, HLL_MAGIC.length);
        return regs;
    }

    /** Check if a byte[] is a valid HLL blob. */
    public static boolean isHll(byte[] data) {
        if (data == null || data.length < HLL_MAGIC.length + HLL_REGISTERS) return false;
        for (int i = 0; i < HLL_MAGIC.length; i++) {
            if (data[i] != HLL_MAGIC[i]) return false;
        }
        return true;
    }

    /**
     * Add an element to the HLL registers.
     * Returns true if any register was updated (i.e. cardinality estimate may have changed).
     */
    public static boolean add(byte[] regs, byte[] element) {
        long hash = murmurHash64A(element, 0xadc83b19L);
        int index = (int) (hash & (HLL_REGISTERS - 1));
        long remaining = hash >>> HLL_BITS;
        int leadingZeros = Long.numberOfLeadingZeros(remaining | (1L << (64 - HLL_BITS))) - HLL_BITS;
        int runLen = leadingZeros + 1;
        if (runLen > 63) runLen = 63;

        int regIdx = HLL_MAGIC.length + index;
        if ((regs[regIdx] & 0xFF) < runLen) {
            regs[regIdx] = (byte) runLen;
            return true;
        }
        return false;
    }

    /**
     * Estimate cardinality from registers.
     */
    public static long count(byte[] regs) {
        double sum = 0.0;
        int zeros = 0;
        for (int i = HLL_MAGIC.length; i < HLL_MAGIC.length + HLL_REGISTERS; i++) {
            int val = regs[i] & 0xFF;
            sum += 1.0 / (1L << val);
            if (val == 0) zeros++;
        }
        double estimate = HLL_ALPHA * HLL_REGISTERS * HLL_REGISTERS / sum;

        // Small range correction
        if (estimate <= 2.5 * HLL_REGISTERS && zeros > 0) {
            estimate = HLL_REGISTERS * Math.log((double) HLL_REGISTERS / zeros);
        }
        // Large range correction
        double limit = (1.0 / 30.0) * (1L << 32);
        if (estimate > limit) {
            estimate = -(1L << 32) * Math.log(1.0 - estimate / (1L << 32));
        }
        return Math.round(estimate);
    }

    /**
     * Merge multiple HLL register arrays into dest.
     * dest must be a valid HLL blob.
     */
    public static void merge(byte[] dest, byte[]... sources) {
        for (byte[] src : sources) {
            if (!isHll(src)) continue;
            for (int i = HLL_MAGIC.length; i < HLL_MAGIC.length + HLL_REGISTERS; i++) {
                if ((src[i] & 0xFF) > (dest[i] & 0xFF)) {
                    dest[i] = src[i];
                }
            }
        }
    }

    // ---- MurmurHash64A ----

    private static long murmurHash64A(byte[] data, long seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        int len = data.length;
        long h = seed ^ (len * m);

        int i = 0;
        while (i + 8 <= len) {
            long k = ((long) data[i] & 0xFF)
                | (((long) data[i + 1] & 0xFF) << 8)
                | (((long) data[i + 2] & 0xFF) << 16)
                | (((long) data[i + 3] & 0xFF) << 24)
                | (((long) data[i + 4] & 0xFF) << 32)
                | (((long) data[i + 5] & 0xFF) << 40)
                | (((long) data[i + 6] & 0xFF) << 48)
                | (((long) data[i + 7] & 0xFF) << 56);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
            i += 8;
        }

        // Tail
        int remaining = len - i;
        switch (remaining) {
            case 7: h ^= ((long) data[i + 6] & 0xFF) << 48;
            case 6: h ^= ((long) data[i + 5] & 0xFF) << 40;
            case 5: h ^= ((long) data[i + 4] & 0xFF) << 32;
            case 4: h ^= ((long) data[i + 3] & 0xFF) << 24;
            case 3: h ^= ((long) data[i + 2] & 0xFF) << 16;
            case 2: h ^= ((long) data[i + 1] & 0xFF) << 8;
            case 1: h ^= ((long) data[i] & 0xFF);
                h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }
}
