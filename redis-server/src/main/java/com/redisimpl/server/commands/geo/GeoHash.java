package com.redisimpl.server.commands.geo;

/**
 * Geohash encoding/decoding utilities for Redis Geo commands.
 * Redis uses a 52-bit interleaved geohash as the ZSet score.
 */
public final class GeoHash {

    /** Earth radius in meters (WGS-84) */
    public static final double EARTH_RADIUS_METERS = 6372797.560856;

    private static final int GEOHASH_STEP = 26; // 52-bit total (26 bits per coordinate)
    private static final double LAT_MIN = -90.0;
    private static final double LAT_MAX = 90.0;
    private static final double LON_MIN = -180.0;
    private static final double LON_MAX = 180.0;

    private GeoHash() {}

    /**
     * Encode (longitude, latitude) to a 52-bit geohash score suitable for ZSet storage.
     */
    public static double encode(double lon, double lat) {
        long lonBits = encodeBits(lon, LON_MIN, LON_MAX, GEOHASH_STEP);
        long latBits = encodeBits(lat, LAT_MIN, LAT_MAX, GEOHASH_STEP);
        long hash = interleave(lonBits, latBits);
        // Store as a positive double (bits fit in 52-bit mantissa)
        return (double) hash;
    }

    /**
     * Decode a ZSet score back to [longitude, latitude].
     */
    public static double[] decode(double score) {
        long hash = (long) score;
        long lonBits = deinterleaveEven(hash);
        long latBits = deinterleaveOdd(hash);
        double lon = decodeBits(lonBits, LON_MIN, LON_MAX, GEOHASH_STEP);
        double lat = decodeBits(latBits, LAT_MIN, LAT_MAX, GEOHASH_STEP);
        return new double[]{lon, lat};
    }

    /** Encode a coordinate value to an integer index. */
    private static long encodeBits(double value, double min, double max, int steps) {
        double range = max - min;
        double normalized = (value - min) / range;
        return (long) (normalized * (1L << steps));
    }

    /** Decode an integer index back to a coordinate value. */
    private static double decodeBits(long bits, double min, double max, int steps) {
        double range = max - min;
        double normalized = (double) bits / (1L << steps);
        return min + normalized * range;
    }

    /** Interleave bits: even positions = lon, odd positions = lat. */
    private static long interleave(long x, long y) {
        long result = 0;
        for (int i = 0; i < 26; i++) {
            result |= ((x >> i) & 1L) << (2 * i);
            result |= ((y >> i) & 1L) << (2 * i + 1);
        }
        return result;
    }

    /** Deinterleave even bits (longitude). */
    private static long deinterleaveEven(long hash) {
        long result = 0;
        for (int i = 0; i < 26; i++) {
            result |= ((hash >> (2 * i)) & 1L) << i;
        }
        return result;
    }

    /** Deinterleave odd bits (latitude). */
    private static long deinterleaveOdd(long hash) {
        long result = 0;
        for (int i = 0; i < 26; i++) {
            result |= ((hash >> (2 * i + 1)) & 1L) << i;
        }
        return result;
    }

    /**
     * Calculate distance between two points in meters (Haversine formula).
     */
    public static double distance(double lon1, double lat1, double lon2, double lat2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_METERS * c;
    }

    /** Convert distance to the given unit. */
    public static double toUnit(double meters, String unit) {
        switch (unit.toLowerCase()) {
            case "m":  return meters;
            case "km": return meters / 1000.0;
            case "mi": return meters / 1609.344;
            case "ft": return meters / 0.3048;
            default:   return meters;
        }
    }

    /** Convert distance from the given unit to meters. */
    public static double toMeters(double value, String unit) {
        switch (unit.toLowerCase()) {
            case "m":  return value;
            case "km": return value * 1000.0;
            case "mi": return value * 1609.344;
            case "ft": return value * 0.3048;
            default:   return value;
        }
    }

    /** Base32 alphabet used by Redis for geohash strings. */
    private static final char[] BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

    /**
     * Encode to an 11-character geohash string (like Redis GEOHASH command).
     */
    public static String encodeString(double lon, double lat) {
        long hash = (long) encode(lon, lat);
        // Pad to 52 bits, encode as 11 base-32 chars (5 bits each = 55 bits, use 52)
        StringBuilder sb = new StringBuilder(11);
        for (int i = 10; i >= 0; i--) {
            int idx = (int) ((hash >> (i * 5)) & 0x1F);
            sb.append(BASE32[idx]);
        }
        return sb.reverse().toString();
    }
}
