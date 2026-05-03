package com.redisimpl.core.object;

/**
 * Redis object type and encoding constants.
 * Mirrors Redis's object.h definitions.
 */
public final class RedisObjectConstants {

    private RedisObjectConstants() {}

    // ---- Object types ----
    /** String object */
    public static final int OBJ_TYPE_STRING = 0;
    /** List object */
    public static final int OBJ_TYPE_LIST   = 1;
    /** Set object */
    public static final int OBJ_TYPE_SET    = 2;
    /** Sorted set object */
    public static final int OBJ_TYPE_ZSET   = 3;
    /** Hash object */
    public static final int OBJ_TYPE_HASH   = 4;
    /** Stream object (Redis 5+) */
    public static final int OBJ_TYPE_STREAM = 6;

    // ---- Object encodings ----
    /** Raw SDS string */
    public static final int OBJ_ENCODING_RAW       = 0;
    /** Long integer */
    public static final int OBJ_ENCODING_INT        = 1;
    /** Hash table (Dict) */
    public static final int OBJ_ENCODING_HT         = 2;
    /** Ziplist (legacy, kept for compat) */
    public static final int OBJ_ENCODING_ZIPLIST    = 3;
    /** Integer set */
    public static final int OBJ_ENCODING_INTSET     = 4;
    /** Skip list + dict */
    public static final int OBJ_ENCODING_SKIPLIST   = 5;
    /** Embedded SDS string (<=44 bytes) */
    public static final int OBJ_ENCODING_EMBSTR     = 8;
    /** Quick list */
    public static final int OBJ_ENCODING_QUICKLIST  = 9;
    /** List pack (compact list) */
    public static final int OBJ_ENCODING_LISTPACK   = 11;
    /** Stream encoding */
    public static final int OBJ_ENCODING_STREAM     = 14;

    // ---- LRU clock ----
    /** LRU clock resolution in milliseconds */
    public static final int LRU_CLOCK_RESOLUTION = 1000;
    /** LRU clock max value (24-bit) */
    public static final int LRU_CLOCK_MAX = 0xFFFFFF;

    // ---- Shared integers ----
    /** Shared integer pool range: 0 to OBJ_SHARED_INTEGERS-1 */
    public static final int OBJ_SHARED_INTEGERS = 10000;

    // ---- String encoding thresholds ----
    /** Max length for EMBSTR encoding */
    public static final int OBJ_ENCODING_EMBSTR_SIZE_LIMIT = 44;

    // ---- List encoding thresholds ----
    public static final int OBJ_LIST_MAX_LISTPACK_ENTRIES = 128;
    public static final int OBJ_LIST_MAX_LISTPACK_VALUE   = 64;

    // ---- Hash encoding thresholds ----
    public static final int OBJ_HASH_MAX_LISTPACK_ENTRIES = 128;
    public static final int OBJ_HASH_MAX_LISTPACK_VALUE   = 64;

    // ---- Set encoding thresholds ----
    public static final int OBJ_SET_MAX_INTSET_ENTRIES    = 512;
    public static final int OBJ_SET_MAX_LISTPACK_ENTRIES  = 128;
    public static final int OBJ_SET_MAX_LISTPACK_VALUE    = 64;

    // ---- ZSet encoding thresholds ----
    public static final int OBJ_ZSET_MAX_LISTPACK_ENTRIES = 128;
    public static final int OBJ_ZSET_MAX_LISTPACK_VALUE   = 64;
}
