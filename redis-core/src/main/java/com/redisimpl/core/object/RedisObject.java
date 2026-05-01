package com.redisimpl.core.object;

import lombok.Getter;
import lombok.Setter;

/**
 * Unified Redis object header.
 * Mirrors Redis's robj (redisObject) structure in server.h.
 *
 * <pre>
 * typedef struct redisObject {
 *     unsigned type:4;
 *     unsigned encoding:4;
 *     unsigned lru:LRU_BITS;
 *     int refcount;
 *     void *ptr;
 * } robj;
 * </pre>
 */
@Getter
@Setter
public class RedisObject {

    /** Object type: OBJ_TYPE_STRING, LIST, SET, ZSET, HASH */
    private int type;

    /** Object encoding: OBJ_ENCODING_RAW, INT, HT, etc. */
    private int encoding;

    /**
     * LRU time (relative to server.lruclock) or LFU data.
     * 24-bit value, max = LRU_CLOCK_MAX.
     */
    private int lruClock;

    /** Reference count. Objects are freed when refcount reaches 0. */
    private volatile int refcount;

    /**
     * Pointer to the actual data structure.
     * Type depends on encoding:
     * - INT: Long
     * - RAW/EMBSTR: Sds or String
     * - HT: Dict
     * - QUICKLIST: QuickList
     * - LISTPACK: ListPack
     * - INTSET: IntSet
     * - SKIPLIST: ZSetData (ZSkipList + Dict)
     */
    private Object ptr;

    private RedisObject() {}

    /**
     * Create a new RedisObject with the given type, encoding, and pointer.
     */
    public static RedisObject createObject(int type, int encoding, Object ptr) {
        RedisObject obj = new RedisObject();
        obj.type = type;
        obj.encoding = encoding;
        obj.ptr = ptr;
        obj.refcount = 1;
        obj.lruClock = currentLruClock();
        return obj;
    }

    /**
     * Increment reference count.
     */
    public void incrRefCount() {
        refcount++;
    }

    /**
     * Decrement reference count.
     * Caller should check if refcount reaches 0 and free the object.
     */
    public void decrRefCount() {
        if (refcount <= 0) {
            throw new IllegalStateException("decrRefCount called on object with refcount=" + refcount);
        }
        refcount--;
    }

    /**
     * Compute the current LRU clock value.
     * Uses seconds resolution (LRU_CLOCK_RESOLUTION = 1000ms).
     */
    public static int currentLruClock() {
        long seconds = System.currentTimeMillis() / RedisObjectConstants.LRU_CLOCK_RESOLUTION;
        return (int) (seconds & RedisObjectConstants.LRU_CLOCK_MAX);
    }

    /**
     * Returns a human-readable type name.
     */
    public String typeName() {
        switch (type) {
            case RedisObjectConstants.OBJ_TYPE_STRING: return "string";
            case RedisObjectConstants.OBJ_TYPE_LIST:   return "list";
            case RedisObjectConstants.OBJ_TYPE_SET:    return "set";
            case RedisObjectConstants.OBJ_TYPE_ZSET:   return "zset";
            case RedisObjectConstants.OBJ_TYPE_HASH:   return "hash";
            default: return "unknown";
        }
    }

    /**
     * Returns a human-readable encoding name.
     */
    public String encodingName() {
        switch (encoding) {
            case RedisObjectConstants.OBJ_ENCODING_RAW:       return "raw";
            case RedisObjectConstants.OBJ_ENCODING_INT:       return "int";
            case RedisObjectConstants.OBJ_ENCODING_HT:        return "hashtable";
            case RedisObjectConstants.OBJ_ENCODING_ZIPLIST:   return "ziplist";
            case RedisObjectConstants.OBJ_ENCODING_INTSET:    return "intset";
            case RedisObjectConstants.OBJ_ENCODING_SKIPLIST:  return "skiplist";
            case RedisObjectConstants.OBJ_ENCODING_EMBSTR:    return "embstr";
            case RedisObjectConstants.OBJ_ENCODING_QUICKLIST: return "quicklist";
            case RedisObjectConstants.OBJ_ENCODING_LISTPACK:  return "listpack";
            default: return "unknown";
        }
    }

    @Override
    public String toString() {
        return "RedisObject{type=" + typeName() + ", encoding=" + encodingName()
                + ", refcount=" + refcount + ", ptr=" + ptr + "}";
    }
}
