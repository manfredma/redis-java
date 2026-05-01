package com.redisimpl.core.object;

import org.junit.jupiter.api.Test;

import static com.redisimpl.core.object.RedisObjectConstants.*;
import static org.junit.jupiter.api.Assertions.*;

class RedisObjectTest {

    @Test
    void typeConstants_areCorrect() {
        assertEquals(0, OBJ_TYPE_STRING);
        assertEquals(1, OBJ_TYPE_LIST);
        assertEquals(2, OBJ_TYPE_SET);
        assertEquals(3, OBJ_TYPE_ZSET);
        assertEquals(4, OBJ_TYPE_HASH);
    }

    @Test
    void encodingConstants_areCorrect() {
        assertEquals(0, OBJ_ENCODING_RAW);
        assertEquals(1, OBJ_ENCODING_INT);
        assertEquals(2, OBJ_ENCODING_HT);
        assertEquals(4, OBJ_ENCODING_INTSET);
        assertEquals(5, OBJ_ENCODING_SKIPLIST);
        assertEquals(8, OBJ_ENCODING_EMBSTR);
        assertEquals(9, OBJ_ENCODING_QUICKLIST);
        assertEquals(11, OBJ_ENCODING_LISTPACK);
    }

    @Test
    void createStringObject_withRawEncoding() {
        RedisObject obj = RedisObject.createObject(OBJ_TYPE_STRING, OBJ_ENCODING_RAW, "hello");
        assertEquals(OBJ_TYPE_STRING, obj.getType());
        assertEquals(OBJ_ENCODING_RAW, obj.getEncoding());
        assertEquals("hello", obj.getPtr());
        assertEquals(1, obj.getRefcount());
    }

    @Test
    void createObject_withIntEncoding() {
        RedisObject obj = RedisObject.createObject(OBJ_TYPE_STRING, OBJ_ENCODING_INT, 42L);
        assertEquals(OBJ_TYPE_STRING, obj.getType());
        assertEquals(OBJ_ENCODING_INT, obj.getEncoding());
        assertEquals(42L, obj.getPtr());
    }

    @Test
    void refcount_incrAndDecr() {
        RedisObject obj = RedisObject.createObject(OBJ_TYPE_STRING, OBJ_ENCODING_RAW, "test");
        assertEquals(1, obj.getRefcount());
        obj.incrRefCount();
        assertEquals(2, obj.getRefcount());
        obj.decrRefCount();
        assertEquals(1, obj.getRefcount());
    }

    @Test
    void lruClock_isSet() {
        RedisObject obj = RedisObject.createObject(OBJ_TYPE_STRING, OBJ_ENCODING_RAW, "x");
        assertTrue(obj.getLruClock() >= 0);
        // LRU clock is 24-bit, max = 2^24 - 1 = 16777215
        assertTrue(obj.getLruClock() <= 0xFFFFFF);
    }

    @Test
    void setEncoding_changesEncoding() {
        RedisObject obj = RedisObject.createObject(OBJ_TYPE_STRING, OBJ_ENCODING_RAW, "hello");
        obj.setEncoding(OBJ_ENCODING_EMBSTR);
        assertEquals(OBJ_ENCODING_EMBSTR, obj.getEncoding());
    }

    @Test
    void setPtr_changesPtr() {
        RedisObject obj = RedisObject.createObject(OBJ_TYPE_STRING, OBJ_ENCODING_RAW, "old");
        obj.setPtr("new");
        assertEquals("new", obj.getPtr());
    }

    @Test
    void lruClockMax_is24bit() {
        assertEquals(0xFFFFFF, RedisObjectConstants.LRU_CLOCK_MAX);
    }

    @Test
    void lruClockResolution_is1000ms() {
        assertEquals(1000, RedisObjectConstants.LRU_CLOCK_RESOLUTION);
    }
}
