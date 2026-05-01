package com.redisimpl.test.integration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Hash commands.
 */
class HashCommandsIntegrationTest extends BaseIntegrationTest {

    @Test
    void hset_and_hget() {
        jedis.hset("hash", "field", "value");
        assertEquals("value", jedis.hget("hash", "field"));
    }

    @Test
    void hget_nonExistentField() {
        jedis.hset("hash", "field", "value");
        assertNull(jedis.hget("hash", "nonexistent"));
    }

    @Test
    void hmset_and_hmget() {
        Map<String, String> fields = new HashMap<>();
        fields.put("f1", "v1");
        fields.put("f2", "v2");
        fields.put("f3", "v3");
        jedis.hmset("hash", fields);
        List<String> values = jedis.hmget("hash", "f1", "f2", "f3", "f4");
        assertEquals("v1", values.get(0));
        assertEquals("v2", values.get(1));
        assertEquals("v3", values.get(2));
        assertNull(values.get(3));
    }

    @Test
    void hdel() {
        jedis.hset("hash", "f1", "v1");
        jedis.hset("hash", "f2", "v2");
        assertEquals(1L, jedis.hdel("hash", "f1"));
        assertNull(jedis.hget("hash", "f1"));
    }

    @Test
    void hexists() {
        jedis.hset("hash", "field", "value");
        assertTrue(jedis.hexists("hash", "field"));
        assertFalse(jedis.hexists("hash", "nonexistent"));
    }

    @Test
    void hlen() {
        jedis.hset("hash", "f1", "v1");
        jedis.hset("hash", "f2", "v2");
        jedis.hset("hash", "f3", "v3");
        assertEquals(3L, jedis.hlen("hash"));
    }

    @Test
    void hkeys() {
        jedis.hset("hash", "f1", "v1");
        jedis.hset("hash", "f2", "v2");
        Set<String> keys = jedis.hkeys("hash");
        assertEquals(2, keys.size());
        assertTrue(keys.contains("f1"));
        assertTrue(keys.contains("f2"));
    }

    @Test
    void hvals() {
        jedis.hset("hash", "f1", "v1");
        jedis.hset("hash", "f2", "v2");
        List<String> vals = jedis.hvals("hash");
        assertEquals(2, vals.size());
        assertTrue(vals.contains("v1"));
        assertTrue(vals.contains("v2"));
    }

    @Test
    void hgetall() {
        jedis.hset("hash", "f1", "v1");
        jedis.hset("hash", "f2", "v2");
        Map<String, String> all = jedis.hgetAll("hash");
        assertEquals(2, all.size());
        assertEquals("v1", all.get("f1"));
        assertEquals("v2", all.get("f2"));
    }

    @Test
    void hincrby() {
        jedis.hset("hash", "count", "10");
        assertEquals(15L, jedis.hincrBy("hash", "count", 5));
    }

    @Test
    void hincrbyfloat() {
        jedis.hset("hash", "float", "10.5");
        double result = jedis.hincrByFloat("hash", "float", 1.5);
        assertEquals(12.0, result, 0.001);
    }

    @Test
    void hsetnx() {
        assertEquals(1L, jedis.hsetnx("hash", "field", "value"));
        assertEquals(0L, jedis.hsetnx("hash", "field", "other"));
        assertEquals("value", jedis.hget("hash", "field"));
    }
}
