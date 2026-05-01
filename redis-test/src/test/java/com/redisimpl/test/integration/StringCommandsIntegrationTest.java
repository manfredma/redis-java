package com.redisimpl.test.integration;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for String commands using Jedis.
 */
class StringCommandsIntegrationTest extends BaseIntegrationTest {

    @Test
    void ping() {
        assertEquals("PONG", jedis.ping());
    }

    @Test
    void set_and_get() {
        jedis.set("key", "value");
        assertEquals("value", jedis.get("key"));
    }

    @Test
    void get_nonExistent_returnsNull() {
        assertNull(jedis.get("nonexistent"));
    }

    @Test
    void set_withEX() throws InterruptedException {
        jedis.set("key", "value", redis.clients.jedis.params.SetParams.setParams().ex(1));
        assertEquals("value", jedis.get("key"));
        Thread.sleep(1100);
        assertNull(jedis.get("key"));
    }

    @Test
    void set_withNX_keyExists_returnsNull() {
        jedis.set("key", "original");
        String result = jedis.set("key", "new", redis.clients.jedis.params.SetParams.setParams().nx());
        assertNull(result);
        assertEquals("original", jedis.get("key"));
    }

    @Test
    void set_withNX_keyNotExists_setsValue() {
        String result = jedis.set("key", "value", redis.clients.jedis.params.SetParams.setParams().nx());
        assertEquals("OK", result);
        assertEquals("value", jedis.get("key"));
    }

    @Test
    void mset_and_mget() {
        jedis.mset("k1", "v1", "k2", "v2", "k3", "v3");
        List<String> values = jedis.mget("k1", "k2", "k3", "k4");
        assertEquals(4, values.size());
        assertEquals("v1", values.get(0));
        assertEquals("v2", values.get(1));
        assertEquals("v3", values.get(2));
        assertNull(values.get(3));
    }

    @Test
    void incr() {
        assertEquals(1L, jedis.incr("counter"));
        assertEquals(2L, jedis.incr("counter"));
        assertEquals(3L, jedis.incr("counter"));
    }

    @Test
    void decr() {
        jedis.set("counter", "10");
        assertEquals(9L, jedis.decr("counter"));
    }

    @Test
    void incrby() {
        jedis.set("counter", "10");
        assertEquals(15L, jedis.incrBy("counter", 5));
    }

    @Test
    void decrby() {
        jedis.set("counter", "10");
        assertEquals(7L, jedis.decrBy("counter", 3));
    }

    @Test
    void incrbyfloat() {
        jedis.set("float", "10.5");
        double result = jedis.incrByFloat("float", 1.5);
        assertEquals(12.0, result, 0.001);
    }

    @Test
    void append() {
        jedis.set("key", "Hello");
        jedis.append("key", " World");
        assertEquals("Hello World", jedis.get("key"));
    }

    @Test
    void strlen() {
        jedis.set("key", "Hello");
        assertEquals(5L, jedis.strlen("key"));
    }

    @Test
    void getrange() {
        jedis.set("key", "Hello World");
        assertEquals("Hello", jedis.getrange("key", 0, 4));
        assertEquals("World", jedis.getrange("key", 6, 10));
        assertEquals("World", jedis.getrange("key", -5, -1));
    }

    @Test
    void setrange() {
        jedis.set("key", "Hello World");
        jedis.setrange("key", 6, "Redis");
        assertEquals("Hello Redis", jedis.get("key"));
    }

    @Test
    void setnx() {
        assertEquals(1L, jedis.setnx("key", "value"));
        assertEquals(0L, jedis.setnx("key", "other"));
        assertEquals("value", jedis.get("key"));
    }

    @Test
    void getset() {
        jedis.set("key", "old");
        assertEquals("old", jedis.getSet("key", "new"));
        assertEquals("new", jedis.get("key"));
    }

    @Test
    void getdel() {
        jedis.set("key", "value");
        assertEquals("value", jedis.getDel("key"));
        assertNull(jedis.get("key"));
    }

    @Test
    void setex() {
        jedis.setex("key", 100L, "value");
        assertEquals("value", jedis.get("key"));
        assertTrue(jedis.ttl("key") > 0);
    }

    @Test
    void psetex() {
        jedis.psetex("key", 100000L, "value");
        assertEquals("value", jedis.get("key"));
        assertTrue(jedis.pttl("key") > 0);
    }

    @Test
    void wrongType_error() {
        jedis.lpush("list", "a", "b");
        try {
            jedis.get("list");
            fail("Expected WRONGTYPE error");
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            assertTrue(e.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    void incr_notInteger_error() {
        jedis.set("key", "notanumber");
        try {
            jedis.incr("key");
            fail("Expected error");
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            assertTrue(e.getMessage().contains("not an integer"));
        }
    }
}
