package com.redisimpl.test.integration;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for generic commands.
 */
class GenericCommandsIntegrationTest extends BaseIntegrationTest {

    @Test
    void del_singleKey() {
        jedis.set("key", "value");
        assertEquals(1L, jedis.del("key"));
        assertNull(jedis.get("key"));
    }

    @Test
    void del_multipleKeys() {
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        jedis.set("k3", "v3");
        assertEquals(3L, jedis.del("k1", "k2", "k3"));
    }

    @Test
    void del_nonExistentKey() {
        assertEquals(0L, jedis.del("nonexistent"));
    }

    @Test
    void exists_key() {
        jedis.set("key", "value");
        assertEquals(1L, jedis.exists("key"));
    }

    @Test
    void exists_nonExistent() {
        assertEquals(0L, jedis.exists("nonexistent"));
    }

    @Test
    void type_string() {
        jedis.set("key", "value");
        assertEquals("string", jedis.type("key"));
    }

    @Test
    void type_list() {
        jedis.lpush("list", "a");
        assertEquals("list", jedis.type("list"));
    }

    @Test
    void type_hash() {
        jedis.hset("hash", "field", "value");
        assertEquals("hash", jedis.type("hash"));
    }

    @Test
    void type_set() {
        jedis.sadd("set", "member");
        assertEquals("set", jedis.type("set"));
    }

    @Test
    void type_zset() {
        jedis.zadd("zset", 1.0, "member");
        assertEquals("zset", jedis.type("zset"));
    }

    @Test
    void type_nonExistent() {
        assertEquals("none", jedis.type("nonexistent"));
    }

    @Test
    void keys_allKeys() {
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        jedis.set("k3", "v3");
        Set<String> keys = jedis.keys("*");
        assertEquals(3, keys.size());
        assertTrue(keys.contains("k1"));
        assertTrue(keys.contains("k2"));
        assertTrue(keys.contains("k3"));
    }

    @Test
    void keys_withPattern() {
        jedis.set("user:1", "a");
        jedis.set("user:2", "b");
        jedis.set("item:1", "c");
        Set<String> keys = jedis.keys("user:*");
        assertEquals(2, keys.size());
    }

    @Test
    void expire_and_ttl() throws InterruptedException {
        jedis.set("key", "value");
        jedis.expire("key", 10L);
        long ttl = jedis.ttl("key");
        assertTrue(ttl > 0 && ttl <= 10);
    }

    @Test
    void expire_keyExpires() throws InterruptedException {
        jedis.set("key", "value");
        jedis.expire("key", 1L);
        Thread.sleep(1100);
        assertNull(jedis.get("key"));
    }

    @Test
    void pexpire_and_pttl() {
        jedis.set("key", "value");
        jedis.pexpire("key", 10000L);
        long pttl = jedis.pttl("key");
        assertTrue(pttl > 0 && pttl <= 10000);
    }

    @Test
    void persist_removesExpiry() {
        jedis.set("key", "value");
        jedis.expire("key", 100L);
        jedis.persist("key");
        assertEquals(-1L, jedis.ttl("key"));
    }

    @Test
    void ttl_noExpiry() {
        jedis.set("key", "value");
        assertEquals(-1L, jedis.ttl("key"));
    }

    @Test
    void ttl_nonExistentKey() {
        assertEquals(-2L, jedis.ttl("nonexistent"));
    }

    @Test
    void rename() {
        jedis.set("src", "value");
        jedis.rename("src", "dst");
        assertNull(jedis.get("src"));
        assertEquals("value", jedis.get("dst"));
    }

    @Test
    void renamenx_success() {
        jedis.set("src", "value");
        assertEquals(1L, jedis.renamenx("src", "dst"));
        assertEquals("value", jedis.get("dst"));
    }

    @Test
    void renamenx_dstExists() {
        jedis.set("src", "value");
        jedis.set("dst", "existing");
        assertEquals(0L, jedis.renamenx("src", "dst"));
        assertEquals("value", jedis.get("src"));
    }

    @Test
    void select_and_dbsize() {
        jedis.select(0);
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        assertEquals(2L, jedis.dbSize());
        jedis.select(1);
        assertEquals(0L, jedis.dbSize());
    }

    @Test
    void flushdb() {
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        jedis.flushDB();
        assertEquals(0L, jedis.dbSize());
    }

    @Test
    void dbsize() {
        assertEquals(0L, jedis.dbSize());
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        assertEquals(2L, jedis.dbSize());
    }

    @Test
    void objectEncoding_string_int() {
        jedis.set("key", "12345");
        assertEquals("int", jedis.objectEncoding("key"));
    }

    @Test
    void objectEncoding_string_embstr() {
        jedis.set("key", "hello");
        assertEquals("embstr", jedis.objectEncoding("key"));
    }

    @Test
    void unlink() {
        jedis.set("key", "value");
        assertEquals(1L, jedis.unlink("key"));
        assertNull(jedis.get("key"));
    }
}
