package com.redisimpl.test.integration;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Set commands.
 */
class SetCommandsIntegrationTest extends BaseIntegrationTest {

    @Test
    void sadd_and_smembers() {
        jedis.sadd("set", "a", "b", "c");
        Set<String> members = jedis.smembers("set");
        assertEquals(3, members.size());
        assertTrue(members.contains("a"));
        assertTrue(members.contains("b"));
        assertTrue(members.contains("c"));
    }

    @Test
    void sadd_duplicate_notAdded() {
        jedis.sadd("set", "a");
        assertEquals(0L, jedis.sadd("set", "a"));
        assertEquals(1L, jedis.scard("set"));
    }

    @Test
    void srem() {
        jedis.sadd("set", "a", "b", "c");
        assertEquals(1L, jedis.srem("set", "b"));
        assertFalse(jedis.smembers("set").contains("b"));
    }

    @Test
    void sismember() {
        jedis.sadd("set", "a", "b");
        assertTrue(jedis.sismember("set", "a"));
        assertFalse(jedis.sismember("set", "x"));
    }

    @Test
    void scard() {
        jedis.sadd("set", "a", "b", "c");
        assertEquals(3L, jedis.scard("set"));
    }

    @Test
    void sinter() {
        jedis.sadd("s1", "a", "b", "c");
        jedis.sadd("s2", "b", "c", "d");
        Set<String> result = jedis.sinter("s1", "s2");
        assertEquals(2, result.size());
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

    @Test
    void sunion() {
        jedis.sadd("s1", "a", "b");
        jedis.sadd("s2", "b", "c");
        Set<String> result = jedis.sunion("s1", "s2");
        assertEquals(3, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

    @Test
    void sdiff() {
        jedis.sadd("s1", "a", "b", "c");
        jedis.sadd("s2", "b", "c");
        Set<String> result = jedis.sdiff("s1", "s2");
        assertEquals(1, result.size());
        assertTrue(result.contains("a"));
    }

    @Test
    void sinterstore() {
        jedis.sadd("s1", "a", "b", "c");
        jedis.sadd("s2", "b", "c", "d");
        long count = jedis.sinterstore("dst", "s1", "s2");
        assertEquals(2L, count);
        Set<String> dst = jedis.smembers("dst");
        assertTrue(dst.contains("b"));
        assertTrue(dst.contains("c"));
    }

    @Test
    void sunionstore() {
        jedis.sadd("s1", "a", "b");
        jedis.sadd("s2", "b", "c");
        long count = jedis.sunionstore("dst", "s1", "s2");
        assertEquals(3L, count);
    }

    @Test
    void sdiffstore() {
        jedis.sadd("s1", "a", "b", "c");
        jedis.sadd("s2", "b", "c");
        long count = jedis.sdiffstore("dst", "s1", "s2");
        assertEquals(1L, count);
    }

    @Test
    void smove() {
        jedis.sadd("s1", "a", "b");
        jedis.sadd("s2", "c");
        assertEquals(1L, jedis.smove("s1", "s2", "a"));
        assertFalse(jedis.sismember("s1", "a"));
        assertTrue(jedis.sismember("s2", "a"));
    }

    @Test
    void spop() {
        jedis.sadd("set", "a", "b", "c");
        String popped = jedis.spop("set");
        assertNotNull(popped);
        assertEquals(2L, jedis.scard("set"));
    }

    @Test
    void srandmember() {
        jedis.sadd("set", "a", "b", "c");
        String member = jedis.srandmember("set");
        assertNotNull(member);
        assertEquals(3L, jedis.scard("set")); // not removed
    }

    @Test
    void intset_encoding() {
        jedis.sadd("intset", "1", "2", "3", "4", "5");
        assertEquals("intset", jedis.objectEncoding("intset"));
    }
}
