package com.redisimpl.test.integration;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for List commands.
 */
class ListCommandsIntegrationTest extends BaseIntegrationTest {

    @Test
    void rpush_and_lrange() {
        jedis.rpush("list", "a", "b", "c");
        List<String> result = jedis.lrange("list", 0, -1);
        assertEquals(3, result.size());
        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
    }

    @Test
    void lpush_and_lrange() {
        jedis.lpush("list", "c", "b", "a");
        List<String> result = jedis.lrange("list", 0, -1);
        assertEquals(3, result.size());
        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
    }

    @Test
    void llen() {
        jedis.rpush("list", "a", "b", "c");
        assertEquals(3L, jedis.llen("list"));
    }

    @Test
    void lpop() {
        jedis.rpush("list", "a", "b", "c");
        assertEquals("a", jedis.lpop("list"));
        assertEquals(2L, jedis.llen("list"));
    }

    @Test
    void rpop() {
        jedis.rpush("list", "a", "b", "c");
        assertEquals("c", jedis.rpop("list"));
        assertEquals(2L, jedis.llen("list"));
    }

    @Test
    void lindex() {
        jedis.rpush("list", "a", "b", "c");
        assertEquals("a", jedis.lindex("list", 0));
        assertEquals("b", jedis.lindex("list", 1));
        assertEquals("c", jedis.lindex("list", 2));
        assertEquals("c", jedis.lindex("list", -1));
        assertNull(jedis.lindex("list", 10));
    }

    @Test
    void lset() {
        jedis.rpush("list", "a", "b", "c");
        jedis.lset("list", 1, "B");
        assertEquals("B", jedis.lindex("list", 1));
    }

    @Test
    void linsert_before() {
        jedis.rpush("list", "a", "c");
        jedis.linsert("list", redis.clients.jedis.args.ListPosition.BEFORE, "c", "b");
        List<String> result = jedis.lrange("list", 0, -1);
        assertEquals("b", result.get(1));
    }

    @Test
    void lrem() {
        jedis.rpush("list", "a", "b", "a", "a", "c");
        assertEquals(2L, jedis.lrem("list", 2, "a"));
        assertEquals(3L, jedis.llen("list"));
    }

    @Test
    void lrange_negativeIndices() {
        jedis.rpush("list", "a", "b", "c", "d", "e");
        List<String> result = jedis.lrange("list", -3, -1);
        assertEquals(3, result.size());
        assertEquals("c", result.get(0));
        assertEquals("d", result.get(1));
        assertEquals("e", result.get(2));
    }

    @Test
    void lmove() {
        jedis.rpush("src", "a", "b", "c");
        jedis.rpush("dst", "x");
        String moved = jedis.lmove("src", "dst",
                redis.clients.jedis.args.ListDirection.LEFT,
                redis.clients.jedis.args.ListDirection.RIGHT);
        assertEquals("a", moved);
        assertEquals(2L, jedis.llen("src"));
        assertEquals(2L, jedis.llen("dst"));
        assertEquals("a", jedis.lindex("dst", 1));
    }
}
