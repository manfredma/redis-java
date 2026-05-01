package com.redisimpl.test.integration;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.resps.Tuple;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Sorted Set (ZSet) commands.
 */
class ZSetCommandsIntegrationTest extends BaseIntegrationTest {

    @Test
    void zadd_and_zrange() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        List<String> result = jedis.zrange("zset", 0, -1);
        assertEquals(3, result.size());
        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
    }

    @Test
    void zcard() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        assertEquals(2L, jedis.zcard("zset"));
    }

    @Test
    void zscore() {
        jedis.zadd("zset", 1.5, "a");
        assertEquals(1.5, jedis.zscore("zset", "a"), 0.001);
    }

    @Test
    void zscore_nonExistent() {
        jedis.zadd("zset", 1.0, "a");
        assertNull(jedis.zscore("zset", "nonexistent"));
    }

    @Test
    void zincrby() {
        jedis.zadd("zset", 1.0, "a");
        double result = jedis.zincrby("zset", 2.5, "a");
        assertEquals(3.5, result, 0.001);
    }

    @Test
    void zrank() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        assertEquals(0L, jedis.zrank("zset", "a"));
        assertEquals(1L, jedis.zrank("zset", "b"));
        assertEquals(2L, jedis.zrank("zset", "c"));
    }

    @Test
    void zrevrank() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        assertEquals(2L, jedis.zrevrank("zset", "a"));
        assertEquals(0L, jedis.zrevrank("zset", "c"));
    }

    @Test
    void zrangebyscore() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        jedis.zadd("zset", 4.0, "d");
        List<String> result = jedis.zrangeByScore("zset", 2.0, 3.0);
        assertEquals(2, result.size());
        assertEquals("b", result.get(0));
        assertEquals("c", result.get(1));
    }

    @Test
    void zrevrange() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        List<String> result = jedis.zrevrange("zset", 0, -1);
        assertEquals(3, result.size());
        assertEquals("c", result.get(0));
        assertEquals("a", result.get(2));
    }

    @Test
    void zrem() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        assertEquals(1L, jedis.zrem("zset", "a"));
        assertEquals(1L, jedis.zcard("zset"));
    }

    @Test
    void zremrangebyscore() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        jedis.zadd("zset", 4.0, "d");
        assertEquals(2L, jedis.zremrangeByScore("zset", 2.0, 3.0));
        assertEquals(2L, jedis.zcard("zset"));
    }

    @Test
    void zremrangebyrank() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        assertEquals(2L, jedis.zremrangeByRank("zset", 0, 1));
        assertEquals(1L, jedis.zcard("zset"));
    }

    @Test
    void zcount() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        assertEquals(2L, jedis.zcount("zset", 1.0, 2.0));
    }

    @Test
    void zpopmin() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        List<Tuple> result = jedis.zpopmin("zset", 1);
        assertEquals(1, result.size());
        assertEquals("a", result.get(0).getElement());
        assertEquals(1.0, result.get(0).getScore(), 0.001);
    }

    @Test
    void zpopmax() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        jedis.zadd("zset", 3.0, "c");
        List<Tuple> result = jedis.zpopmax("zset", 1);
        assertEquals(1, result.size());
        assertEquals("c", result.get(0).getElement());
    }

    @Test
    void zunionstore() {
        jedis.zadd("z1", 1.0, "a");
        jedis.zadd("z1", 2.0, "b");
        jedis.zadd("z2", 3.0, "b");
        jedis.zadd("z2", 4.0, "c");
        long count = jedis.zunionstore("dst", "z1", "z2");
        assertEquals(3L, count);
        assertEquals(5.0, jedis.zscore("dst", "b"), 0.001); // scores summed
    }

    @Test
    void zinterstore() {
        jedis.zadd("z1", 1.0, "a");
        jedis.zadd("z1", 2.0, "b");
        jedis.zadd("z2", 3.0, "b");
        jedis.zadd("z2", 4.0, "c");
        long count = jedis.zinterstore("dst", "z1", "z2");
        assertEquals(1L, count);
        assertEquals(5.0, jedis.zscore("dst", "b"), 0.001);
    }

    @Test
    void zrangewithscores() {
        jedis.zadd("zset", 1.0, "a");
        jedis.zadd("zset", 2.0, "b");
        List<Tuple> result = jedis.zrangeWithScores("zset", 0, -1);
        assertEquals(2, result.size());
        assertEquals("a", result.get(0).getElement());
        assertEquals(1.0, result.get(0).getScore(), 0.001);
    }
}
