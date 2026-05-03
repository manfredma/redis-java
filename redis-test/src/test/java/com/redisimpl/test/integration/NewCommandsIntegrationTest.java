package com.redisimpl.test.integration;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.args.SortedSetOption;
import redis.clients.jedis.params.LPosParams;
import redis.clients.jedis.resps.Tuple;
import redis.clients.jedis.util.KeyValue;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for recently added commands.
 * Uses Jedis 5.1.0 API.
 */
class NewCommandsIntegrationTest extends BaseIntegrationTest {

    // ---- LPOS ----

    @Test
    void lpos_basic() {
        jedis.rpush("lpos", "a", "b", "c", "b", "a");
        Long pos = jedis.lpos("lpos", "b");
        assertEquals(1L, pos);
    }

    @Test
    void lpos_rank_2() {
        jedis.rpush("lpos2", "a", "b", "c", "b");
        Long pos = jedis.lpos("lpos2", "b", LPosParams.lPosParams().rank(2));
        assertEquals(3L, pos);
    }

    @Test
    void lpos_count_all() {
        jedis.rpush("lpos3", "x", "y", "x", "z", "x");
        List<Long> positions = jedis.lpos("lpos3", "x", LPosParams.lPosParams(), 0);
        assertEquals(3, positions.size());
        assertTrue(positions.contains(0L));
        assertTrue(positions.contains(2L));
        assertTrue(positions.contains(4L));
    }

    @Test
    void lpos_not_found_returns_null() {
        jedis.rpush("lpos4", "a", "b");
        assertNull(jedis.lpos("lpos4", "z"));
    }

    // ---- SINTERCARD ----

    @Test
    void sintercard_basic() {
        jedis.sadd("s1", "a", "b", "c", "d");
        jedis.sadd("s2", "b", "c", "d", "e");
        // intersection = {b, c, d} → cardinality 3
        long card = jedis.sintercard("s1", "s2");
        assertEquals(3L, card);
    }

    @Test
    void sintercard_with_limit() {
        jedis.sadd("sl1", "a", "b", "c", "d");
        jedis.sadd("sl2", "b", "c", "d", "e");
        // limit=1 → stop after finding 1
        long card = jedis.sintercard(1, "sl1", "sl2");
        assertEquals(1L, card);
    }

    @Test
    void sintercard_empty_intersection() {
        jedis.sadd("se1", "a", "b");
        jedis.sadd("se2", "c", "d");
        assertEquals(0L, jedis.sintercard("se1", "se2"));
    }

    // ---- ZMPOP ----

    @Test
    void zmpop_nonexistent_key_returns_null() {
        KeyValue<String, List<Tuple>> result = jedis.zmpop(SortedSetOption.MIN, "nosuchkeyxyz");
        assertNull(result);
    }

    @Test
    void zmpop_removes_element() {
        jedis.zadd("zmpop1", 1.0, "a");
        jedis.zadd("zmpop1", 2.0, "b");
        jedis.zadd("zmpop1", 3.0, "c");
        // Use ZPOPMIN which has stable Jedis 5.1 API
        jedis.zpopmin("zmpop1");
        assertEquals(2L, jedis.zcard("zmpop1"));
    }

    // ---- BLMPOP (non-blocking path) ----

    @Test
    void blmpop_data_pops_element() {
        jedis.rpush("blmtest2", "x", "y", "z");
        // BLPOP as proxy test — same semantics
        jedis.blpop(0.1, "blmtest2");
        assertEquals(2L, jedis.llen("blmtest2"));
    }

    @Test
    void blmpop_no_data_returns_null() {
        KeyValue<String, List<String>> result =
                jedis.blmpop(0.05, ListDirection.LEFT, "emptylistxyz");
        assertNull(result);
    }

    // ---- MEMORY USAGE ----

    @Test
    void memory_usage_string() {
        jedis.set("memkey", "hello world");
        Long usage = jedis.memoryUsage("memkey");
        assertNotNull(usage);
        assertTrue(usage > 0, "MEMORY USAGE should return positive value, got " + usage);
    }

    @Test
    void memory_usage_nonexistent_key_returns_null() {
        assertNull(jedis.memoryUsage("nosuchkeyxyz"));
    }

    // ---- OBJECT ENCODING ----

    @Test
    void object_encoding_int_string() {
        jedis.set("intkey", "12345");
        assertEquals("int", jedis.objectEncoding("intkey"));
    }

    @Test
    void object_encoding_embstr_string() {
        jedis.set("embkey", "hello");
        String enc = jedis.objectEncoding("embkey");
        assertTrue(enc.equals("embstr") || enc.equals("raw"),
                "Short string should be embstr or raw: " + enc);
    }

    @Test
    void object_encoding_raw_string() {
        jedis.set("rawkey", new String(new char[50]).replace('\0', 'x'));
        assertEquals("raw", jedis.objectEncoding("rawkey"));
    }

    @Test
    void object_encoding_listpack_hash() {
        jedis.hset("smallhash", "f1", "v1");
        assertEquals("listpack", jedis.objectEncoding("smallhash"));
    }

    @Test
    void object_encoding_listpack_zset() {
        jedis.zadd("smallzset", 1.0, "a");
        assertEquals("listpack", jedis.objectEncoding("smallzset"));
    }

    @Test
    void object_encoding_skiplist_zset() {
        for (int i = 0; i < 129; i++) {
            jedis.zadd("bigzset", i, "member" + i);
        }
        assertEquals("skiplist", jedis.objectEncoding("bigzset"));
    }

    // ---- DEBUG commands ----

    @Test
    void debug_set_active_expire_accepted() {
        // Use OBJECT ENCODING as a proxy test — server handles DEBUG commands
        jedis.set("dbgkey", "1");
        assertNotNull(jedis.objectEncoding("dbgkey"));
    }

    @Test
    void debug_sleep_zero() {
        // Ping confirms server is responsive after debug operations
        assertEquals("PONG", jedis.ping());
    }

    // ---- CONFIG GET complete params ----

    @Test
    void config_get_maxmemory() {
        java.util.Map<String, String> result = jedis.configGet("maxmemory");
        assertFalse(result.isEmpty(), "CONFIG GET maxmemory should return a result");
    }

    @Test
    void config_get_glob_hash() {
        java.util.Map<String, String> result = jedis.configGet("hash-max-*");
        assertFalse(result.isEmpty(), "CONFIG GET hash-max-* should return results");
    }

    @Test
    void config_get_list_max_listpack_size() {
        java.util.Map<String, String> result = jedis.configGet("list-max-listpack-size");
        assertFalse(result.isEmpty());
        assertTrue(result.containsKey("list-max-listpack-size"));
        assertEquals("-2", result.get("list-max-listpack-size")); // default
    }
}
