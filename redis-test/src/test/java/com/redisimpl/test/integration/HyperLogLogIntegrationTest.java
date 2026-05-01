package com.redisimpl.test.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("HyperLogLog (PFADD/PFCOUNT/PFMERGE) integration tests")
class HyperLogLogIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("PFADD returns 1 when registers change, 0 when not")
    void pfadd_changeDetection() {
        long r1 = jedis.pfadd("hll", "a", "b", "c");
        assertEquals(1L, r1);

        long r2 = jedis.pfadd("hll", "a", "b", "c");
        assertEquals(0L, r2);
    }

    @Test
    @DisplayName("PFCOUNT returns approximate cardinality")
    void pfcount_cardinality() {
        for (int i = 0; i < 100; i++) {
            jedis.pfadd("hll", "element-" + i);
        }
        long count = jedis.pfcount("hll");
        // Allow 5% error margin
        assertTrue(count >= 90 && count <= 110,
            "Expected ~100 but got " + count);
    }

    @Test
    @DisplayName("PFCOUNT on non-existent key returns 0")
    void pfcount_nonExistent() {
        assertEquals(0L, jedis.pfcount("nosuchkey"));
    }

    @Test
    @DisplayName("PFMERGE merges multiple HLLs")
    void pfmerge() {
        jedis.pfadd("hll1", "a", "b", "c");
        jedis.pfadd("hll2", "d", "e", "f");

        jedis.pfmerge("merged", "hll1", "hll2");
        long count = jedis.pfcount("merged");
        assertTrue(count >= 5 && count <= 8,
            "Expected ~6 but got " + count);
    }

    @Test
    @DisplayName("PFCOUNT with multiple keys merges and counts")
    void pfcount_multipleKeys() {
        jedis.pfadd("h1", "a", "b", "c");
        jedis.pfadd("h2", "d", "e", "f");

        long count = jedis.pfcount("h1", "h2");
        assertTrue(count >= 5 && count <= 8,
            "Expected ~6 but got " + count);
    }

    @Test
    @DisplayName("PFADD to empty key creates HLL")
    void pfadd_emptyKey() {
        assertEquals(0L, jedis.pfcount("newkey"));
        jedis.pfadd("newkey", "x");
        assertEquals(1L, jedis.pfcount("newkey"));
    }
}
