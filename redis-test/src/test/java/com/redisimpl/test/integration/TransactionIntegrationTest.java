package com.redisimpl.test.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Transaction;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Transaction (MULTI/EXEC) integration tests")
class TransactionIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("MULTI/EXEC executes queued commands atomically")
    void multiExec_basic() {
        Transaction tx = jedis.multi();
        tx.set("k1", "v1");
        tx.set("k2", "v2");
        tx.get("k1");
        List<Object> results = tx.exec();

        assertNotNull(results);
        assertEquals(3, results.size());
        assertEquals("OK", results.get(0));
        assertEquals("OK", results.get(1));
        assertEquals("v1", results.get(2));

        assertEquals("v1", jedis.get("k1"));
        assertEquals("v2", jedis.get("k2"));
    }

    @Test
    @DisplayName("DISCARD cancels a transaction")
    void discard() {
        jedis.set("k", "original");
        Transaction tx = jedis.multi();
        tx.set("k", "changed");
        tx.discard();

        assertEquals("original", jedis.get("k"));
    }

    @Test
    @DisplayName("Empty EXEC returns empty array")
    void exec_empty() {
        Transaction tx = jedis.multi();
        List<Object> results = tx.exec();
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    @DisplayName("Commands are queued and executed in order")
    void queued_response() {
        jedis.set("counter", "0");
        Transaction tx = jedis.multi();
        tx.incr("counter");
        tx.incr("counter");
        tx.incr("counter");
        List<Object> results = tx.exec();

        assertEquals(3, results.size());
        assertEquals(1L, results.get(0));
        assertEquals(2L, results.get(1));
        assertEquals(3L, results.get(2));
    }

    @Test
    @DisplayName("Multiple types of commands work in a transaction")
    void multipleTypes_inTransaction() {
        Transaction tx = jedis.multi();
        tx.set("str", "hello");
        tx.lpush("lst", "item1", "item2");
        tx.hset("hsh", "field", "value");
        tx.sadd("st", "member");
        List<Object> results = tx.exec();

        assertEquals(4, results.size());
        assertEquals("OK", results.get(0));
        assertEquals(2L, results.get(1));
        assertEquals(1L, results.get(2));
        assertEquals(1L, results.get(3));

        assertEquals("hello", jedis.get("str"));
    }
}
