package com.redisimpl.server.client;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RedisClientTest {

    @Test
    void create_defaultState() {
        RedisClient client = new RedisClient(1);
        assertEquals(1, client.getFd());
        assertEquals(0, client.getDb());
        assertEquals(0, client.getArgc());
        assertNotNull(client.getQuerybuf());
        assertEquals(0, client.getQuerybuf().length());
    }

    @Test
    void appendToQuerybuf() {
        RedisClient client = new RedisClient(1);
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        client.appendQueryBuf(data);
        assertEquals(5, client.getQuerybuf().length());
        assertEquals("hello", client.getQuerybuf().toStr());
    }

    @Test
    void appendToQuerybuf_multiple() {
        RedisClient client = new RedisClient(1);
        client.appendQueryBuf("hello".getBytes(StandardCharsets.UTF_8));
        client.appendQueryBuf(" world".getBytes(StandardCharsets.UTF_8));
        assertEquals("hello world", client.getQuerybuf().toStr());
    }

    @Test
    void writeToFixedBuf_withinCapacity() {
        RedisClient client = new RedisClient(1);
        byte[] data = "OK\r\n".getBytes(StandardCharsets.UTF_8);
        boolean written = client.addReplyToFixedBuf(data);
        assertTrue(written);
        assertEquals(data.length, client.getBufpos());
    }

    @Test
    void writeToFixedBuf_overflow_usesReplyList() {
        RedisClient client = new RedisClient(1);
        // Fill up fixed buffer (16KB = 16384 bytes)
        byte[] big = new byte[RedisClient.REDIS_REPLY_CHUNK_BYTES];
        java.util.Arrays.fill(big, (byte) 'x');
        boolean first = client.addReplyToFixedBuf(big);
        assertTrue(first);

        // Next write should overflow to reply list
        byte[] extra = "overflow".getBytes(StandardCharsets.UTF_8);
        boolean second = client.addReplyToFixedBuf(extra);
        assertFalse(second); // can't fit in fixed buf
        client.addReplyToReplyList(extra);
        assertEquals(1, client.getReply().size());
    }

    @Test
    void setAndGetArgv() {
        RedisClient client = new RedisClient(1);
        byte[][] argv = {
                "SET".getBytes(StandardCharsets.UTF_8),
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8)
        };
        client.setArgv(argv);
        assertEquals(3, client.getArgc());
        assertEquals("SET", new String(client.getArgv()[0], StandardCharsets.UTF_8));
    }

    @Test
    void dbIndex_default_zero() {
        RedisClient client = new RedisClient(1);
        assertEquals(0, client.getDb());
    }

    @Test
    void setDb_changesDb() {
        RedisClient client = new RedisClient(1);
        client.setDb(3);
        assertEquals(3, client.getDb());
    }

    @Test
    void flags_defaultZero() {
        RedisClient client = new RedisClient(1);
        assertEquals(0, client.getFlags());
    }

    @Test
    void resetQuerybuf() {
        RedisClient client = new RedisClient(1);
        client.appendQueryBuf("hello".getBytes(StandardCharsets.UTF_8));
        client.resetQueryBuf();
        assertEquals(0, client.getQuerybuf().length());
    }

    @Test
    void lastInteraction_isSet() {
        long before = System.currentTimeMillis();
        RedisClient client = new RedisClient(1);
        long after = System.currentTimeMillis();
        assertTrue(client.getLastInteraction() >= before);
        assertTrue(client.getLastInteraction() <= after);
    }
}
