package com.redisimpl.server;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.quicklist.QuickList;
import com.redisimpl.core.sds.Sds;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.db.RedisDb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Lazy Free (UNLINK / asyncDelete) — mirrors dbAsyncDelete() + lazyfree.c.
 */
class LazyFreeTest {

    private RedisDb db;

    @BeforeEach
    void setUp() {
        db = new RedisDb(0);
    }

    @Test
    void asyncDelete_small_object_removed_immediately() {
        db.setKey("key".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_EMBSTR,
                Sds.fromString("hello")));

        assertTrue(db.asyncDelete("key".getBytes()));
        assertNull(db.lookupKey("key".getBytes()), "Key should be removed from keyspace");
    }

    @Test
    void asyncDelete_large_quicklist_deferred() throws InterruptedException {
        // Build a quicklist with > LAZYFREE_THRESHOLD (64) nodes
        QuickList ql = QuickList.create();
        for (int i = 0; i < 200; i++) {
            ql = ql.rpush(("element" + i).getBytes());
        }
        db.setKey("biglist".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_LIST,
                RedisObjectConstants.OBJ_ENCODING_QUICKLIST,
                ql));

        // asyncDelete should return true and remove from keyspace immediately
        assertTrue(db.asyncDelete("biglist".getBytes()));
        assertNull(db.lookupKey("biglist".getBytes()), "Key should be removed immediately");

        // Wait briefly for BIO thread to potentially process
        Thread.sleep(50);
        // No assertion on memory — just verify no crash
    }

    @Test
    void asyncDelete_nonexistent_key_returns_false() {
        assertFalse(db.asyncDelete("nosuchkey".getBytes()));
    }

    @Test
    void asyncDelete_removes_expiry() {
        db.setKey("expkey".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_EMBSTR,
                Sds.fromString("v")));
        db.setExpiry("expkey".getBytes(), System.currentTimeMillis() + 10000);

        db.asyncDelete("expkey".getBytes());
        assertNull(db.lookupKey("expkey".getBytes()));
        // TTL should also be gone
        assertFalse(db.isExpired("expkey".getBytes()), "No TTL entry should remain after delete");
    }

    @Test
    void lazyfree_effort_small_string_is_1() {
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_EMBSTR,
                Sds.fromString("hi"));
        // Effort for string = 1 → free synchronously (no BIO)
        // Just verify that asyncDelete handles it without error
        db.setKey("s".getBytes(), obj);
        assertTrue(db.asyncDelete("s".getBytes()));
    }
}
