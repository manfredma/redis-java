package com.redisimpl.server;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.sds.Sds;
import com.redisimpl.server.db.RedisDb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the tiered active expire cycle — mirrors activeExpireCycle() in expire.c.
 */
class ActiveExpireTest {

    private RedisDb db;

    @BeforeEach
    void setUp() {
        db = new RedisDb(0);
    }

    private void addKeyWithTtl(String key, long ttlMs) {
        db.setKey(key.getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_EMBSTR,
                Sds.fromString("val")));
        db.setExpiry(key.getBytes(), System.currentTimeMillis() + ttlMs);
    }

    @Test
    void activeExpireCycle_removes_expired_keys() throws InterruptedException {
        addKeyWithTtl("exp1", -100); // already expired
        addKeyWithTtl("exp2", -200); // already expired
        addKeyWithTtl("live1", 60000); // still alive
        addKeyWithTtl("live2", 60000); // still alive

        int deleted = db.activeExpireCycle(20);
        assertTrue(deleted >= 2, "Should delete at least 2 expired keys, got " + deleted);

        assertNull(db.lookupKey("exp1".getBytes()), "exp1 should be deleted");
        assertNull(db.lookupKey("exp2".getBytes()), "exp2 should be deleted");
        assertNotNull(db.lookupKey("live1".getBytes()), "live1 should survive");
        assertNotNull(db.lookupKey("live2".getBytes()), "live2 should survive");
    }

    @Test
    void activeExpireCycle_cursor_advances_across_calls() {
        // Add many expired keys
        for (int i = 0; i < 50; i++) {
            addKeyWithTtl("exp" + i, -1);
        }

        // Run multiple cycles, cursor should advance
        int totalDeleted = 0;
        for (int i = 0; i < 10; i++) {
            totalDeleted += db.activeExpireCycle(5);
        }
        assertTrue(totalDeleted > 0, "Should delete some expired keys across cycles");
    }

    @Test
    void activeExpireCycle_empty_db_no_crash() {
        int deleted = db.activeExpireCycle(20);
        assertEquals(0, deleted);
    }

    @Test
    void activeExpireCycle_does_not_delete_live_keys() {
        for (int i = 0; i < 20; i++) {
            addKeyWithTtl("live" + i, 60000);
        }
        int deleted = db.activeExpireCycle(20);
        assertEquals(0, deleted, "No keys should be deleted — all are alive");
        assertEquals(20, db.dbSize(), "All 20 keys should still exist");
    }

    @Test
    void lazy_expiry_on_access() {
        addKeyWithTtl("lazyexp", -1000); // already expired
        // lookupKey triggers lazy expiry
        assertNull(db.lookupKey("lazyexp".getBytes()),
                "Expired key should return null on lookupKey");
    }
}
