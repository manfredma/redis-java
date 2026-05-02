package com.redisimpl.replication;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReplicationBacklog unit tests")
class ReplicationBacklogTest {

    private ReplicationBacklog backlog;

    @BeforeEach
    void setUp() {
        backlog = new ReplicationBacklog(1024); // 1KB for testing
    }

    @Test
    @DisplayName("append increases offset")
    void append_increasesOffset() {
        byte[] data = "SET k v\r\n".getBytes();
        backlog.append(data);
        assertEquals(data.length, backlog.getMasterOffset());
    }

    @Test
    @DisplayName("getFrom returns data from offset")
    void getFrom_returnsData() {
        byte[] cmd1 = "SET k1 v1\r\n".getBytes();
        byte[] cmd2 = "SET k2 v2\r\n".getBytes();
        backlog.append(cmd1);
        backlog.append(cmd2);

        byte[] result = backlog.getFrom(0, 1024);
        assertNotNull(result);
        assertEquals(cmd1.length + cmd2.length, result.length);
    }

    @Test
    @DisplayName("getFrom with offset returns partial data")
    void getFrom_withOffset() {
        byte[] cmd1 = "SET k1 v1\r\n".getBytes();
        byte[] cmd2 = "SET k2 v2\r\n".getBytes();
        backlog.append(cmd1);
        backlog.append(cmd2);

        byte[] result = backlog.getFrom(cmd1.length, 1024);
        assertNotNull(result);
        assertEquals(cmd2.length, result.length);
    }

    @Test
    @DisplayName("canPartialResync returns true when offset is in backlog")
    void canPartialResync_inRange() {
        byte[] data = new byte[100];
        backlog.append(data);
        assertTrue(backlog.canPartialResync(50));
    }

    @Test
    @DisplayName("canPartialResync returns false when offset is before backlog start")
    void canPartialResync_beforeStart() {
        // Fill backlog past capacity
        byte[] data = new byte[512];
        for (int i = 0; i < 4; i++) backlog.append(data); // 2KB total, wraps 1KB backlog
        assertFalse(backlog.canPartialResync(0));
    }

    @Test
    @DisplayName("wrap-around works correctly")
    void wrapAround() {
        // Fill more than capacity
        byte[] data = new byte[600];
        backlog.append(data); // 600 bytes
        backlog.append(data); // 1200 bytes - wraps 1KB backlog

        long offset = backlog.getMasterOffset();
        assertTrue(offset > 1024);
        // Can only partial resync from after the wrap point
        assertTrue(backlog.canPartialResync(offset - 100));
        assertFalse(backlog.canPartialResync(0));
    }
}
