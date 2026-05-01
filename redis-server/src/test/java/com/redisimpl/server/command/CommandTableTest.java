package com.redisimpl.server.command;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CommandTableTest {

    private CommandTable table;

    @BeforeEach
    void setUp() {
        table = new CommandTable();
        table.register(new TestCommands());
    }

    @Test
    void lookup_existingCommand_found() {
        CommandEntry entry = table.lookup("ping");
        assertNotNull(entry);
        assertEquals("ping", entry.getName());
    }

    @Test
    void lookup_caseInsensitive() {
        assertNotNull(table.lookup("PING"));
        assertNotNull(table.lookup("Ping"));
        assertNotNull(table.lookup("ping"));
    }

    @Test
    void lookup_nonExistentCommand_returnsNull() {
        assertNull(table.lookup("nonexistent"));
    }

    @Test
    void lookup_multiArgCommand() {
        CommandEntry entry = table.lookup("set");
        assertNotNull(entry);
        assertEquals("set", entry.getName());
        assertEquals(-3, entry.getArity()); // SET key value [options...]
    }

    @Test
    void commandCount_correct() {
        int count = table.commandCount();
        assertTrue(count >= 2); // at least PING and SET registered
    }

    @Test
    void getAll_returnsAllCommands() {
        assertFalse(table.getAll().isEmpty());
    }

    @Test
    void lookup_getFlags() {
        CommandEntry entry = table.lookup("ping");
        assertNotNull(entry);
        assertTrue(entry.getFlags().contains("fast"));
    }

    // Test command class
    static class TestCommands {

        @RedisCommand(name = "ping", arity = -1, flags = "fast stale no-monitor", firstKey = 0, lastKey = 0, step = 0)
        public byte[] ping(com.redisimpl.server.client.RedisClient client, byte[][] argv) {
            return com.redisimpl.server.resp.RespEncoder.PONG;
        }

        @RedisCommand(name = "set", arity = -3, flags = "write denyoom", firstKey = 1, lastKey = 1, step = 1)
        public byte[] set(com.redisimpl.server.client.RedisClient client, byte[][] argv) {
            return com.redisimpl.server.resp.RespEncoder.OK;
        }
    }
}
