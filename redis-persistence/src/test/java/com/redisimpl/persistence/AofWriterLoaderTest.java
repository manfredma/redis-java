package com.redisimpl.persistence;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.server.db.RedisDb;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD tests for AofWriter, AofLoader, and AofRewriter.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AofWriterLoaderTest {

    private Path tmpDir;
    private RedisConfig config;
    private RedisDb[] dbs;

    @BeforeEach
    void setUp() throws IOException {
        tmpDir = Files.createTempDirectory("aof-test-");
        config = new RedisConfig();
        config.setDir(tmpDir.toString());
        config.setAppendfilename("test.aof");
        config.setDbfilename("test.rdb");
        config.setAppendonly(true);
        config.setAppendfsync("no"); // use "no" for fast tests
        config.setAofUseRdbPreamble(false);
        dbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) {
            dbs[i] = new RedisDb(i);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        java.io.File dir = tmpDir.toFile();
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File f : files) f.delete();
        }
        dir.delete();
    }

    // ---- AOF write tests ----

    @Test
    @Order(1)
    @DisplayName("AOF writer creates file when first command is written")
    void aofWriter_firstWrite_createsFile() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "key".getBytes(), "value".getBytes()});
        writer.close();

        assertTrue(Files.exists(Paths.get(config.getAofFilePath())),
                "AOF file should be created");
    }

    @Test
    @Order(2)
    @DisplayName("AOF writer appends RESP-formatted commands")
    void aofWriter_appendCommand_writesRespFormat() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "hello".getBytes(), "world".getBytes()});
        writer.close();

        String content = new String(Files.readAllBytes(Paths.get(config.getAofFilePath())),
                StandardCharsets.UTF_8);
        // Should contain RESP array format
        assertTrue(content.contains("*3"), "Should contain array header *3");
        assertTrue(content.contains("$3"), "Should contain bulk string length $3");
        assertTrue(content.contains("SET"), "Should contain SET command");
        assertTrue(content.contains("hello"), "Should contain key");
        assertTrue(content.contains("world"), "Should contain value");
    }

    @Test
    @Order(3)
    @DisplayName("AOF writer includes SELECT command for non-zero database")
    void aofWriter_nonZeroDb_includesSelectCommand() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(3, new byte[][]{"SET".getBytes(), "key".getBytes(), "val".getBytes()});
        writer.close();

        String content = new String(Files.readAllBytes(Paths.get(config.getAofFilePath())),
                StandardCharsets.UTF_8);
        assertTrue(content.contains("SELECT"), "Should contain SELECT command for db 3");
        assertTrue(content.contains("3"), "Should contain db index 3");
    }

    @Test
    @Order(4)
    @DisplayName("AOF writer does not add SELECT for database 0")
    void aofWriter_dbZero_noSelectCommand() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "key".getBytes(), "val".getBytes()});
        writer.close();

        String content = new String(Files.readAllBytes(Paths.get(config.getAofFilePath())),
                StandardCharsets.UTF_8);
        assertFalse(content.startsWith("*2\r\nSELECT"),
                "Should not start with SELECT for db 0");
    }

    @Test
    @Order(5)
    @DisplayName("AOF writer appends multiple commands sequentially")
    void aofWriter_multipleCommands_allAppended() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "k1".getBytes(), "v1".getBytes()});
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "k2".getBytes(), "v2".getBytes()});
        writer.appendCommand(0, new byte[][]{"DEL".getBytes(), "k1".getBytes()});
        writer.close();

        String content = new String(Files.readAllBytes(Paths.get(config.getAofFilePath())),
                StandardCharsets.UTF_8);
        assertTrue(content.contains("k1"), "Should contain k1");
        assertTrue(content.contains("k2"), "Should contain k2");
        assertTrue(content.contains("DEL"), "Should contain DEL");
    }

    // ---- AOF load tests ----

    @Test
    @Order(10)
    @DisplayName("AOF loader replays SET command")
    void aofLoader_setCommand_keyRestored() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "mykey".getBytes(), "myval".getBytes()});
        writer.close();

        AofLoader loader = new AofLoader(config);
        loader.load(dbs);

        RedisObject obj = dbs[0].lookupKey("mykey".getBytes());
        assertNotNull(obj, "Key should be restored from AOF");
        assertEquals(RedisObjectConstants.OBJ_TYPE_STRING, obj.getType());
    }

    @Test
    @Order(11)
    @DisplayName("AOF loader replays SELECT + SET commands")
    void aofLoader_selectThenSet_keyInCorrectDb() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(2, new byte[][]{"SET".getBytes(), "dbkey".getBytes(), "dbval".getBytes()});
        writer.close();

        AofLoader loader = new AofLoader(config);
        loader.load(dbs);

        assertNull(dbs[0].lookupKey("dbkey".getBytes()), "Key should not be in db 0");
        assertNotNull(dbs[2].lookupKey("dbkey".getBytes()), "Key should be in db 2");
    }

    @Test
    @Order(12)
    @DisplayName("AOF loader replays DEL command")
    void aofLoader_delCommand_keyRemoved() throws IOException {
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "delkey".getBytes(), "val".getBytes()});
        writer.appendCommand(0, new byte[][]{"DEL".getBytes(), "delkey".getBytes()});
        writer.close();

        AofLoader loader = new AofLoader(config);
        loader.load(dbs);

        assertNull(dbs[0].lookupKey("delkey".getBytes()), "Key should be deleted");
    }

    @Test
    @Order(13)
    @DisplayName("AOF loader handles non-existent file gracefully")
    void aofLoader_noFile_noException() throws IOException {
        AofLoader loader = new AofLoader(config);
        assertDoesNotThrow(() -> loader.load(dbs), "Should not throw if AOF file missing");
    }

    @Test
    @Order(14)
    @DisplayName("AOF loader replays EXPIRE command")
    void aofLoader_expireCommand_expirySet() throws IOException {
        long futureExpiry = System.currentTimeMillis() + 60_000;
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "expkey".getBytes(), "val".getBytes()});
        writer.appendCommand(0, new byte[][]{"PEXPIREAT".getBytes(), "expkey".getBytes(),
                String.valueOf(futureExpiry).getBytes()});
        writer.close();

        AofLoader loader = new AofLoader(config);
        loader.load(dbs);

        assertNotNull(dbs[0].lookupKey("expkey".getBytes()), "Key should exist (not yet expired)");
        long expiry = dbs[0].getExpiry("expkey".getBytes());
        assertTrue(expiry > 0, "Expiry should be set");
    }

    // ---- AOF rewriter tests ----

    @Test
    @Order(20)
    @DisplayName("BGREWRITEAOF produces a valid AOF file")
    void aofRewriter_bgRewrite_producesValidFile() throws Exception {
        // Setup some state
        dbs[0].setKey("rw1".getBytes(), strObj("val1"));
        dbs[0].setKey("rw2".getBytes(), strObj("val2"));
        dbs[1].setKey("rw3".getBytes(), strObj("val3"));

        AofRewriter rewriter = new AofRewriter(config);
        rewriter.bgRewrite(dbs);

        // Wait for completion
        long deadline = System.currentTimeMillis() + 5000;
        while (rewriter.isRewriteInProgress() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertFalse(rewriter.isRewriteInProgress(), "Rewrite should complete within 5 seconds");

        // Load the new AOF and verify
        RedisDb[] freshDbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) freshDbs[i] = new RedisDb(i);
        AofLoader loader = new AofLoader(config);
        loader.load(freshDbs);

        assertNotNull(freshDbs[0].lookupKey("rw1".getBytes()));
        assertNotNull(freshDbs[0].lookupKey("rw2".getBytes()));
        assertNotNull(freshDbs[1].lookupKey("rw3".getBytes()));
    }

    @Test
    @Order(21)
    @DisplayName("BGREWRITEAOF with RDB preamble produces loadable file")
    void aofRewriter_bgRewriteWithRdbPreamble_loadable() throws Exception {
        config.setAofUseRdbPreamble(true);
        dbs[0].setKey("preamble_key".getBytes(), strObj("preamble_val"));

        AofRewriter rewriter = new AofRewriter(config);
        rewriter.bgRewrite(dbs);

        long deadline = System.currentTimeMillis() + 5000;
        while (rewriter.isRewriteInProgress() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertFalse(rewriter.isRewriteInProgress());

        // Load: AofLoader should detect RDB preamble and use RdbLoader for the preamble
        RedisDb[] freshDbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) freshDbs[i] = new RedisDb(i);
        AofLoader loader = new AofLoader(config);
        loader.load(freshDbs);

        assertNotNull(freshDbs[0].lookupKey("preamble_key".getBytes()));
    }

    @Test
    @Order(22)
    @DisplayName("Incremental buffer is appended during BGREWRITEAOF")
    void aofRewriter_incrementalBuffer_appendedAfterRewrite() throws Exception {
        // Start with one key
        dbs[0].setKey("before".getBytes(), strObj("v1"));

        AofRewriter rewriter = new AofRewriter(config);

        // Buffer a command before/during the rewrite — it should be included in the output
        rewriter.appendToBuffer(0, new byte[][]{"SET".getBytes(), "after".getBytes(), "v2".getBytes()});
        rewriter.bgRewrite(dbs);

        long deadline = System.currentTimeMillis() + 5000;
        while (rewriter.isRewriteInProgress() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertFalse(rewriter.isRewriteInProgress(), "Rewrite should complete");

        // Load and verify both keys present
        RedisDb[] freshDbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) freshDbs[i] = new RedisDb(i);
        AofLoader loader = new AofLoader(config);
        loader.load(freshDbs);

        assertNotNull(freshDbs[0].lookupKey("before".getBytes()));
        assertNotNull(freshDbs[0].lookupKey("after".getBytes()));
    }

    // ---- fsync strategy tests ----

    @Test
    @Order(30)
    @DisplayName("AofWriter with fsync=always syncs after each write")
    void aofWriter_fsyncAlways_fileVisible() throws IOException {
        config.setAppendfsync("always");
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "k".getBytes(), "v".getBytes()});
        writer.close();

        assertTrue(Files.exists(Paths.get(config.getAofFilePath())));
        assertTrue(Files.size(Paths.get(config.getAofFilePath())) > 0);
    }

    @Test
    @Order(31)
    @DisplayName("AofWriter with fsync=everysec creates background fsync thread")
    void aofWriter_fsyncEverysec_backgroundThreadActive() throws IOException, InterruptedException {
        config.setAppendfsync("everysec");
        AofWriter writer = new AofWriter(config);
        writer.appendCommand(0, new byte[][]{"SET".getBytes(), "k".getBytes(), "v".getBytes()});
        Thread.sleep(100); // give background thread time to run
        writer.close();

        assertTrue(Files.exists(Paths.get(config.getAofFilePath())));
    }

    // ---- Helpers ----

    private static RedisObject strObj(String value) {
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                value.getBytes(StandardCharsets.UTF_8));
    }
}
