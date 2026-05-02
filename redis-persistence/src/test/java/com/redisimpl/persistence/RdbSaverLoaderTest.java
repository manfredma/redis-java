package com.redisimpl.persistence;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.intset.IntSet;
import com.redisimpl.core.listpack.ListPack;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.core.quicklist.QuickList;
import com.redisimpl.core.zskiplist.ZSkipList;
import com.redisimpl.server.commands.zset.ZSetCommands;
import com.redisimpl.server.db.RedisDb;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD tests for RdbSaver and RdbLoader.
 * Tests are written first (red phase), then implementation makes them green.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RdbSaverLoaderTest {

    private Path tmpDir;
    private RedisConfig config;
    private RedisDb[] dbs;

    @BeforeEach
    void setUp() throws IOException {
        tmpDir = Files.createTempDirectory("rdb-test-");
        config = new RedisConfig();
        config.setDir(tmpDir.toString());
        config.setDbfilename("test.rdb");
        dbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) {
            dbs[i] = new RedisDb(i);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up temp files
        File dir = tmpDir.toFile();
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) f.delete();
        }
        dir.delete();
    }

    // ---- CRC64 tests ----

    @Test
    @Order(1)
    @DisplayName("CRC64 of empty input is 0")
    void crc64_emptyInput_isZero() {
        assertEquals(0L, Crc64.digest(new byte[0]));
    }

    @Test
    @Order(2)
    @DisplayName("CRC64 produces stable output for known input")
    void crc64_knownInput_stableOutput() {
        // Redis test vector: "123456789" -> known CRC64
        byte[] input = "123456789".getBytes(StandardCharsets.US_ASCII);
        long crc = Crc64.digest(input);
        // CRC-64/Jones (Redis polynomial 0xad93d23594c935a9) of "123456789"
        assertEquals(0xcf228cf2176e85edL, crc);
    }

    @Test
    @Order(3)
    @DisplayName("CRC64 incremental update matches full digest")
    void crc64_incrementalUpdate_matchesFullDigest() {
        byte[] data = "Hello, Redis!".getBytes(StandardCharsets.UTF_8);
        long full = Crc64.digest(data);
        long incremental = 0L;
        incremental = Crc64.digest(incremental, data, 0, 5);
        incremental = Crc64.digest(incremental, data, 5, data.length - 5);
        assertEquals(full, incremental);
    }

    // ---- RDB magic header ----

    @Test
    @Order(10)
    @DisplayName("RDB file starts with REDIS0011 magic")
    void rdbSave_emptyDb_hasMagicHeader() throws IOException {
        RdbSaver saver = new RdbSaver(config);
        saver.save(dbs);

        byte[] fileBytes = Files.readAllBytes(Paths.get(config.getRdbFilePath()));
        assertTrue(fileBytes.length >= 9, "RDB file too short");
        String magic = new String(fileBytes, 0, 9, StandardCharsets.US_ASCII);
        assertEquals("REDIS0011", magic);
    }

    @Test
    @Order(11)
    @DisplayName("RDB file ends with EOF opcode and 8-byte CRC64")
    void rdbSave_emptyDb_hasEofAndCrc() throws IOException {
        RdbSaver saver = new RdbSaver(config);
        saver.save(dbs);

        byte[] fileBytes = Files.readAllBytes(Paths.get(config.getRdbFilePath()));
        // Second-to-last 9 bytes: [0xFF] + 8-byte CRC
        int eofOffset = fileBytes.length - 9;
        assertTrue(eofOffset >= 0);
        assertEquals((byte) 0xFF, fileBytes[eofOffset], "Expected EOF opcode 0xFF");
        // Verify CRC: CRC of everything before the 8-byte checksum
        long expectedCrc = Crc64.digest(fileBytes, 0, fileBytes.length - 8);
        long actualCrc = Crc64.readLE64(fileBytes, fileBytes.length - 8);
        assertEquals(expectedCrc, actualCrc, "CRC64 checksum mismatch");
    }

    // ---- String encoding ----

    @Test
    @Order(20)
    @DisplayName("Save and load integer-encoded string (INT8)")
    void rdbSaveLoad_intString_int8() throws IOException {
        byte[] key = "myint".getBytes();
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_INT,
                42L);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_STRING, loaded.getType());
        assertEquals("42", getStringValue(loaded));
    }

    @Test
    @Order(21)
    @DisplayName("Save and load integer-encoded string (INT16)")
    void rdbSaveLoad_intString_int16() throws IOException {
        byte[] key = "myint16".getBytes();
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_INT,
                1000L);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals("1000", getStringValue(loaded));
    }

    @Test
    @Order(22)
    @DisplayName("Save and load integer-encoded string (INT32)")
    void rdbSaveLoad_intString_int32() throws IOException {
        byte[] key = "myint32".getBytes();
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_INT,
                70000L);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals("70000", getStringValue(loaded));
    }

    @Test
    @Order(23)
    @DisplayName("Save and load raw string (short)")
    void rdbSaveLoad_rawString_short() throws IOException {
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                value);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertArrayEquals(value, getStringBytes(loaded));
    }

    @Test
    @Order(24)
    @DisplayName("Save and load LZF-compressed string (long string)")
    void rdbSaveLoad_lzfString_longString() throws IOException {
        byte[] key = "longkey".getBytes();
        // Create a compressible 100-byte string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) sb.append("AAAAAAAAAA");
        byte[] value = sb.toString().getBytes(StandardCharsets.UTF_8);
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                value);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertArrayEquals(value, getStringBytes(loaded));
    }

    // ---- Expiry ----

    @Test
    @Order(30)
    @DisplayName("Save and load key with expiry time")
    void rdbSaveLoad_keyWithExpiry() throws IOException {
        byte[] key = "expkey".getBytes();
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                "value".getBytes());
        dbs[0].setKey(key, obj);
        long expiry = System.currentTimeMillis() + 60_000; // 1 minute from now
        dbs[0].setExpiry(key, expiry);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded, "Key should not be expired yet");
        long loadedExpiry = dbs[0].getExpiry(key);
        assertEquals(expiry, loadedExpiry, "Expiry time should be preserved");
    }

    @Test
    @Order(31)
    @DisplayName("Expired key is not loaded")
    void rdbSaveLoad_expiredKey_notLoaded() throws IOException {
        byte[] key = "expiredkey".getBytes();
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                "value".getBytes());
        dbs[0].setKey(key, obj);
        long expiry = System.currentTimeMillis() - 1000; // already expired
        dbs[0].setExpiry(key, expiry);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNull(loaded, "Expired key should not be loaded");
    }

    // ---- List ----

    @Test
    @Order(40)
    @DisplayName("Save and load list (quicklist encoding)")
    void rdbSaveLoad_list_quicklist() throws IOException {
        byte[] key = "mylist".getBytes();
        QuickList ql = QuickList.create();
        ql = ql.rpush("one".getBytes());
        ql = ql.rpush("two".getBytes());
        ql = ql.rpush("three".getBytes());
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_LIST,
                RedisObjectConstants.OBJ_ENCODING_QUICKLIST,
                ql);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_LIST, loaded.getType());
        QuickList loadedQl = (QuickList) loaded.getPtr();
        assertEquals(3L, loadedQl.llen());
        assertArrayEquals("one".getBytes(), loadedQl.index(0));
        assertArrayEquals("two".getBytes(), loadedQl.index(1));
        assertArrayEquals("three".getBytes(), loadedQl.index(2));
    }

    // ---- Hash ----

    @Test
    @Order(50)
    @DisplayName("Save and load hash (listpack encoding)")
    void rdbSaveLoad_hash_listpack() throws IOException {
        byte[] key = "myhash".getBytes();
        ListPack lp = ListPack.create();
        lp = lp.append("field1".getBytes());
        lp = lp.append("value1".getBytes());
        lp = lp.append("field2".getBytes());
        lp = lp.append("value2".getBytes());
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_HASH,
                RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                lp);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_HASH, loaded.getType());
    }

    @Test
    @Order(51)
    @DisplayName("Save and load hash (hashtable encoding)")
    void rdbSaveLoad_hash_hashtable() throws IOException {
        byte[] key = "myhash_ht".getBytes();
        Dict dict = Dict.create();
        dict.put("f1".getBytes(), "v1".getBytes());
        dict.put("f2".getBytes(), "v2".getBytes());
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_HASH,
                RedisObjectConstants.OBJ_ENCODING_HT,
                dict);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_HASH, loaded.getType());
        Dict loadedDict = (Dict) loaded.getPtr();
        assertArrayEquals("v1".getBytes(), (byte[]) loadedDict.get("f1".getBytes()));
        assertArrayEquals("v2".getBytes(), (byte[]) loadedDict.get("f2".getBytes()));
    }

    // ---- Set ----

    @Test
    @Order(60)
    @DisplayName("Save and load set (intset encoding)")
    void rdbSaveLoad_set_intset() throws IOException {
        byte[] key = "myset_intset".getBytes();
        IntSet is = IntSet.create();
        is = is.add(1L);
        is = is.add(2L);
        is = is.add(3L);
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_SET,
                RedisObjectConstants.OBJ_ENCODING_INTSET,
                is);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_SET, loaded.getType());
        IntSet loadedIs = (IntSet) loaded.getPtr();
        assertEquals(3, loadedIs.getLength());
        assertTrue(loadedIs.contains(1L));
        assertTrue(loadedIs.contains(2L));
        assertTrue(loadedIs.contains(3L));
    }

    @Test
    @Order(61)
    @DisplayName("Save and load set (hashtable encoding)")
    void rdbSaveLoad_set_hashtable() throws IOException {
        byte[] key = "myset_ht".getBytes();
        Dict dict = Dict.create();
        dict.put("member1".getBytes(), Boolean.TRUE);
        dict.put("member2".getBytes(), Boolean.TRUE);
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_SET,
                RedisObjectConstants.OBJ_ENCODING_HT,
                dict);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_SET, loaded.getType());
        Dict loadedDict = (Dict) loaded.getPtr();
        assertNotNull(loadedDict.get("member1".getBytes()));
        assertNotNull(loadedDict.get("member2".getBytes()));
    }

    // ---- ZSet ----

    @Test
    @Order(70)
    @DisplayName("Save and load zset (listpack encoding)")
    void rdbSaveLoad_zset_listpack() throws IOException {
        byte[] key = "myzset_lp".getBytes();
        ListPack lp = ListPack.create();
        lp = lp.append("member1".getBytes());
        lp = lp.append("1.5".getBytes());
        lp = lp.append("member2".getBytes());
        lp = lp.append("2.5".getBytes());
        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_ZSET,
                RedisObjectConstants.OBJ_ENCODING_LISTPACK,
                lp);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_ZSET, loaded.getType());
    }

    @Test
    @Order(71)
    @DisplayName("Save and load zset (skiplist encoding)")
    void rdbSaveLoad_zset_skiplist() throws IOException {
        byte[] key = "myzset_sl".getBytes();

        // ZSetData contains both a ZSkipList and a Dict
        ZSetCommands.ZSetData zsetData = new ZSetCommands.ZSetData();
        zsetData.zsl.insert(1.0, "alpha".getBytes());
        zsetData.zsl.insert(2.0, "beta".getBytes());
        zsetData.dict.put("alpha".getBytes(), 1.0);
        zsetData.dict.put("beta".getBytes(), 2.0);

        RedisObject obj = RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_ZSET,
                RedisObjectConstants.OBJ_ENCODING_SKIPLIST,
                zsetData);
        dbs[0].setKey(key, obj);

        saveAndReload();

        RedisObject loaded = dbs[0].lookupKey(key);
        assertNotNull(loaded);
        assertEquals(RedisObjectConstants.OBJ_TYPE_ZSET, loaded.getType());
        assertEquals(RedisObjectConstants.OBJ_ENCODING_SKIPLIST, loaded.getEncoding());
    }

    // ---- Multiple databases ----

    @Test
    @Order(80)
    @DisplayName("Save and load multiple databases")
    void rdbSaveLoad_multipleDbs() throws IOException {
        dbs[0].setKey("key0".getBytes(), strObj("val0"));
        dbs[1].setKey("key1".getBytes(), strObj("val1"));
        dbs[2].setKey("key2".getBytes(), strObj("val2"));

        saveAndReload();

        assertNotNull(dbs[0].lookupKey("key0".getBytes()));
        assertNotNull(dbs[1].lookupKey("key1".getBytes()));
        assertNotNull(dbs[2].lookupKey("key2".getBytes()));
    }

    // ---- Round-trip integrity ----

    @Test
    @Order(90)
    @DisplayName("Empty database round-trip produces valid RDB")
    void rdbSaveLoad_emptyDb_roundTrip() throws IOException {
        RdbSaver saver = new RdbSaver(config);
        saver.save(dbs);

        RedisDb[] freshDbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) freshDbs[i] = new RedisDb(i);
        RdbLoader loader = new RdbLoader(config);
        loader.load(freshDbs);

        for (RedisDb db : freshDbs) {
            assertEquals(0, db.dbSize());
        }
    }

    @Test
    @Order(91)
    @DisplayName("RDB loader validates CRC64 checksum")
    void rdbLoader_corruptedCrc_throwsException() throws IOException {
        RdbSaver saver = new RdbSaver(config);
        saver.save(dbs);

        // Corrupt the last byte of the CRC
        Path rdbPath = Paths.get(config.getRdbFilePath());
        byte[] bytes = Files.readAllBytes(rdbPath);
        bytes[bytes.length - 1] ^= 0xFF; // flip bits
        Files.write(rdbPath, bytes);

        RedisDb[] freshDbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) freshDbs[i] = new RedisDb(i);
        RdbLoader loader = new RdbLoader(config);
        assertThrows(IOException.class, () -> loader.load(freshDbs));
    }

    // ---- LASTSAVE ----

    @Test
    @Order(100)
    @DisplayName("RdbSaver tracks last save time")
    void rdbSaver_lastSaveTime_updatedAfterSave() throws IOException {
        RdbSaver saver = new RdbSaver(config);
        long before = System.currentTimeMillis() / 1000;
        saver.save(dbs);
        long after = System.currentTimeMillis() / 1000;

        long lastSave = saver.getLastSaveTime();
        assertTrue(lastSave >= before && lastSave <= after + 1,
                "lastSaveTime should be set to current time");
    }

    // ---- BGSAVE ----

    @Test
    @Order(110)
    @DisplayName("BGSAVE completes asynchronously and produces valid RDB")
    void rdbSaver_bgSave_completesAndProducesValidFile() throws Exception {
        dbs[0].setKey("bgkey".getBytes(), strObj("bgval"));

        RdbSaver saver = new RdbSaver(config);
        saver.bgSave(dbs);

        // Wait for background save to complete (max 5 seconds)
        long deadline = System.currentTimeMillis() + 5000;
        while (saver.isBgSaveInProgress() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertFalse(saver.isBgSaveInProgress(), "BGSAVE should complete within 5 seconds");

        // Verify the file is valid
        RedisDb[] freshDbs = new RedisDb[16];
        for (int i = 0; i < 16; i++) freshDbs[i] = new RedisDb(i);
        RdbLoader loader = new RdbLoader(config);
        loader.load(freshDbs);
        assertNotNull(freshDbs[0].lookupKey("bgkey".getBytes()));
    }

    // ---- Helpers ----

    private void saveAndReload() throws IOException {
        RdbSaver saver = new RdbSaver(config);
        saver.save(dbs);

        // Reset dbs
        for (int i = 0; i < 16; i++) dbs[i] = new RedisDb(i);

        RdbLoader loader = new RdbLoader(config);
        loader.load(dbs);
    }

    private static RedisObject strObj(String value) {
        return RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_RAW,
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static String getStringValue(RedisObject obj) {
        Object ptr = obj.getPtr();
        if (ptr instanceof Long) return String.valueOf((Long) ptr);
        if (ptr instanceof byte[]) return new String((byte[]) ptr, StandardCharsets.UTF_8);
        return ptr.toString();
    }

    private static byte[] getStringBytes(RedisObject obj) {
        Object ptr = obj.getPtr();
        if (ptr instanceof com.redisimpl.core.sds.Sds) return ((com.redisimpl.core.sds.Sds) ptr).toBytes();
        if (ptr instanceof byte[]) return (byte[]) ptr;
        if (ptr instanceof Long) return String.valueOf((Long) ptr).getBytes(StandardCharsets.UTF_8);
        return ptr.toString().getBytes(StandardCharsets.UTF_8);
    }

}
