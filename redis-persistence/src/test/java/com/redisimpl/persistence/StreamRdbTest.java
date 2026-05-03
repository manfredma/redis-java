package com.redisimpl.persistence;

import com.redisimpl.core.object.RedisObject;
import com.redisimpl.core.object.RedisObjectConstants;
import com.redisimpl.server.commands.stream.StreamObject;
import com.redisimpl.server.db.RedisDb;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Stream RDB serialization/deserialization.
 * Verifies that Stream data survives a full save→load cycle.
 */
class StreamRdbTest {

    private RedisDb[] srcDbs;
    private RedisDb[] dstDbs;
    private RedisConfig config;

    @BeforeEach
    void setUp() {
        srcDbs = new RedisDb[]{new RedisDb(0)};
        dstDbs = new RedisDb[]{new RedisDb(0)};
        config = new RedisConfig();
    }

    private void roundTrip() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new RdbSaver(config).saveToStream(baos, srcDbs);
        new RdbLoader(config).loadFromBytes(baos.toByteArray(), dstDbs);
    }

    @Test
    void stream_empty_survives_rdb() throws IOException {
        StreamObject stream = new StreamObject();
        srcDbs[0].setKey("mystream".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STREAM,
                14, // OBJ_ENCODING_STREAM
                stream));

        roundTrip();

        RedisObject loaded = dstDbs[0].lookupKey("mystream".getBytes());
        assertNotNull(loaded, "Stream key should be present after RDB load");
        assertEquals(RedisObjectConstants.OBJ_TYPE_STREAM, loaded.getType());
        StreamObject result = (StreamObject) loaded.getPtr();
        assertEquals(0, result.size());
    }

    @Test
    void stream_entries_survive_rdb() throws IOException {
        StreamObject stream = new StreamObject();
        Map<String, String> f1 = new LinkedHashMap<>();
        f1.put("name", "Alice"); f1.put("age", "30");
        Map<String, String> f2 = new LinkedHashMap<>();
        f2.put("name", "Bob"); f2.put("age", "25");
        stream.addRdb("1000-0", f1);
        stream.addRdb("1000-1", f2);

        srcDbs[0].setKey("stream".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STREAM, 14, stream));

        roundTrip();

        RedisObject loaded = dstDbs[0].lookupKey("stream".getBytes());
        assertNotNull(loaded);
        StreamObject result = (StreamObject) loaded.getPtr();
        assertEquals(2, result.size());

        // Verify entries are correct
        java.util.List<com.redisimpl.server.commands.stream.StreamEntry> entries =
                result.range("-", "+", 10);
        assertEquals(2, entries.size());
        assertEquals("1000-0", entries.get(0).getId());
        assertEquals("Alice", entries.get(0).getFields().get("name"));
        assertEquals("1000-1", entries.get(1).getId());
        assertEquals("Bob", entries.get(1).getFields().get("name"));
    }

    @Test
    void stream_with_consumer_group_survives_rdb() throws IOException {
        StreamObject stream = new StreamObject();
        Map<String, String> f1 = new LinkedHashMap<>();
        f1.put("msg", "hello");
        stream.addRdb("2000-0", f1);
        stream.createGroup("mygroup", "0-0");

        srcDbs[0].setKey("s".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STREAM, 14, stream));

        roundTrip();

        RedisObject loaded = dstDbs[0].lookupKey("s".getBytes());
        assertNotNull(loaded);
        StreamObject result = (StreamObject) loaded.getPtr();
        assertEquals(1, result.size());
        assertTrue(result.getGroups().containsKey("mygroup"),
                "Consumer group should survive RDB: " + result.getGroups().keySet());
    }

    @Test
    void stream_last_id_preserved() throws IOException {
        StreamObject stream = new StreamObject();
        Map<String, String> m1 = new LinkedHashMap<>(); m1.put("x", "y");
        stream.addRdb("5000-3", m1);

        srcDbs[0].setKey("s".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STREAM, 14, stream));

        roundTrip();

        StreamObject result = (StreamObject) ((RedisObject) dstDbs[0].lookupKey("s".getBytes())).getPtr();
        assertEquals(5000L, result.getLastMillis());
        assertEquals(3L,    result.getLastSeq());
    }

    @Test
    void multiple_types_including_stream_roundtrip() throws IOException {
        // Add a String
        com.redisimpl.core.sds.Sds sds = com.redisimpl.core.sds.Sds.fromString("hello");
        srcDbs[0].setKey("str".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STRING,
                RedisObjectConstants.OBJ_ENCODING_EMBSTR, sds));

        // Add a Stream
        StreamObject stream = new StreamObject();
        Map<String, String> mf = new LinkedHashMap<>(); mf.put("f", "v");
        stream.addRdb("1-0", mf);
        srcDbs[0].setKey("stream".getBytes(), RedisObject.createObject(
                RedisObjectConstants.OBJ_TYPE_STREAM, 14, stream));

        roundTrip();

        assertNotNull(dstDbs[0].lookupKey("str".getBytes()), "String should be present");
        assertNotNull(dstDbs[0].lookupKey("stream".getBytes()), "Stream should be present");
    }
}
