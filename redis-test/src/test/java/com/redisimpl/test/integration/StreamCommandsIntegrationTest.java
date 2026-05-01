package com.redisimpl.test.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Stream commands integration tests")
class StreamCommandsIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("XADD auto-generates ID and XLEN returns count")
    void xadd_autoId_xlen() {
        Map<String, String> fields = new HashMap<>();
        fields.put("name", "alice");
        fields.put("age", "30");

        StreamEntryID id = jedis.xadd("mystream", XAddParams.xAddParams(), fields);
        assertNotNull(id);
        assertEquals(1L, jedis.xlen("mystream"));

        jedis.xadd("mystream", XAddParams.xAddParams(), fields);
        assertEquals(2L, jedis.xlen("mystream"));
    }

    @Test
    @DisplayName("XADD with explicit ID and XRANGE")
    void xadd_explicitId_xrange() {
        Map<String, String> f = new HashMap<>();
        f.put("k", "v1");
        jedis.xadd("s", new StreamEntryID(1000, 0), f);
        f.put("k", "v2");
        jedis.xadd("s", new StreamEntryID(2000, 0), f);
        f.put("k", "v3");
        jedis.xadd("s", new StreamEntryID(3000, 0), f);

        List<StreamEntry> range = jedis.xrange("s",
            new StreamEntryID(1000, 0), new StreamEntryID(2000, 0));
        assertEquals(2, range.size());
        assertEquals("v1", range.get(0).getFields().get("k"));
        assertEquals("v2", range.get(1).getFields().get("k"));
    }

    @Test
    @DisplayName("XREVRANGE returns entries in reverse order")
    void xrevrange() {
        Map<String, String> f = new HashMap<>();
        for (int i = 1; i <= 3; i++) {
            f.put("i", String.valueOf(i));
            jedis.xadd("s", new StreamEntryID(i * 1000L, 0), f);
        }

        // XREVRANGE s + - (all entries reversed)
        List<StreamEntry> rev = jedis.xrevrange("s",
            StreamEntryID.MAXIMUM_ID,
            StreamEntryID.MINIMUM_ID);
        assertEquals(3, rev.size());
        assertEquals("3", rev.get(0).getFields().get("i"));
        assertEquals("1", rev.get(2).getFields().get("i"));
    }

    @Test
    @DisplayName("XREAD reads entries after given ID")
    void xread() {
        Map<String, String> f = new HashMap<>();
        f.put("v", "1");
        jedis.xadd("s", new StreamEntryID(1000, 0), f);
        f.put("v", "2");
        jedis.xadd("s", new StreamEntryID(2000, 0), f);
        f.put("v", "3");
        jedis.xadd("s", new StreamEntryID(3000, 0), f);

        Map<String, StreamEntryID> streams = new HashMap<>();
        streams.put("s", new StreamEntryID(1000, 0));
        List<Map.Entry<String, List<StreamEntry>>> result =
            jedis.xread(XReadParams.xReadParams().count(10), streams);

        assertNotNull(result);
        assertEquals(1, result.size());
        List<StreamEntry> entries = result.get(0).getValue();
        assertEquals(2, entries.size());
        assertEquals("2", entries.get(0).getFields().get("v"));
    }

    @Test
    @DisplayName("XDEL removes entries")
    void xdel() {
        Map<String, String> f = new HashMap<>();
        f.put("k", "v");
        StreamEntryID id1 = jedis.xadd("s", XAddParams.xAddParams(), f);
        jedis.xadd("s", XAddParams.xAddParams(), f);

        long deleted = jedis.xdel("s", id1);
        assertEquals(1L, deleted);
        assertEquals(1L, jedis.xlen("s"));
    }

    @Test
    @DisplayName("XTRIM limits stream length")
    void xtrim() {
        Map<String, String> f = new HashMap<>();
        f.put("k", "v");
        for (int i = 0; i < 10; i++) {
            jedis.xadd("s", XAddParams.xAddParams(), f);
        }
        assertEquals(10L, jedis.xlen("s"));

        long trimmed = jedis.xtrim("s", 5, false);
        assertEquals(5L, trimmed);
        assertEquals(5L, jedis.xlen("s"));
    }

    @Test
    @DisplayName("XGROUP CREATE and XREADGROUP basic flow")
    void xgroup_xreadgroup() {
        Map<String, String> f = new HashMap<>();
        f.put("msg", "hello");
        jedis.xadd("s", new StreamEntryID(1000, 0), f);
        f.put("msg", "world");
        jedis.xadd("s", new StreamEntryID(2000, 0), f);

        // Create group starting from beginning (0-0)
        jedis.xgroupCreate("s", "grp", new StreamEntryID(0, 0), false);

        // Read with group using ">" to get new entries
        Map<String, StreamEntryID> streams = new HashMap<>();
        streams.put("s", StreamEntryID.NEW_ENTRY);
        List<Map.Entry<String, List<StreamEntry>>> result =
            jedis.xreadGroup("grp", "consumer1",
                XReadGroupParams.xReadGroupParams().count(10), streams);

        assertNotNull(result);
        assertEquals(2, result.get(0).getValue().size());
    }

    @Test
    @DisplayName("XACK acknowledges pending entries")
    void xack() {
        Map<String, String> f = new HashMap<>();
        f.put("k", "v");
        jedis.xadd("s", new StreamEntryID(1000, 0), f);

        jedis.xgroupCreate("s", "grp", new StreamEntryID(0, 0), false);

        Map<String, StreamEntryID> streams = new HashMap<>();
        streams.put("s", StreamEntryID.NEW_ENTRY);
        List<Map.Entry<String, List<StreamEntry>>> result =
            jedis.xreadGroup("grp", "c1",
                XReadGroupParams.xReadGroupParams().count(10), streams);

        StreamEntryID id = result.get(0).getValue().get(0).getID();
        long acked = jedis.xack("s", "grp", id);
        assertEquals(1L, acked);
    }
}
