package com.redisimpl.server.commands.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StreamObject unit tests")
class StreamObjectTest {

    private StreamObject stream;

    @BeforeEach
    void setUp() {
        stream = new StreamObject();
    }

    private Map<String, String> fields(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    @Test
    @DisplayName("auto-id generates monotonically increasing IDs")
    void autoId_monotonic() {
        StreamEntry e1 = stream.add(-1, -1, fields("k", "v1"));
        StreamEntry e2 = stream.add(-1, -1, fields("k", "v2"));
        assertTrue(StreamEntry.compare(e1, e2) < 0);
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("explicit full ID is stored correctly")
    void explicitId_stored() {
        StreamEntry e = stream.add(1000L, 0L, fields("name", "alice"));
        assertEquals("1000-0", e.getId());
        assertEquals("alice", e.getFields().get("name"));
    }

    @Test
    @DisplayName("explicit ID must be greater than last ID")
    void explicitId_mustBeGreater() {
        stream.add(1000L, 0L, fields("a", "b"));
        assertThrows(IllegalArgumentException.class,
            () -> stream.add(999L, 0L, fields("a", "b")));
        assertThrows(IllegalArgumentException.class,
            () -> stream.add(1000L, 0L, fields("a", "b")));
    }

    @Test
    @DisplayName("XRANGE returns entries in range")
    void xrange_basic() {
        stream.add(1000L, 0L, fields("a", "1"));
        stream.add(2000L, 0L, fields("a", "2"));
        stream.add(3000L, 0L, fields("a", "3"));

        List<StreamEntry> r = stream.range("1000-0", "2000-0", 0);
        assertEquals(2, r.size());
        assertEquals("1000-0", r.get(0).getId());
        assertEquals("2000-0", r.get(1).getId());
    }

    @Test
    @DisplayName("XRANGE with - and + returns all entries")
    void xrange_minMax() {
        stream.add(1000L, 0L, fields("a", "1"));
        stream.add(2000L, 0L, fields("a", "2"));
        List<StreamEntry> r = stream.range("-", "+", 0);
        assertEquals(2, r.size());
    }

    @Test
    @DisplayName("XREVRANGE returns entries in reverse order")
    void xrevrange_basic() {
        stream.add(1000L, 0L, fields("a", "1"));
        stream.add(2000L, 0L, fields("a", "2"));
        stream.add(3000L, 0L, fields("a", "3"));

        List<StreamEntry> r = stream.revrange("+", "-", 0);
        assertEquals(3, r.size());
        assertEquals("3000-0", r.get(0).getId());
        assertEquals("1000-0", r.get(2).getId());
    }

    @Test
    @DisplayName("XREAD returns entries after given ID")
    void xread_afterId() {
        stream.add(1000L, 0L, fields("a", "1"));
        stream.add(2000L, 0L, fields("a", "2"));
        stream.add(3000L, 0L, fields("a", "3"));

        List<StreamEntry> r = stream.read("1000-0", 0);
        assertEquals(2, r.size());
        assertEquals("2000-0", r.get(0).getId());
    }

    @Test
    @DisplayName("XDEL removes entries by ID")
    void xdel_basic() {
        stream.add(1000L, 0L, fields("a", "1"));
        stream.add(2000L, 0L, fields("a", "2"));
        long deleted = stream.delete(Arrays.asList("1000-0", "9999-0"));
        assertEquals(1, deleted);
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("XTRIM trims to maxLen")
    void xtrim_basic() {
        for (int i = 0; i < 10; i++) {
            stream.add(1000L + i, 0L, fields("i", String.valueOf(i)));
        }
        long removed = stream.trim(5, false);
        assertEquals(5, removed);
        assertEquals(5, stream.size());
    }

    @Test
    @DisplayName("maxLen enforced on XADD")
    void maxLen_enforcedOnAdd() {
        stream.setMaxLen(3);
        for (int i = 0; i < 5; i++) {
            stream.add(1000L + i, 0L, fields("i", String.valueOf(i)));
        }
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("XRANGE with count limits results")
    void xrange_count() {
        for (int i = 0; i < 5; i++) {
            stream.add(1000L + i, 0L, fields("i", String.valueOf(i)));
        }
        List<StreamEntry> r = stream.range("-", "+", 2);
        assertEquals(2, r.size());
    }
}
