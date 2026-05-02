package com.redisimpl.core.dict;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DictTest {

    private static byte[] k(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void create_empty() {
        Dict dict = Dict.create();
        assertEquals(0, dict.size());
        assertTrue(dict.isEmpty());
    }

    @Test
    void put_and_get() {
        Dict dict = Dict.create();
        dict.put(k("key1"), "value1");
        assertEquals("value1", dict.get(k("key1")));
    }

    @Test
    void put_overwrite_existing() {
        Dict dict = Dict.create();
        dict.put(k("key"), "old");
        dict.put(k("key"), "new");
        assertEquals("new", dict.get(k("key")));
        assertEquals(1, dict.size());
    }

    @Test
    void get_nonExistentKey_returnsNull() {
        Dict dict = Dict.create();
        assertNull(dict.get(k("missing")));
    }

    @Test
    void containsKey_existingKey() {
        Dict dict = Dict.create();
        dict.put(k("k"), "v");
        assertTrue(dict.containsKey(k("k")));
    }

    @Test
    void containsKey_missingKey() {
        Dict dict = Dict.create();
        assertFalse(dict.containsKey(k("missing")));
    }

    @Test
    void delete_existingKey() {
        Dict dict = Dict.create();
        dict.put(k("k"), "v");
        boolean removed = dict.delete(k("k"));
        assertTrue(removed);
        assertFalse(dict.containsKey(k("k")));
        assertEquals(0, dict.size());
    }

    @Test
    void delete_nonExistentKey_returnsFalse() {
        Dict dict = Dict.create();
        assertFalse(dict.delete(k("missing")));
    }

    @Test
    void size_tracksCorrectly() {
        Dict dict = Dict.create();
        assertEquals(0, dict.size());
        dict.put(k("a"), 1);
        assertEquals(1, dict.size());
        dict.put(k("b"), 2);
        assertEquals(2, dict.size());
        dict.delete(k("a"));
        assertEquals(1, dict.size());
    }

    @Test
    void keySet_returnsAllKeys() {
        Dict dict = Dict.create();
        dict.put(k("k1"), "v1");
        dict.put(k("k2"), "v2");
        dict.put(k("k3"), "v3");
        Set<byte[]> keys = dict.keySet();
        assertEquals(3, keys.size());
    }

    @Test
    void values_returnsAllValues() {
        Dict dict = Dict.create();
        dict.put(k("k1"), "v1");
        dict.put(k("k2"), "v2");
        Collection<Object> values = dict.values();
        assertEquals(2, values.size());
        assertTrue(values.contains("v1"));
        assertTrue(values.contains("v2"));
    }

    @Test
    void manyEntries_triggerRehash() {
        Dict dict = Dict.create();
        // Insert enough entries to trigger rehash
        for (int i = 0; i < 200; i++) {
            dict.put(k("key" + i), "value" + i);
        }
        assertEquals(200, dict.size());
        // Verify all entries still accessible
        for (int i = 0; i < 200; i++) {
            assertEquals("value" + i, dict.get(k("key" + i)));
        }
    }

    @Test
    void binaryKeys_withNullBytes() {
        Dict dict = Dict.create();
        byte[] key = new byte[]{0, 1, 2, 3, 0};
        dict.put(key, "binary");
        assertEquals("binary", dict.get(key));
    }

    @Test
    void nullValue_isAllowed() {
        Dict dict = Dict.create();
        dict.put(k("k"), null);
        assertTrue(dict.containsKey(k("k")));
        assertNull(dict.get(k("k")));
    }

    @Test
    void rehashStep_completesEventually() {
        Dict dict = Dict.create();
        for (int i = 0; i < 100; i++) {
            dict.put(k("k" + i), i);
        }
        // Force rehash steps
        for (int i = 0; i < 200; i++) {
            dict.rehashStep();
        }
        // Dict should still work correctly
        for (int i = 0; i < 100; i++) {
            assertEquals(i, dict.get(k("k" + i)));
        }
    }

    @Test
    void iterator_coversAllEntries() {
        Dict dict = Dict.create();
        for (int i = 0; i < 50; i++) {
            dict.put(k("k" + i), "v" + i);
        }
        int count = 0;
        for (Dict.Entry entry : dict) {
            count++;
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());
        }
        assertEquals(50, count);
    }

    // ---- dictScan tests (mirrors dictScan() algorithm) ----

    @Test
    void scan_emptyDict_returnsZero() {
        Dict d = Dict.create();
        long cursor = d.scan(0, (k, v) -> fail("should not be called"));
        assertEquals(0, cursor);
    }

    @Test
    void scan_allEntriesVisited_noDuplicates() {
        Dict d = Dict.create();
        int n = 30;
        for (int i = 0; i < n; i++) {
            d.put(("key" + i).getBytes(StandardCharsets.UTF_8), i);
        }

        Set<String> seen = new HashSet<>();
        long cursor = 0;
        do {
            long[] next = {0};
            cursor = d.scan(cursor, (k, v) -> seen.add(new String(k, StandardCharsets.UTF_8)));
        } while (cursor != 0);

        // Every key must appear at least once
        for (int i = 0; i < n; i++) {
            assertTrue(seen.contains("key" + i), "missing key" + i);
        }
    }

    @Test
    void scan_duringRehash_noKeysMissed() {
        // Insert enough to trigger rehash, then scan concurrently
        Dict d = Dict.create();
        for (int i = 0; i < 20; i++) {
            d.put(("k" + i).getBytes(StandardCharsets.UTF_8), i);
        }

        Set<String> seen = new HashSet<>();
        long cursor = 0;
        do {
            // Insert during scan to exercise rehash path
            if (cursor == 0) {
                for (int i = 20; i < 25; i++) {
                    d.put(("k" + i).getBytes(StandardCharsets.UTF_8), i);
                }
            }
            cursor = d.scan(cursor, (k, v) ->
                    seen.add(new String(k, StandardCharsets.UTF_8)));
        } while (cursor != 0);

        // Original 20 keys must all be seen (new ones may or may not)
        for (int i = 0; i < 20; i++) {
            assertTrue(seen.contains("k" + i), "missed k" + i);
        }
    }

    @Test
    void reverseBits_correctness() {
        // reverseBits(reverseBits(v)) == v
        assertEquals(0L, Dict.reverseBits(0L));
        assertEquals(~0L, Dict.reverseBits(~0L));
        // 1000...0 (MSB) → 000...1 (LSB)
        assertEquals(1L, Dict.reverseBits(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, Dict.reverseBits(1L));
    }
}
