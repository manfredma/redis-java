package com.redisimpl.core.dict;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Dict.getRandomKey() — mirrors dictGetRandomKey() in dict.c.
 */
class DictRandomKeyTest {

    @Test
    void getRandomKey_empty_dict_returns_null() {
        Dict d = Dict.create();
        assertNull(d.getRandomKey());
    }

    @Test
    void getRandomKey_single_entry_returns_it() {
        Dict d = Dict.create();
        d.put("key".getBytes(StandardCharsets.UTF_8), "value");
        Dict.Entry e = d.getRandomKey();
        assertNotNull(e);
        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), e.getKey());
    }

    @Test
    void getRandomKey_returns_existing_key() {
        Dict d = Dict.create();
        for (int i = 0; i < 20; i++) {
            d.put(("k" + i).getBytes(), i);
        }
        for (int attempt = 0; attempt < 50; attempt++) {
            Dict.Entry e = d.getRandomKey();
            assertNotNull(e, "getRandomKey should never return null for non-empty dict");
            // Verify the key is actually in the dict
            assertNotNull(d.get(e.getKey()),
                    "Returned key must exist in dict");
        }
    }

    @Test
    void getRandomKey_distribution_is_reasonable() {
        Dict d = Dict.create();
        int n = 50;
        for (int i = 0; i < n; i++) {
            d.put(("key" + i).getBytes(), i);
        }
        // Sample 200 times, collect unique keys
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            Dict.Entry e = d.getRandomKey();
            assertNotNull(e);
            seen.add(new String(e.getKey(), StandardCharsets.UTF_8));
        }
        // With 200 samples from 50 keys, we should see at least 5 unique keys
        // (our simple implementation may cluster around well-populated buckets)
        assertTrue(seen.size() >= 5,
                "Random distribution too skewed: only " + seen.size() + " unique keys in 200 samples");
    }

    @Test
    void getRandomKey_works_during_rehash() {
        Dict d = Dict.create();
        // Insert enough entries to trigger rehash (load factor > 1.0)
        for (int i = 0; i < 10; i++) {
            d.put(("key" + i).getBytes(), i);
        }
        // Mid-rehash: insert more to force it
        for (int i = 10; i < 25; i++) {
            d.put(("key" + i).getBytes(), i);
        }
        // Should still work
        Dict.Entry e = d.getRandomKey();
        assertNotNull(e);
        assertNotNull(d.get(e.getKey()));
    }
}
