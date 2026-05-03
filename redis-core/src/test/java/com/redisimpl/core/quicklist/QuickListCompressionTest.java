package com.redisimpl.core.quicklist;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for QuickList LZF compression — mirrors quicklist.c node compression.
 */
class QuickListCompressionTest {

    @Test
    void no_compression_by_default() {
        QuickList ql = QuickList.create();
        for (int i = 0; i < 5; i++) {
            ql = ql.rpush(("element" + i).getBytes());
        }
        // Default compress=0: no compression
        assertEquals(0, ql.getCompress());
        assertEquals(5, ql.llen());
    }

    @Test
    void compress_1_data_integrity_preserved() {
        // Use fill=5 (count-based) + compress=1
        // Verify all data survives applyCompression via range() which auto-decompresses
        QuickList ql = QuickList.create(5, 1);

        for (int i = 0; i < 30; i++) {
            ql = ql.rpush(("item-" + i).getBytes());
        }
        ql = ql.applyCompression();

        assertEquals(30, ql.llen(), "All elements must be present after compression");

        // Verify data via range() which handles decompression internally
        List<byte[]> all = ql.range(0, -1);
        assertEquals(30, all.size());
        assertEquals("item-0", new String(all.get(0)));
        assertEquals("item-29", new String(all.get(29)));
        assertEquals("item-15", new String(all.get(15)));
    }

    @Test
    void compression_does_not_lose_data() {
        QuickList ql = QuickList.create(-1, 1); // 4096 bytes/node, compress=1

        // Add large elements to force multiple nodes
        for (int i = 0; i < 100; i++) {
            byte[] elem = ("element-" + i + "-" + new String(new char[40]).replace("\0", "x")).getBytes();
            ql = ql.rpush(elem);
        }
        ql = ql.applyCompression();

        // All elements should be accessible
        List<byte[]> all = ql.range(0, -1);
        assertEquals(100, all.size());
        for (int i = 0; i < 100; i++) {
            String expected = "element-" + i + "-" + new String(new char[40]).replace("\0", "x");
            assertEquals(expected, new String(all.get(i)),
                    "Element " + i + " corrupted after compression");
        }
    }

    @Test
    void fill_minus_2_default_8192_bytes_per_node() {
        QuickList ql = QuickList.create(-2, 0);
        assertEquals(-2, ql.getFill());

        // 100-byte elements should fit many per node (8192/100 ≈ 81)
        for (int i = 0; i < 50; i++) {
            ql = ql.rpush((new String(new char[100]).replace("\0", "x")).getBytes());
        }
        assertEquals(50, ql.llen());
    }

    @Test
    void fill_positive_count_based() {
        // fill=5 means max 5 entries per node
        QuickList ql = QuickList.create(5, 0);
        assertEquals(5, ql.getFill());

        for (int i = 0; i < 20; i++) {
            ql = ql.rpush(("elem" + i).getBytes());
        }
        assertEquals(20, ql.llen());
        // Data integrity
        assertEquals("elem0", new String(ql.index(0)));
        assertEquals("elem19", new String(ql.index(19)));
    }

    @Test
    void node_exceeds_limit_negative_fill() {
        // fill=-1 = 4096 bytes per node (optimization_level[0] = 4096)
        // nodeByteSize=50 + elemSz=10 + overhead=8 = 68 < 4096 → does NOT exceed
        assertFalse(QuickList.nodeExceedsLimit(-1, 50, 1, 10),
                "68 bytes should NOT exceed 4096 limit");
        // nodeByteSize=4000 + elemSz=100 + overhead=8 = 4108 > 4096 → exceeds
        assertTrue(QuickList.nodeExceedsLimit(-1, 4000, 1, 100),
                "4108 bytes should exceed 4096 limit");
    }

    @Test
    void node_exceeds_limit_positive_fill() {
        // fill=3 = max 3 entries
        assertFalse(QuickList.nodeExceedsLimit(3, 0, 2, 10),
                "2 entries < 3 limit: should NOT exceed");
        assertTrue(QuickList.nodeExceedsLimit(3, 0, 3, 10),
                "3 entries >= 3 limit: should exceed");
    }

    @Test
    void lrange_after_compression_correct() {
        QuickList ql = QuickList.create(-1, 1);
        for (int i = 0; i < 30; i++) {
            ql = ql.rpush(("item" + i).getBytes());
        }
        ql = ql.applyCompression();

        List<byte[]> range = ql.range(5, 15);
        assertEquals(11, range.size());
        assertEquals("item5", new String(range.get(0)));
        assertEquals("item15", new String(range.get(10)));
    }
}
