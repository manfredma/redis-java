package com.redisimpl.core.zskiplist;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ZSkipListTest {

    private ZSkipList zsl;

    private static byte[] e(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @BeforeEach
    void setUp() {
        zsl = new ZSkipList();
    }

    @Test
    void empty_skiplist() {
        assertEquals(0, zsl.length());
    }

    @Test
    void insert_singleElement() {
        zsl.insert(1.0, e("a"));
        assertEquals(1, zsl.length());
    }

    @Test
    void insert_multipleElements() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        zsl.insert(3.0, e("c"));
        assertEquals(3, zsl.length());
    }

    @Test
    void delete_existingElement() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        boolean deleted = zsl.delete(1.0, e("a"));
        assertTrue(deleted);
        assertEquals(1, zsl.length());
    }

    @Test
    void delete_nonExistentElement() {
        zsl.insert(1.0, e("a"));
        boolean deleted = zsl.delete(99.0, e("x"));
        assertFalse(deleted);
        assertEquals(1, zsl.length());
    }

    @Test
    void rank_singleElement() {
        zsl.insert(1.0, e("a"));
        assertEquals(1, zsl.rank(1.0, e("a")));
    }

    @Test
    void rank_multipleElements_ascending() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        zsl.insert(3.0, e("c"));
        assertEquals(1, zsl.rank(1.0, e("a")));
        assertEquals(2, zsl.rank(2.0, e("b")));
        assertEquals(3, zsl.rank(3.0, e("c")));
    }

    @Test
    void rank_notFound_returnsZero() {
        zsl.insert(1.0, e("a"));
        assertEquals(0, zsl.rank(99.0, e("x")));
    }

    @Test
    void rangeByRank_full() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        zsl.insert(3.0, e("c"));
        List<ZSkipListNode> result = zsl.rangeByRank(1, 3);
        assertEquals(3, result.size());
        assertEquals(1.0, result.get(0).getScore());
        assertEquals(2.0, result.get(1).getScore());
        assertEquals(3.0, result.get(2).getScore());
    }

    @Test
    void rangeByRank_partial() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        zsl.insert(3.0, e("c"));
        List<ZSkipListNode> result = zsl.rangeByRank(2, 3);
        assertEquals(2, result.size());
        assertEquals(2.0, result.get(0).getScore());
    }

    @Test
    void rangeByScore_inclusive() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        zsl.insert(3.0, e("c"));
        zsl.insert(4.0, e("d"));
        List<ZSkipListNode> result = zsl.rangeByScore(2.0, 3.0, false, false);
        assertEquals(2, result.size());
        assertEquals(2.0, result.get(0).getScore());
        assertEquals(3.0, result.get(1).getScore());
    }

    @Test
    void rangeByScore_exclusive() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        zsl.insert(3.0, e("c"));
        zsl.insert(4.0, e("d"));
        List<ZSkipListNode> result = zsl.rangeByScore(1.0, 4.0, true, true);
        assertEquals(2, result.size());
        assertEquals(2.0, result.get(0).getScore());
        assertEquals(3.0, result.get(1).getScore());
    }

    @Test
    void count_byScore() {
        zsl.insert(1.0, e("a"));
        zsl.insert(2.0, e("b"));
        zsl.insert(3.0, e("c"));
        assertEquals(2, zsl.count(1.0, 2.0));
        assertEquals(3, zsl.count(1.0, 3.0));
        assertEquals(0, zsl.count(5.0, 10.0));
    }

    @Test
    void sameScore_differentElements_orderedByLex() {
        zsl.insert(1.0, e("b"));
        zsl.insert(1.0, e("a"));
        zsl.insert(1.0, e("c"));
        List<ZSkipListNode> result = zsl.rangeByRank(1, 3);
        assertEquals(3, result.size());
        // Lexicographic order: a, b, c
        assertArrayEquals(e("a"), result.get(0).getEle());
        assertArrayEquals(e("b"), result.get(1).getEle());
        assertArrayEquals(e("c"), result.get(2).getEle());
    }

    @Test
    void rangeByLex_inclusive() {
        zsl.insert(0.0, e("a"));
        zsl.insert(0.0, e("b"));
        zsl.insert(0.0, e("c"));
        zsl.insert(0.0, e("d"));
        zsl.insert(0.0, e("e"));
        List<ZSkipListNode> result = zsl.rangeByLex("[b", "[d");
        assertEquals(3, result.size());
        assertArrayEquals(e("b"), result.get(0).getEle());
        assertArrayEquals(e("c"), result.get(1).getEle());
        assertArrayEquals(e("d"), result.get(2).getEle());
    }

    @Test
    void rangeByLex_exclusive() {
        zsl.insert(0.0, e("a"));
        zsl.insert(0.0, e("b"));
        zsl.insert(0.0, e("c"));
        zsl.insert(0.0, e("d"));
        List<ZSkipListNode> result = zsl.rangeByLex("(a", "(d");
        assertEquals(2, result.size());
        assertArrayEquals(e("b"), result.get(0).getEle());
        assertArrayEquals(e("c"), result.get(1).getEle());
    }

    @Test
    void rangeByLex_minusAndPlus() {
        zsl.insert(0.0, e("a"));
        zsl.insert(0.0, e("b"));
        zsl.insert(0.0, e("c"));
        List<ZSkipListNode> result = zsl.rangeByLex("-", "+");
        assertEquals(3, result.size());
    }

    @Test
    void manyElements_correctOrder() {
        for (int i = 100; i >= 1; i--) {
            zsl.insert(i, e("item" + i));
        }
        assertEquals(100, zsl.length());
        List<ZSkipListNode> result = zsl.rangeByRank(1, 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(i + 1.0, result.get(i).getScore(), 0.0001);
        }
    }
}
