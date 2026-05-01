package com.redisimpl.core.quicklist;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QuickListTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void create_empty() {
        QuickList ql = QuickList.create();
        assertEquals(0, ql.llen());
    }

    @Test
    void rpush_and_llen() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        assertEquals(3, ql.llen());
    }

    @Test
    void lpush_and_llen() {
        QuickList ql = QuickList.create();
        ql = ql.lpush(b("c"));
        ql = ql.lpush(b("b"));
        ql = ql.lpush(b("a"));
        assertEquals(3, ql.llen());
    }

    @Test
    void rpush_lpop_order() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        QuickList.PopResult r1 = ql.lpopResult();
        assertEquals("a", s(r1.value));
        QuickList.PopResult r2 = r1.list.lpopResult();
        assertEquals("b", s(r2.value));
        QuickList.PopResult r3 = r2.list.lpopResult();
        assertEquals("c", s(r3.value));
    }

    @Test
    void lpush_rpop_order() {
        QuickList ql = QuickList.create();
        ql = ql.lpush(b("a"));
        ql = ql.lpush(b("b"));
        ql = ql.lpush(b("c"));
        QuickList.PopResult r1 = ql.rpopResult();
        assertEquals("a", s(r1.value));
        QuickList.PopResult r2 = r1.list.rpopResult();
        assertEquals("b", s(r2.value));
        QuickList.PopResult r3 = r2.list.rpopResult();
        assertEquals("c", s(r3.value));
    }

    @Test
    void lpop_empty_returnsNull() {
        QuickList ql = QuickList.create();
        assertNull(ql.lpop());
    }

    @Test
    void rpop_empty_returnsNull() {
        QuickList ql = QuickList.create();
        assertNull(ql.rpop());
    }

    @Test
    void index_positiveIndex() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        assertEquals("a", s(ql.index(0)));
        assertEquals("b", s(ql.index(1)));
        assertEquals("c", s(ql.index(2)));
    }

    @Test
    void index_negativeIndex() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        assertEquals("c", s(ql.index(-1)));
        assertEquals("b", s(ql.index(-2)));
        assertEquals("a", s(ql.index(-3)));
    }

    @Test
    void index_outOfBounds_returnsNull() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        assertNull(ql.index(5));
        assertNull(ql.index(-5));
    }

    @Test
    void range_full() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        List<byte[]> result = ql.range(0, -1);
        assertEquals(3, result.size());
        assertEquals("a", s(result.get(0)));
        assertEquals("b", s(result.get(1)));
        assertEquals("c", s(result.get(2)));
    }

    @Test
    void range_partial() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        ql = ql.rpush(b("d"));
        List<byte[]> result = ql.range(1, 2);
        assertEquals(2, result.size());
        assertEquals("b", s(result.get(0)));
        assertEquals("c", s(result.get(1)));
    }

    @Test
    void range_negativeIndices() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        List<byte[]> result = ql.range(-2, -1);
        assertEquals(2, result.size());
        assertEquals("b", s(result.get(0)));
        assertEquals("c", s(result.get(1)));
    }

    @Test
    void linsert_before() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("c"));
        ql = ql.linsert(b("c"), true, b("b"));
        assertEquals(3, ql.llen());
        assertEquals("b", s(ql.index(1)));
    }

    @Test
    void linsert_after() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("c"));
        ql = ql.linsert(b("a"), false, b("b"));
        assertEquals(3, ql.llen());
        assertEquals("b", s(ql.index(1)));
    }

    @Test
    void linsert_pivotNotFound_returnsUnchanged() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        QuickList result = ql.linsert(b("x"), true, b("b"));
        assertNull(result); // returns null when pivot not found
    }

    @Test
    void lset_changesElement() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("c"));
        ql = ql.lset(1, b("B"));
        assertEquals("B", s(ql.index(1)));
    }

    @Test
    void lrem_removesCount() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("a"));
        QuickList.LremResult res = ql.lremResult(2, b("a"));
        assertEquals(2, res.removed);
        assertEquals(2, res.list.llen());
    }

    @Test
    void lrem_negativeCount_removesFromTail() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("a"));
        QuickList.LremResult res = ql.lremResult(-1, b("a"));
        assertEquals(1, res.removed);
        assertEquals(3, res.list.llen());
        // last "a" should be removed
        assertEquals("a", s(res.list.index(2)));
    }

    @Test
    void lrem_zeroCount_removesAll() {
        QuickList ql = QuickList.create();
        ql = ql.rpush(b("a"));
        ql = ql.rpush(b("b"));
        ql = ql.rpush(b("a"));
        QuickList.LremResult res = ql.lremResult(0, b("a"));
        assertEquals(2, res.removed);
        assertEquals(1, res.list.llen());
    }

    @Test
    void manyElements_acrossMultipleNodes() {
        QuickList ql = QuickList.create();
        // Insert more than LIST_MAX_LISTPACK_SIZE elements
        for (int i = 0; i < 200; i++) {
            ql = ql.rpush(b("item" + i));
        }
        assertEquals(200, ql.llen());
        assertEquals("item0", s(ql.index(0)));
        assertEquals("item199", s(ql.index(199)));
        assertEquals("item100", s(ql.index(100)));
    }

    @Test
    void lmove_leftToLeft() {
        QuickList src = QuickList.create();
        src = src.rpush(b("a"));
        src = src.rpush(b("b"));
        QuickList dst = QuickList.create();
        dst = dst.rpush(b("c"));
        // Move from src left to dst left
        byte[] moved = src.lpop();
        dst = dst.lpush(moved);
        assertEquals("a", s(dst.index(0)));
    }
}
