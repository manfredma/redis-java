package com.redisimpl.core.listpack;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ListPackTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void create_isEmpty() {
        ListPack lp = ListPack.create();
        assertEquals(0, lp.size());
        assertTrue(lp.toList().isEmpty());
    }

    @Test
    void append_singleElement() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("hello"));
        assertEquals(1, lp.size());
        assertArrayEquals(bytes("hello"), lp.get(0));
    }

    @Test
    void append_multipleElements() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("a"));
        lp = lp.append(bytes("b"));
        lp = lp.append(bytes("c"));
        assertEquals(3, lp.size());
        assertArrayEquals(bytes("a"), lp.get(0));
        assertArrayEquals(bytes("b"), lp.get(1));
        assertArrayEquals(bytes("c"), lp.get(2));
    }

    @Test
    void prepend_singleElement() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("b"));
        lp = lp.prepend(bytes("a"));
        assertEquals(2, lp.size());
        assertArrayEquals(bytes("a"), lp.get(0));
        assertArrayEquals(bytes("b"), lp.get(1));
    }

    @Test
    void insert_atIndex() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("a"));
        lp = lp.append(bytes("c"));
        lp = lp.insert(1, bytes("b"));
        assertEquals(3, lp.size());
        assertArrayEquals(bytes("a"), lp.get(0));
        assertArrayEquals(bytes("b"), lp.get(1));
        assertArrayEquals(bytes("c"), lp.get(2));
    }

    @Test
    void insert_atBeginning() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("b"));
        lp = lp.insert(0, bytes("a"));
        assertEquals(2, lp.size());
        assertArrayEquals(bytes("a"), lp.get(0));
        assertArrayEquals(bytes("b"), lp.get(1));
    }

    @Test
    void insert_atEnd() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("a"));
        lp = lp.insert(1, bytes("b"));
        assertEquals(2, lp.size());
        assertArrayEquals(bytes("b"), lp.get(1));
    }

    @Test
    void delete_firstElement() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("a"));
        lp = lp.append(bytes("b"));
        lp = lp.append(bytes("c"));
        lp = lp.delete(0);
        assertEquals(2, lp.size());
        assertArrayEquals(bytes("b"), lp.get(0));
        assertArrayEquals(bytes("c"), lp.get(1));
    }

    @Test
    void delete_lastElement() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("a"));
        lp = lp.append(bytes("b"));
        lp = lp.delete(1);
        assertEquals(1, lp.size());
        assertArrayEquals(bytes("a"), lp.get(0));
    }

    @Test
    void delete_middleElement() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("a"));
        lp = lp.append(bytes("b"));
        lp = lp.append(bytes("c"));
        lp = lp.delete(1);
        assertEquals(2, lp.size());
        assertArrayEquals(bytes("a"), lp.get(0));
        assertArrayEquals(bytes("c"), lp.get(1));
    }

    @Test
    void toList_returnsAllElements() {
        ListPack lp = ListPack.create();
        lp = lp.append(bytes("x"));
        lp = lp.append(bytes("y"));
        lp = lp.append(bytes("z"));
        List<byte[]> list = lp.toList();
        assertEquals(3, list.size());
        assertArrayEquals(bytes("x"), list.get(0));
        assertArrayEquals(bytes("y"), list.get(1));
        assertArrayEquals(bytes("z"), list.get(2));
    }

    @Test
    void get_outOfBounds_throwsException() {
        ListPack lp = ListPack.create();
        final ListPack lp1 = lp.append(bytes("a"));
        assertThrows(IndexOutOfBoundsException.class, () -> lp1.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> lp1.get(-1));
    }

    @Test
    void immutability_appendDoesNotMutate() {
        ListPack lp1 = ListPack.create();
        lp1 = lp1.append(bytes("a"));
        ListPack lp2 = lp1.append(bytes("b"));
        assertEquals(1, lp1.size());
        assertEquals(2, lp2.size());
    }

    @Test
    void largeElements_handled() {
        // Element > 64 bytes (but ListPack still stores it)
        byte[] big = new byte[100];
        java.util.Arrays.fill(big, (byte) 'X');
        ListPack lp = ListPack.create();
        lp = lp.append(big);
        assertEquals(1, lp.size());
        assertArrayEquals(big, lp.get(0));
    }

    @Test
    void manyElements() {
        ListPack lp = ListPack.create();
        for (int i = 0; i < 128; i++) {
            lp = lp.append(bytes("item" + i));
        }
        assertEquals(128, lp.size());
        assertArrayEquals(bytes("item0"), lp.get(0));
        assertArrayEquals(bytes("item127"), lp.get(127));
    }

    @Test
    void emptyBytes_element() {
        ListPack lp = ListPack.create();
        lp = lp.append(new byte[0]);
        assertEquals(1, lp.size());
        assertArrayEquals(new byte[0], lp.get(0));
    }
}
