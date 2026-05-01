package com.redisimpl.core.sds;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SdsTest {

    @Test
    void fromString_createsCorrectSds() {
        Sds s = Sds.fromString("hello");
        assertEquals(5, s.length());
        assertEquals("hello", s.toStr());
        assertFalse(s.isEmpty());
    }

    @Test
    void fromBytes_createsCorrectSds() {
        byte[] bytes = "world".getBytes(StandardCharsets.UTF_8);
        Sds s = Sds.fromBytes(bytes);
        assertEquals(5, s.length());
        assertArrayEquals(bytes, s.toBytes());
    }

    @Test
    void empty_sds() {
        Sds s = Sds.empty();
        assertEquals(0, s.length());
        assertTrue(s.isEmpty());
        assertEquals("", s.toStr());
    }

    @Test
    void append_toEmpty() {
        Sds s = Sds.empty();
        Sds result = s.append("hello".getBytes(StandardCharsets.UTF_8));
        assertEquals("hello", result.toStr());
        assertEquals(5, result.length());
    }

    @Test
    void append_toExisting() {
        Sds s = Sds.fromString("hello");
        Sds result = s.append(" world".getBytes(StandardCharsets.UTF_8));
        assertEquals("hello world", result.toStr());
        assertEquals(11, result.length());
    }

    @Test
    void append_doesNotMutateOriginal() {
        Sds original = Sds.fromString("hello");
        original.append(" world".getBytes(StandardCharsets.UTF_8));
        // original should be unchanged (immutable semantics)
        assertEquals("hello", original.toStr());
    }

    @Test
    void append_growsCapacity_smallString() {
        // For len < 1MB, capacity should double
        Sds s = Sds.fromString("a");
        Sds result = s.append("b".getBytes(StandardCharsets.UTF_8));
        assertEquals("ab", result.toStr());
        // alloc should be >= len
        assertTrue(result.getAlloc() >= result.length());
    }

    @Test
    void sdsrange_positiveIndices() {
        Sds s = Sds.fromString("hello world");
        Sds result = s.sdsrange(0, 4);
        assertEquals("hello", result.toStr());
    }

    @Test
    void sdsrange_negativeIndices() {
        Sds s = Sds.fromString("hello world");
        // -5 to -1 = "world"
        Sds result = s.sdsrange(-5, -1);
        assertEquals("world", result.toStr());
    }

    @Test
    void sdsrange_outOfBounds_returnsEmpty() {
        Sds s = Sds.fromString("hello");
        Sds result = s.sdsrange(10, 20);
        assertEquals("", result.toStr());
    }

    @Test
    void sdsgrowzero_expandsAndFillsZero() {
        Sds s = Sds.fromString("hi");
        Sds result = s.sdsgrowzero(5);
        assertEquals(5, result.length());
        byte[] bytes = result.toBytes();
        assertEquals('h', bytes[0]);
        assertEquals('i', bytes[1]);
        assertEquals(0, bytes[2]);
        assertEquals(0, bytes[3]);
        assertEquals(0, bytes[4]);
    }

    @Test
    void sdsgrowzero_shorterThanCurrent_returnsUnchanged() {
        Sds s = Sds.fromString("hello");
        Sds result = s.sdsgrowzero(3);
        // growzero with smaller len should not truncate
        assertEquals(5, result.length());
        assertEquals("hello", result.toStr());
    }

    @Test
    void toStr_utf8() {
        String chinese = "你好";
        Sds s = Sds.fromString(chinese);
        assertEquals(chinese, s.toStr());
    }

    @Test
    void length_returnsCorrectLength() {
        Sds s = Sds.fromString("12345");
        assertEquals(5, s.length());
    }

    @Test
    void alloc_atLeastLength() {
        Sds s = Sds.fromString("hello");
        assertTrue(s.getAlloc() >= s.length());
    }

    @Test
    void append_largeData_growsCorrectly() {
        // Build a 1MB string
        byte[] mb = new byte[1024 * 1024];
        java.util.Arrays.fill(mb, (byte) 'x');
        Sds s = Sds.fromBytes(mb);
        byte[] extra = new byte[1024];
        java.util.Arrays.fill(extra, (byte) 'y');
        Sds result = s.append(extra);
        assertEquals(mb.length + extra.length, result.length());
        // After 1MB, growth should be +1MB (not double)
        assertTrue(result.getAlloc() <= result.length() + 1024 * 1024 + 1);
    }
}
