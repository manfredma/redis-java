package com.redisimpl.core.listpack;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ListPackTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- Helper: read little-endian uint32 ----
    private static long readUint32LE(byte[] data, int off) {
        return ((data[off] & 0xFFL))
                | ((data[off + 1] & 0xFFL) << 8)
                | ((data[off + 2] & 0xFFL) << 16)
                | ((data[off + 3] & 0xFFL) << 24);
    }

    private static int readUint16LE(byte[] data, int off) {
        return (data[off] & 0xFF) | ((data[off + 1] & 0xFF) << 8);
    }

    // =========================================================
    // 1. Binary format: empty listpack = 7 bytes
    // =========================================================

    @Test
    void emptyListPack_binaryFormat() {
        ListPack lp = ListPack.create();
        byte[] raw = lp.getBytes();

        // total size = 7 (6 header + 1 EOF)
        assertEquals(7, raw.length);
        // bytes [0-3] = total-bytes = 7 (LE)
        assertEquals(7L, readUint32LE(raw, 0));
        // bytes [4-5] = num-elements = 0 (LE)
        assertEquals(0, readUint16LE(raw, 4));
        // byte [6] = 0xFF (EOF)
        assertEquals(0xFF, raw[6] & 0xFF);
    }

    @Test
    void totalBytes_numElements_accessors() {
        ListPack lp = ListPack.create();
        assertEquals(7, lp.totalBytes());
        assertEquals(0, lp.numElements());
    }

    // =========================================================
    // 2. Integer encoding types
    // =========================================================

    @Test
    void appendInteger_7bit_encoding() {
        // 7BIT_UINT: 0xxxxxxx, value 0-127
        ListPack lp = ListPack.create().appendInteger(0);
        byte[] raw = lp.getBytes();
        // Entry: [0x00][backlen=1] + header(6) + EOF(1) = 9 bytes
        assertEquals(9, raw.length);
        assertEquals(0x00, raw[6] & 0xFF);   // encoding byte = 0
        assertEquals(1, raw[7] & 0xFF);      // backlen = 1
        assertEquals(0xFF, raw[8] & 0xFF);   // EOF

        lp = ListPack.create().appendInteger(127);
        raw = lp.getBytes();
        assertEquals(9, raw.length);
        assertEquals(0x7F, raw[6] & 0xFF);   // 0111_1111
        assertEquals(1, raw[7] & 0xFF);
    }

    @Test
    void appendInteger_13bit_encoding() {
        // 13BIT_INT: 110xxxxx xxxxxxxx, range -4096..4095 (excluding 0..127)
        ListPack lp = ListPack.create().appendInteger(128);
        byte[] raw = lp.getBytes();
        // Entry: 2 encoding bytes + backlen(1) = 3 bytes
        // total = 6 + 3 + 1 = 10
        assertEquals(10, raw.length);
        // encoding: 0xC0 | (128 >> 8) = 0xC0, then 128 & 0xFF = 0x80
        assertEquals(0xC0, raw[6] & 0xFF);
        assertEquals(0x80, raw[7] & 0xFF);
        assertEquals(2, raw[8] & 0xFF);      // backlen = 2
        assertEquals(0xFF, raw[9] & 0xFF);   // EOF

        // -1: in 13-bit two's complement: v = (1<<13) + (-1) = 8191 = 0x1FFF
        // enc[0] = (8191>>8)|0xC0 = 0x1F|0xC0 = 0xDF
        // enc[1] = 8191 & 0xFF = 0xFF
        lp = ListPack.create().appendInteger(-1);
        raw = lp.getBytes();
        assertEquals(10, raw.length);
        assertEquals(0xDF, raw[6] & 0xFF);
        assertEquals(0xFF, raw[7] & 0xFF);
        assertEquals(2, raw[8] & 0xFF);
    }

    @Test
    void appendInteger_16bit_encoding() {
        // 16BIT_INT: 0xF1 + 2 bytes LE, range -32768..32767 (excl 13-bit range)
        ListPack lp = ListPack.create().appendInteger(4096);
        byte[] raw = lp.getBytes();
        // Entry: 3 encoding bytes + backlen(1) = 4 bytes
        assertEquals(11, raw.length);
        assertEquals(0xF1, raw[6] & 0xFF);
        assertEquals(0x00, raw[7] & 0xFF);  // 4096 & 0xFF = 0
        assertEquals(0x10, raw[8] & 0xFF);  // 4096 >> 8 = 0x10
        assertEquals(3, raw[9] & 0xFF);     // backlen = 3
        assertEquals(0xFF, raw[10] & 0xFF);

        lp = ListPack.create().appendInteger(-32768);
        raw = lp.getBytes();
        // -32768 as unsigned 16-bit: (1<<16)+(-32768) = 32768 = 0x8000
        assertEquals(0xF1, raw[6] & 0xFF);
        assertEquals(0x00, raw[7] & 0xFF);  // 0x8000 & 0xFF = 0
        assertEquals(0x80, raw[8] & 0xFF);  // 0x8000 >> 8 = 0x80
    }

    @Test
    void appendInteger_24bit_encoding() {
        // 24BIT_INT: 0xF2 + 3 bytes LE, range -8388608..8388607 (excl 16-bit)
        ListPack lp = ListPack.create().appendInteger(32768);
        byte[] raw = lp.getBytes();
        // Entry: 4 encoding bytes + backlen(1) = 5 bytes
        assertEquals(12, raw.length);
        assertEquals(0xF2, raw[6] & 0xFF);
        assertEquals(0x00, raw[7] & 0xFF);  // 32768 & 0xFF = 0
        assertEquals(0x80, raw[8] & 0xFF);  // (32768>>8) & 0xFF = 0x80
        assertEquals(0x00, raw[9] & 0xFF);  // 32768>>16 = 0
        assertEquals(4, raw[10] & 0xFF);    // backlen = 4
    }

    @Test
    void appendInteger_32bit_encoding() {
        // 32BIT_INT: 0xF3 + 4 bytes LE, range -2147483648..2147483647 (excl 24-bit)
        ListPack lp = ListPack.create().appendInteger(8388608L);
        byte[] raw = lp.getBytes();
        // Entry: 5 encoding bytes + backlen(1) = 6 bytes
        assertEquals(13, raw.length);
        assertEquals(0xF3, raw[6] & 0xFF);
        assertEquals(0x00, raw[7] & 0xFF);
        assertEquals(0x00, raw[8] & 0xFF);
        assertEquals(0x80, raw[9] & 0xFF);  // 8388608 = 0x800000 -> byte2 = 0x80
        assertEquals(0x00, raw[10] & 0xFF);
        assertEquals(5, raw[11] & 0xFF);    // backlen = 5
    }

    @Test
    void appendInteger_64bit_encoding() {
        // 64BIT_INT: 0xF4 + 8 bytes LE
        long val = 2147483648L; // > INT32_MAX
        ListPack lp = ListPack.create().appendInteger(val);
        byte[] raw = lp.getBytes();
        // Entry: 9 encoding bytes + backlen(1) = 10 bytes
        assertEquals(17, raw.length);
        assertEquals(0xF4, raw[6] & 0xFF);
        // 2147483648 = 0x80000000
        assertEquals(0x00, raw[7] & 0xFF);
        assertEquals(0x00, raw[8] & 0xFF);
        assertEquals(0x00, raw[9] & 0xFF);
        assertEquals(0x80, raw[10] & 0xFF);
        assertEquals(0x00, raw[11] & 0xFF);
        assertEquals(0x00, raw[12] & 0xFF);
        assertEquals(0x00, raw[13] & 0xFF);
        assertEquals(0x00, raw[14] & 0xFF);
        assertEquals(9, raw[15] & 0xFF);    // backlen = 9
    }

    // =========================================================
    // 3. String encoding types
    // =========================================================

    @Test
    void append_6bitStr_encoding() {
        // 6BIT_STR: 10xxxxxx, len < 64
        ListPack lp = ListPack.create().append(bytes("hi"));
        byte[] raw = lp.getBytes();
        // Entry: 1 enc byte + 2 data bytes + backlen(1 for enclen=3) = 4 bytes
        // total = 6 + 4 + 1 = 11
        assertEquals(11, raw.length);
        // enc byte: 0x80 | 2 = 0x82
        assertEquals(0x82, raw[6] & 0xFF);
        assertEquals('h', raw[7] & 0xFF);
        assertEquals('i', raw[8] & 0xFF);
        assertEquals(3, raw[9] & 0xFF);     // backlen = enclen = 1+2 = 3
        assertEquals(0xFF, raw[10] & 0xFF);
    }

    @Test
    void append_12bitStr_encoding() {
        // 12BIT_STR: 1110xxxx xxxxxxxx, 64 <= len < 4096
        byte[] data = new byte[64];
        java.util.Arrays.fill(data, (byte) 'A');
        ListPack lp = ListPack.create().append(data);
        byte[] raw = lp.getBytes();
        // Entry: 2 enc bytes + 64 data bytes + backlen(1 for enclen=66)
        // enclen = 2+64 = 66, backlen(66) = 1 byte
        // total = 6 + 2 + 64 + 1 + 1 = 74
        assertEquals(74, raw.length);
        // enc[0] = 0xE0 | (64>>8) = 0xE0
        assertEquals(0xE0, raw[6] & 0xFF);
        // enc[1] = 64 & 0xFF = 0x40
        assertEquals(0x40, raw[7] & 0xFF);
        // backlen = 66 (fits in 1 byte since 66 <= 127)
        assertEquals(66, raw[6 + 2 + 64] & 0xFF);
        assertEquals(0xFF, raw[73] & 0xFF);
    }

    @Test
    void append_32bitStr_encoding() {
        // 32BIT_STR: 0xF0 + 4 bytes LE len, len >= 4096
        byte[] data = new byte[4096];
        java.util.Arrays.fill(data, (byte) 'B');
        ListPack lp = ListPack.create().append(data);
        byte[] raw = lp.getBytes();
        // Entry: 5 enc bytes + 4096 data bytes + backlen(?)
        // enclen = 5+4096 = 4101
        // backlen(4101): 4101 >= 2097151? no. 4101 >= 16383? no. 4101 >= 128? yes -> 2 bytes
        // total = 6 + 5 + 4096 + 2 + 1 = 4110
        assertEquals(4110, raw.length);
        assertEquals(0xF0, raw[6] & 0xFF);
        // len = 4096 = 0x00001000 in LE
        assertEquals(0x00, raw[7] & 0xFF);
        assertEquals(0x10, raw[8] & 0xFF);
        assertEquals(0x00, raw[9] & 0xFF);
        assertEquals(0x00, raw[10] & 0xFF);
    }

    // =========================================================
    // 4. Backlen encoding (multi-byte)
    // =========================================================

    @Test
    void backlen_twoBytes_when_enclenOver127() {
        // enclen = 128 -> backlen needs 2 bytes
        // 12BIT_STR with len=126: enclen = 2+126 = 128
        byte[] data = new byte[126];
        java.util.Arrays.fill(data, (byte) 'X');
        ListPack lp = ListPack.create().append(data);
        byte[] raw = lp.getBytes();
        // Entry: 2+126 = 128 enc+data, backlen(128) = 2 bytes
        // total = 6 + 128 + 2 + 1 = 137
        assertEquals(137, raw.length);
        // backlen bytes at raw[6+128] and raw[6+128+1]
        // For l=128: buf[0]=128>>7=1, buf[1]=(128&127)|128 = 0|128 = 128
        int bl0 = raw[6 + 128] & 0xFF;
        int bl1 = raw[6 + 128 + 1] & 0xFF;
        assertEquals(1, bl0);
        assertEquals(128, bl1);
    }

    // =========================================================
    // 5. next / prev traversal
    // =========================================================

    @Test
    void first_next_traversal() {
        ListPack lp = ListPack.create()
                .append(bytes("a"))
                .append(bytes("b"))
                .append(bytes("c"));

        int pos = lp.first();
        assertNotEquals(-1, pos);
        assertArrayEquals(bytes("a"), lp.getAt(pos));

        pos = lp.next(pos);
        assertNotEquals(-1, pos);
        assertArrayEquals(bytes("b"), lp.getAt(pos));

        pos = lp.next(pos);
        assertNotEquals(-1, pos);
        assertArrayEquals(bytes("c"), lp.getAt(pos));

        pos = lp.next(pos);
        assertEquals(-1, pos);
    }

    @Test
    void prev_traversal() {
        ListPack lp = ListPack.create()
                .append(bytes("x"))
                .append(bytes("y"))
                .append(bytes("z"));

        // walk to last
        int pos = lp.first();
        while (true) {
            int n = lp.next(pos);
            if (n == -1) break;
            pos = n;
        }
        assertArrayEquals(bytes("z"), lp.getAt(pos));

        pos = lp.prev(pos);
        assertArrayEquals(bytes("y"), lp.getAt(pos));

        pos = lp.prev(pos);
        assertArrayEquals(bytes("x"), lp.getAt(pos));

        assertEquals(-1, lp.prev(pos));
    }

    @Test
    void first_emptyListPack_returnsMinusOne() {
        assertEquals(-1, ListPack.create().first());
    }

    // =========================================================
    // 6. getAt / getLongAt
    // =========================================================

    @Test
    void getLongAt_integer_entry() {
        ListPack lp = ListPack.create().appendInteger(42);
        int pos = lp.first();
        assertEquals(42L, lp.getLongAt(pos));
    }

    @Test
    void getLongAt_string_that_is_integer() {
        ListPack lp = ListPack.create().append(bytes("123"));
        int pos = lp.first();
        // "123" will be encoded as 7BIT_UINT (fits 0-127), so getLongAt returns 123
        assertEquals(123L, lp.getLongAt(pos));
    }

    // =========================================================
    // 7. delete / insert / replace
    // =========================================================

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
    void replace_element() {
        ListPack lp = ListPack.create()
                .append(bytes("a"))
                .append(bytes("b"))
                .append(bytes("c"));
        lp = lp.replace(1, bytes("B"));
        assertEquals(3, lp.size());
        assertArrayEquals(bytes("a"), lp.get(0));
        assertArrayEquals(bytes("B"), lp.get(1));
        assertArrayEquals(bytes("c"), lp.get(2));
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
        // Element > 64 bytes uses 12BIT_STR encoding
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

    // =========================================================
    // 8. fromBytes round-trip
    // =========================================================

    @Test
    void fromBytes_roundTrip_strings() {
        ListPack original = ListPack.create()
                .append(bytes("hello"))
                .append(bytes("world"))
                .append(bytes("foo"));
        byte[] raw = original.getBytes();
        ListPack restored = ListPack.fromBytes(raw);
        assertEquals(original.size(), restored.size());
        for (int i = 0; i < original.size(); i++) {
            assertArrayEquals(original.get(i), restored.get(i),
                    "Mismatch at index " + i);
        }
    }

    @Test
    void fromBytes_roundTrip_integers() {
        ListPack original = ListPack.create()
                .appendInteger(0)
                .appendInteger(127)
                .appendInteger(128)
                .appendInteger(-1)
                .appendInteger(4096)
                .appendInteger(-32768)
                .appendInteger(32768)
                .appendInteger(-8388608)
                .appendInteger(8388608)
                .appendInteger(-2147483648L)
                .appendInteger(2147483648L)
                .appendInteger(Long.MIN_VALUE)
                .appendInteger(Long.MAX_VALUE);
        byte[] raw = original.getBytes();
        ListPack restored = ListPack.fromBytes(raw);
        assertEquals(original.size(), restored.size());
        for (int i = 0; i < original.size(); i++) {
            assertArrayEquals(original.get(i), restored.get(i),
                    "Mismatch at index " + i);
        }
    }

    @Test
    void fromBytes_roundTrip_mixed() {
        ListPack original = ListPack.create()
                .append(bytes("key"))
                .appendInteger(42)
                .append(bytes("value"))
                .appendInteger(-100);
        byte[] raw = original.getBytes();
        ListPack restored = ListPack.fromBytes(raw);
        assertEquals(original.size(), restored.size());
        for (int i = 0; i < original.size(); i++) {
            assertArrayEquals(original.get(i), restored.get(i));
        }
    }

    // =========================================================
    // 9. getLong (by index) — integer-encoded entries
    // =========================================================

    @Test
    void getLong_integerEncoded() {
        ListPack lp = ListPack.create()
                .appendInteger(42)
                .appendInteger(-100)
                .appendInteger(Long.MAX_VALUE);
        assertEquals(42L, lp.getLong(0));
        assertEquals(-100L, lp.getLong(1));
        assertEquals(Long.MAX_VALUE, lp.getLong(2));
    }

    @Test
    void getLong_stringEncodedNumber() {
        // "42" stored as string (if not integer-parseable by lpStringToInt64 rules,
        // but "42" is parseable so it gets stored as 7BIT_UINT)
        ListPack lp = ListPack.create().append(bytes("42"));
        assertEquals(42L, lp.getLong(0));
    }

    @Test
    void getLong_nonNumericString_throwsNumberFormatException() {
        ListPack lp = ListPack.create().append(bytes("hello"));
        assertThrows(NumberFormatException.class, () -> lp.getLong(0));
    }

    // =========================================================
    // 10. Header fields after mutations
    // =========================================================

    @Test
    void totalBytes_updatedAfterAppend() {
        ListPack lp = ListPack.create();
        assertEquals(7, lp.totalBytes());
        lp = lp.append(bytes("hi")); // 6 + (1+2+1) + 1 = 11
        assertEquals(11, lp.totalBytes());
    }

    @Test
    void numElements_updatedAfterAppend() {
        ListPack lp = ListPack.create();
        assertEquals(0, lp.numElements());
        lp = lp.append(bytes("a"));
        assertEquals(1, lp.numElements());
        lp = lp.append(bytes("b"));
        assertEquals(2, lp.numElements());
        lp = lp.delete(0);
        assertEquals(1, lp.numElements());
    }

    // =========================================================
    // 11. Integer string encoding: lpStringToInt64 strict rules
    // =========================================================

    @Test
    void stringEncoding_leadingZero_notInteger() {
        // "01" has leading zero, should be stored as 6BIT_STR not integer
        ListPack lp = ListPack.create().append(bytes("01"));
        byte[] raw = lp.getBytes();
        // Should be 6BIT_STR: enc byte = 0x80|2 = 0x82
        assertEquals(0x82, raw[6] & 0xFF);
    }

    @Test
    void stringEncoding_emptyString_notInteger() {
        ListPack lp = ListPack.create().append(new byte[0]);
        byte[] raw = lp.getBytes();
        // 6BIT_STR with len=0: enc byte = 0x80
        assertEquals(0x80, raw[6] & 0xFF);
        assertEquals(1, raw[7] & 0xFF); // backlen = enclen = 1
    }

    @Test
    void stringEncoding_singleZero_isInteger() {
        // "0" is valid integer: stored as 7BIT_UINT
        ListPack lp = ListPack.create().append(bytes("0"));
        byte[] raw = lp.getBytes();
        assertEquals(0x00, raw[6] & 0xFF); // 7BIT_UINT encoding of 0
    }

    @Test
    void stringEncoding_negativeSign_only_notInteger() {
        // "-" alone is not a valid integer
        ListPack lp = ListPack.create().append(bytes("-"));
        byte[] raw = lp.getBytes();
        // 6BIT_STR: 0x80 | 1 = 0x81
        assertEquals(0x81, raw[6] & 0xFF);
    }

    // =========================================================
    // 12. indexOf (legacy API compatibility)
    // =========================================================

    @Test
    void indexOf_found() {
        ListPack lp = ListPack.create()
                .append(bytes("a"))
                .append(bytes("b"))
                .append(bytes("c"));
        assertEquals(1, lp.indexOf(bytes("b")));
        assertEquals(-1, lp.indexOf(bytes("z")));
    }

    // =========================================================
    // 13. set (alias for replace) — legacy API
    // =========================================================

    @Test
    void set_replacesElement() {
        ListPack lp = ListPack.create()
                .append(bytes("a"))
                .append(bytes("b"));
        lp = lp.set(0, bytes("A"));
        assertArrayEquals(bytes("A"), lp.get(0));
        assertArrayEquals(bytes("b"), lp.get(1));
    }

    // =========================================================
    // 14. Negative index in get (should throw)
    // =========================================================

    @Test
    void get_negativeIndex_throwsException() {
        ListPack lp = ListPack.create().append(bytes("a"));
        assertThrows(IndexOutOfBoundsException.class, () -> lp.get(-1));
    }

    // =========================================================
    // 15. Binary compatibility: specific byte-level checks
    // =========================================================

    @Test
    void binaryFormat_twoStrings() {
        // append "a" then "b"
        // "a" -> 7BIT_UINT? No: 'a' = 97, which is > 0 and <= 127. lpStringToInt64("a"...) fails
        // because 'a' is not a digit. So "a" stored as 6BIT_STR.
        // enc[0] = 0x80|1 = 0x81, data='a', backlen=2
        // "b" -> same: enc[0]=0x81, data='b', backlen=2
        // Total: 6 + (1+1+1) + (1+1+1) + 1 = 13
        ListPack lp = ListPack.create().append(bytes("a")).append(bytes("b"));
        byte[] raw = lp.getBytes();
        assertEquals(13, raw.length);
        assertEquals(13L, readUint32LE(raw, 0));
        assertEquals(2, readUint16LE(raw, 4));
        // entry 1: offset 6
        assertEquals(0x81, raw[6] & 0xFF);
        assertEquals('a', raw[7] & 0xFF);
        assertEquals(2, raw[8] & 0xFF);
        // entry 2: offset 9
        assertEquals(0x81, raw[9] & 0xFF);
        assertEquals('b', raw[10] & 0xFF);
        assertEquals(2, raw[11] & 0xFF);
        assertEquals(0xFF, raw[12] & 0xFF);
    }

    @Test
    void binaryFormat_integerZero() {
        // "0" -> lpStringToInt64 returns 1 with value=0 -> 7BIT_UINT
        ListPack lp = ListPack.create().append(bytes("0"));
        byte[] raw = lp.getBytes();
        // total = 6 + 1 + 1 + 1 = 9
        assertEquals(9, raw.length);
        assertEquals(0x00, raw[6] & 0xFF); // 7BIT_UINT(0)
        assertEquals(1, raw[7] & 0xFF);    // backlen
        assertEquals(0xFF, raw[8] & 0xFF);
    }
}
