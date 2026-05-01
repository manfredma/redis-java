package com.redisimpl.core.intset;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntSetTest {

    @Test
    void create_empty() {
        IntSet is = IntSet.create();
        assertEquals(0, is.length());
        assertEquals(IntSet.INTSET_ENC_INT16, is.getEncoding());
    }

    @Test
    void add_smallIntegers_staysInt16() {
        IntSet is = IntSet.create();
        is = is.add(1);
        is = is.add(2);
        is = is.add(3);
        assertEquals(3, is.length());
        assertEquals(IntSet.INTSET_ENC_INT16, is.getEncoding());
        assertTrue(is.contains(1));
        assertTrue(is.contains(2));
        assertTrue(is.contains(3));
        assertFalse(is.contains(4));
    }

    @Test
    void add_duplicate_doesNotIncrease() {
        IntSet is = IntSet.create();
        is = is.add(5);
        is = is.add(5);
        assertEquals(1, is.length());
    }

    @Test
    void add_int32Value_upgradesEncoding() {
        IntSet is = IntSet.create();
        is = is.add(1);
        is = is.add(2);
        // Short.MAX_VALUE + 1 = 32768, exceeds int16
        is = is.add(Short.MAX_VALUE + 1L);
        assertEquals(IntSet.INTSET_ENC_INT32, is.getEncoding());
        assertEquals(3, is.length());
        assertTrue(is.contains(Short.MAX_VALUE + 1L));
    }

    @Test
    void add_int64Value_upgradesEncoding() {
        IntSet is = IntSet.create();
        is = is.add(1);
        // Integer.MAX_VALUE + 1 exceeds int32
        is = is.add((long) Integer.MAX_VALUE + 1L);
        assertEquals(IntSet.INTSET_ENC_INT64, is.getEncoding());
        assertEquals(2, is.length());
        assertTrue(is.contains((long) Integer.MAX_VALUE + 1L));
    }

    @Test
    void remove_existingElement() {
        IntSet is = IntSet.create();
        is = is.add(1);
        is = is.add(2);
        is = is.add(3);
        is = is.remove(2);
        assertEquals(2, is.length());
        assertFalse(is.contains(2));
        assertTrue(is.contains(1));
        assertTrue(is.contains(3));
    }

    @Test
    void remove_nonExistingElement_noChange() {
        IntSet is = IntSet.create();
        is = is.add(1);
        is = is.remove(99);
        assertEquals(1, is.length());
    }

    @Test
    void toArray_returnsSortedArray() {
        IntSet is = IntSet.create();
        is = is.add(5);
        is = is.add(3);
        is = is.add(1);
        is = is.add(4);
        is = is.add(2);
        long[] arr = is.toArray();
        assertEquals(5, arr.length);
        for (int i = 0; i < arr.length - 1; i++) {
            assertTrue(arr[i] < arr[i + 1]);
        }
    }

    @Test
    void contains_negativeNumbers() {
        IntSet is = IntSet.create();
        is = is.add(-100);
        is = is.add(0);
        is = is.add(100);
        assertTrue(is.contains(-100));
        assertTrue(is.contains(0));
        assertTrue(is.contains(100));
        assertFalse(is.contains(-99));
    }

    @Test
    void add_int16Boundaries() {
        IntSet is = IntSet.create();
        is = is.add(Short.MIN_VALUE);
        is = is.add(Short.MAX_VALUE);
        assertEquals(IntSet.INTSET_ENC_INT16, is.getEncoding());
        assertTrue(is.contains(Short.MIN_VALUE));
        assertTrue(is.contains(Short.MAX_VALUE));
    }

    @Test
    void add_int32Boundaries() {
        IntSet is = IntSet.create();
        is = is.add(Integer.MIN_VALUE);
        is = is.add(Integer.MAX_VALUE);
        assertEquals(IntSet.INTSET_ENC_INT32, is.getEncoding());
        assertTrue(is.contains(Integer.MIN_VALUE));
        assertTrue(is.contains(Integer.MAX_VALUE));
    }

    @Test
    void add_int64Boundaries() {
        IntSet is = IntSet.create();
        is = is.add(Long.MIN_VALUE);
        is = is.add(Long.MAX_VALUE);
        assertEquals(IntSet.INTSET_ENC_INT64, is.getEncoding());
        assertTrue(is.contains(Long.MIN_VALUE));
        assertTrue(is.contains(Long.MAX_VALUE));
    }

    @Test
    void binarySearch_correctness() {
        IntSet is = IntSet.create();
        for (int i = 0; i < 100; i++) {
            is = is.add(i * 2); // even numbers 0..198
        }
        // All even numbers present
        for (int i = 0; i < 100; i++) {
            assertTrue(is.contains(i * 2));
        }
        // Odd numbers absent
        for (int i = 0; i < 100; i++) {
            assertFalse(is.contains(i * 2 + 1));
        }
    }

    @Test
    void immutability_addDoesNotMutateOriginal() {
        IntSet is1 = IntSet.create();
        is1 = is1.add(1);
        IntSet is2 = is1.add(2);
        assertEquals(1, is1.length());
        assertEquals(2, is2.length());
    }
}
