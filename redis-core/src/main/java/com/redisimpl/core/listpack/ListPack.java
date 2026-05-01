package com.redisimpl.core.listpack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ListPack — Java port of Redis's listpack.c.
 *
 * <p>A compact, append-friendly list that stores variable-length byte arrays.
 * This implementation uses a simple ArrayList-based approach that preserves
 * the semantics of Redis's listpack (ordered, indexed, supports prepend/append/insert/delete).
 *
 * <p>Immutable semantics: all mutating operations return a new ListPack.
 *
 * <p>Note: The actual Redis listpack uses a binary encoding with:
 *   - 4-byte total-bytes header
 *   - 2-byte num-elements header
 *   - Entries with back-length for reverse traversal
 *   - 0xFF terminator
 * This implementation abstracts that into a Java ArrayList for clarity,
 * while preserving the same API contract.
 */
public final class ListPack {

    /** Maximum entries before conversion to QuickList */
    public static final int LIST_MAX_LISTPACK_SIZE = 128;
    /** Maximum element size before conversion to QuickList */
    public static final int LIST_MAX_LISTPACK_VALUE = 64;

    /** Internal storage: each entry is a byte array */
    private final List<byte[]> entries;

    private ListPack(List<byte[]> entries) {
        this.entries = entries;
    }

    // ---- Factory ----

    public static ListPack create() {
        return new ListPack(new ArrayList<>());
    }

    // ---- Core operations ----

    /**
     * Append an element to the end. Returns a new ListPack.
     */
    public ListPack append(byte[] element) {
        List<byte[]> newEntries = copyEntries();
        newEntries.add(cloneBytes(element));
        return new ListPack(newEntries);
    }

    /**
     * Prepend an element to the beginning. Returns a new ListPack.
     */
    public ListPack prepend(byte[] element) {
        List<byte[]> newEntries = new ArrayList<>(entries.size() + 1);
        newEntries.add(cloneBytes(element));
        for (byte[] e : entries) newEntries.add(cloneBytes(e));
        return new ListPack(newEntries);
    }

    /**
     * Insert an element at the given index. Returns a new ListPack.
     * Index 0 inserts at beginning, index size() inserts at end.
     */
    public ListPack insert(int index, byte[] element) {
        if (index < 0 || index > entries.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + entries.size());
        }
        List<byte[]> newEntries = new ArrayList<>(entries.size() + 1);
        for (int i = 0; i < index; i++) newEntries.add(cloneBytes(entries.get(i)));
        newEntries.add(cloneBytes(element));
        for (int i = index; i < entries.size(); i++) newEntries.add(cloneBytes(entries.get(i)));
        return new ListPack(newEntries);
    }

    /**
     * Delete element at the given index. Returns a new ListPack.
     */
    public ListPack delete(int index) {
        checkIndex(index);
        List<byte[]> newEntries = new ArrayList<>(entries.size() - 1);
        for (int i = 0; i < entries.size(); i++) {
            if (i != index) newEntries.add(cloneBytes(entries.get(i)));
        }
        return new ListPack(newEntries);
    }

    /**
     * Replace element at the given index. Returns a new ListPack.
     */
    public ListPack set(int index, byte[] element) {
        checkIndex(index);
        List<byte[]> newEntries = copyEntries();
        newEntries.set(index, cloneBytes(element));
        return new ListPack(newEntries);
    }

    // ---- Accessors ----

    /**
     * Get element at index. Throws IndexOutOfBoundsException if out of range.
     */
    public byte[] get(int index) {
        checkIndex(index);
        return cloneBytes(entries.get(index));
    }

    /**
     * Number of elements.
     */
    public int size() {
        return entries.size();
    }

    /**
     * Return all elements as a new list (defensive copies).
     */
    public List<byte[]> toList() {
        List<byte[]> result = new ArrayList<>(entries.size());
        for (byte[] e : entries) result.add(cloneBytes(e));
        return result;
    }

    /**
     * Find the first occurrence of {@code element} (byte-by-byte comparison).
     * Returns index or -1 if not found.
     */
    public int indexOf(byte[] element) {
        for (int i = 0; i < entries.size(); i++) {
            if (Arrays.equals(entries.get(i), element)) return i;
        }
        return -1;
    }

    /**
     * Find the last occurrence of {@code element}.
     * Returns index or -1 if not found.
     */
    public int lastIndexOf(byte[] element) {
        for (int i = entries.size() - 1; i >= 0; i--) {
            if (Arrays.equals(entries.get(i), element)) return i;
        }
        return -1;
    }

    // ---- Internal helpers ----

    private void checkIndex(int index) {
        if (index < 0 || index >= entries.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + entries.size());
        }
    }

    private List<byte[]> copyEntries() {
        List<byte[]> copy = new ArrayList<>(entries.size() + 1);
        for (byte[] e : entries) copy.add(cloneBytes(e));
        return copy;
    }

    private static byte[] cloneBytes(byte[] b) {
        if (b == null) return new byte[0];
        return b.clone();
    }

    @Override
    public String toString() {
        return "ListPack{size=" + entries.size() + "}";
    }
}
