package com.redisimpl.core.dict;

import java.util.*;

/**
 * Dict — Java port of Redis's dict.c.
 *
 * <p>A hash table with incremental (progressive) rehashing.
 * Uses two internal hash tables (ht[0] and ht[1]).
 * During rehash, lookups and inserts check both tables.
 * Each operation migrates one bucket from ht[0] to ht[1].
 *
 * <p>Keys are byte arrays; values are arbitrary Objects.
 * Key comparison is byte-by-byte (Arrays.equals).
 */
public final class Dict implements Iterable<Dict.Entry> {

    /** Load factor threshold to trigger resize */
    private static final double LOAD_FACTOR = 1.0;

    /** Minimum hash table size */
    private static final int DICT_HT_INITIAL_SIZE = 4;

    /** Entry in the hash table */
    public static final class Entry {
        private final byte[] key;
        private Object value;
        private Entry next; // chaining

        Entry(byte[] key, Object value) {
            this.key = key.clone();
            this.value = value;
            this.next = null;
        }

        public byte[] getKey() { return key.clone(); }
        public Object getValue() { return value; }
        void setValue(Object value) { this.value = value; }
    }

    /** Single hash table */
    private static class HashTable {
        Entry[] table;
        int size;     // capacity (power of 2)
        int sizeMask; // size - 1
        int used;     // number of entries

        HashTable(int size) {
            this.size = size;
            this.sizeMask = size - 1;
            this.table = new Entry[size];
            this.used = 0;
        }

        static HashTable empty() {
            return new HashTable(0) {
                @Override
                public String toString() { return "HashTable{empty}"; }
            };
        }
    }

    private HashTable ht0;
    private HashTable ht1;
    private int rehashIdx; // -1 = not rehashing

    private Dict() {
        this.ht0 = new HashTable(DICT_HT_INITIAL_SIZE);
        this.ht1 = null;
        this.rehashIdx = -1;
    }

    public static Dict create() {
        return new Dict();
    }

    // ---- Core operations ----

    /**
     * Insert or update a key-value pair.
     */
    public void put(byte[] key, Object value) {
        if (isRehashing()) rehashStep();

        // Check if key exists (update)
        Entry existing = findEntry(key);
        if (existing != null) {
            existing.setValue(value);
            return;
        }

        // Check if we need to resize
        if (!isRehashing() && needsResize()) {
            startRehash();
        }

        // Insert into ht1 if rehashing, else ht0
        HashTable target = isRehashing() ? ht1 : ht0;
        int h = hash(key) & target.sizeMask;
        Entry newEntry = new Entry(key, value);
        newEntry.next = target.table[h];
        target.table[h] = newEntry;
        target.used++;
    }

    /**
     * Get value for key. Returns null if not found.
     */
    public Object get(byte[] key) {
        if (isRehashing()) rehashStep();
        Entry entry = findEntry(key);
        return entry != null ? entry.value : null;
    }

    /**
     * Delete a key. Returns true if the key existed.
     */
    public boolean delete(byte[] key) {
        if (isRehashing()) rehashStep();

        for (int t = 0; t <= (isRehashing() ? 1 : 0); t++) {
            HashTable ht = (t == 0) ? ht0 : ht1;
            if (ht == null || ht.size == 0) continue;
            int h = hash(key) & ht.sizeMask;
            Entry prev = null;
            Entry curr = ht.table[h];
            while (curr != null) {
                if (Arrays.equals(curr.key, key)) {
                    if (prev == null) ht.table[h] = curr.next;
                    else prev.next = curr.next;
                    ht.used--;
                    // Finish rehash if ht0 is empty
                    if (isRehashing() && ht0.used == 0) finishRehash();
                    return true;
                }
                prev = curr;
                curr = curr.next;
            }
        }
        return false;
    }

    /**
     * Check if key exists.
     */
    public boolean containsKey(byte[] key) {
        if (isRehashing()) rehashStep();
        return findEntry(key) != null;
    }

    /**
     * Number of entries across both tables.
     */
    public int size() {
        int count = ht0.used;
        if (ht1 != null) count += ht1.used;
        return count;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Return all keys (defensive copies).
     */
    public Set<byte[]> keySet() {
        Set<byte[]> keys = new HashSet<>();
        for (Entry e : this) {
            keys.add(e.getKey());
        }
        return keys;
    }

    /**
     * Return all values.
     */
    public Collection<Object> values() {
        List<Object> vals = new ArrayList<>();
        for (Entry e : this) {
            vals.add(e.getValue());
        }
        return vals;
    }

    // ---- Rehash ----

    public boolean isRehashing() {
        return rehashIdx >= 0;
    }

    /**
     * Migrate one bucket from ht0 to ht1.
     */
    public void rehashStep() {
        rehashStep(1);
    }

    /**
     * Migrate {@code n} buckets from ht0 to ht1.
     */
    public void rehashStep(int n) {
        if (!isRehashing()) return;
        int emptyVisits = n * 10; // max empty buckets to visit
        while (n > 0 && ht0.used > 0) {
            while (ht0.table[rehashIdx] == null) {
                rehashIdx++;
                if (--emptyVisits == 0) return;
                if (rehashIdx >= ht0.size) {
                    finishRehash();
                    return;
                }
            }
            // Move all entries in this bucket
            Entry entry = ht0.table[rehashIdx];
            while (entry != null) {
                Entry next = entry.next;
                int h = hash(entry.key) & ht1.sizeMask;
                entry.next = ht1.table[h];
                ht1.table[h] = entry;
                ht0.used--;
                ht1.used++;
                entry = next;
            }
            ht0.table[rehashIdx] = null;
            rehashIdx++;
            n--;
            if (rehashIdx >= ht0.size) {
                finishRehash();
                return;
            }
        }
        if (ht0.used == 0) finishRehash();
    }

    private void startRehash() {
        int newSize = nextPowerOf2(ht0.used * 2);
        ht1 = new HashTable(newSize);
        rehashIdx = 0;
    }

    private void finishRehash() {
        ht0 = ht1;
        ht1 = null;
        rehashIdx = -1;
    }

    private boolean needsResize() {
        if (ht0.size == 0) return false;
        return (double) ht0.used / ht0.size >= LOAD_FACTOR;
    }

    // ---- Internal helpers ----

    private Entry findEntry(byte[] key) {
        for (int t = 0; t <= (isRehashing() ? 1 : 0); t++) {
            HashTable ht = (t == 0) ? ht0 : ht1;
            if (ht == null || ht.size == 0) continue;
            int h = hash(key) & ht.sizeMask;
            Entry entry = ht.table[h];
            while (entry != null) {
                if (Arrays.equals(entry.key, key)) return entry;
                entry = entry.next;
            }
        }
        return null;
    }

    /**
     * SipHash-inspired hash for byte arrays.
     * Using a simple but effective FNV-1a variant for Java 8 compatibility.
     */
    private static int hash(byte[] key) {
        if (key == null || key.length == 0) return 0;
        int h = 0x811c9dc5; // FNV offset basis
        for (byte b : key) {
            h ^= (b & 0xFF);
            h *= 0x01000193; // FNV prime
        }
        return h & Integer.MAX_VALUE; // ensure non-negative
    }

    private static int nextPowerOf2(int n) {
        if (n <= DICT_HT_INITIAL_SIZE) return DICT_HT_INITIAL_SIZE;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        return n + 1;
    }

    // ---- Iterator ----

    @Override
    public Iterator<Entry> iterator() {
        return new DictIterator();
    }

    private class DictIterator implements Iterator<Entry> {
        private int tableIdx = 0; // 0 = ht0, 1 = ht1
        private int bucketIdx = 0;
        private Entry current = null;

        DictIterator() {
            advance();
        }

        private void advance() {
            while (true) {
                if (current != null && current.next != null) {
                    current = current.next;
                    return;
                }
                // Move to next bucket
                HashTable ht = tableIdx == 0 ? ht0 : ht1;
                if (ht == null || ht.size == 0) {
                    if (tableIdx == 0 && ht1 != null) {
                        tableIdx = 1;
                        bucketIdx = 0;
                        ht = ht1;
                    } else {
                        current = null;
                        return;
                    }
                }
                while (bucketIdx < ht.size) {
                    if (ht.table[bucketIdx] != null) {
                        current = ht.table[bucketIdx];
                        bucketIdx++;
                        return;
                    }
                    bucketIdx++;
                }
                // Exhausted this table
                if (tableIdx == 0 && ht1 != null) {
                    tableIdx = 1;
                    bucketIdx = 0;
                } else {
                    current = null;
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Entry next() {
            if (current == null) throw new NoSuchElementException();
            Entry result = current;
            advance();
            return result;
        }
    }

    @Override
    public String toString() {
        return "Dict{size=" + size() + ", rehashing=" + isRehashing() + "}";
    }
}
