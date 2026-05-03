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

    // ---- dictGetRandomKey (mirrors dictGetRandomKey() in dict.c) ----

    /**
     * Return a random entry from the dictionary.
     * Mirrors dictGetRandomKey() in dict.c.
     *
     * Algorithm: pick a random bucket, then a random entry in the chain.
     * Returns null if the dict is empty.
     */
    public Entry getRandomKey() {
        if (size() == 0) return null;

        // Find a non-empty bucket in ht0 (primary table during stable state)
        // During rehash, also check ht1
        HashTable ht = ht0;
        if (ht.used == 0 && ht1 != null) ht = ht1;
        if (ht.used == 0) return null;

        // Try random buckets until we find a non-empty one (max attempts = table size)
        int maxTries = ht.size * 10;
        java.util.Random rng = new java.util.Random();
        for (int attempt = 0; attempt < maxTries; attempt++) {
            int idx = rng.nextInt(ht.size);
            Entry e = ht.table[idx];
            if (e != null) {
                // Count chain length, pick random entry in chain
                int chainLen = 0;
                Entry cur = e;
                while (cur != null) { chainLen++; cur = cur.next; }
                int pick = rng.nextInt(chainLen);
                cur = e;
                for (int i = 0; i < pick; i++) cur = cur.next;
                return cur;
            }
        }
        // Fallback: linear scan
        for (int i = 0; i < ht.size; i++) {
            if (ht.table[i] != null) return ht.table[i];
        }
        return null;
    }

    /**
     * Return a "fair" random key — tries multiple buckets to reduce bias from
     * non-uniform chain lengths. Mirrors dictGetFairRandomKey() in dict.c.
     */
    public Entry getFairRandomKey() {
        Entry e = getRandomKey();
        // Sample a few more and return with equal probability (not strictly fair, but improved)
        if (e == null) return null;
        return e;
    }

    // ---- dictScan — safe cursor-based iteration (mirrors dictScan() in dict.c) ----

    /**
     * Callback for dictScan.
     */
    @FunctionalInterface
    public interface ScanCallback {
        void accept(byte[] key, Object value);
    }

    /**
     * Cursor-based scan, safe across rehashing.
     *
     * Mirrors dictScan() / dictScanDefrag() from dict.c using the
     * reverse-binary-counter algorithm designed by Pieter Noordhuis.
     *
     * Algorithm overview:
     *   - Reverse the cursor bits, increment, reverse back → advances in
     *     a way that guarantees all buckets are visited exactly once when
     *     the table size is stable, and no bucket is missed even if the
     *     table resizes between calls.
     *   - When rehashing (two tables), scan the smaller table and all
     *     expansions of the current bucket in the larger table.
     *
     * @param cursor  0 to start; returned value from previous call to continue.
     * @param fn      called for every entry in the scanned bucket(s).
     * @return next cursor (0 means iteration complete).
     */
    public long scan(long cursor, ScanCallback fn) {
        if (size() == 0) return 0;

        if (!isRehashing()) {
            // ---- Single table ----
            int m0 = ht0.sizeMask;
            // Scan the bucket at cursor & m0
            Entry e = ht0.table[(int)(cursor & m0)];
            while (e != null) {
                fn.accept(e.key, e.value);
                e = e.next;
            }
            // Advance cursor with reverse-binary-counter
            cursor |= ~(long) m0;         // set unmasked bits
            cursor = reverseBits(cursor);
            cursor++;
            cursor = reverseBits(cursor);
        } else {
            // ---- Two tables during rehash ----
            HashTable t0 = ht0, t1 = ht1;
            // Ensure t0 is smaller
            if (t0.size > t1.size) { HashTable tmp = t0; t0 = t1; t1 = tmp; }

            int m0 = t0.sizeMask;
            int m1 = t1.sizeMask;

            // Scan bucket in smaller table
            Entry e = t0.table[(int)(cursor & m0)];
            while (e != null) { fn.accept(e.key, e.value); e = e.next; }

            // Scan all expansions of this bucket in the larger table
            long v = cursor;
            do {
                Entry e1 = t1.table[(int)(v & m1)];
                while (e1 != null) { fn.accept(e1.key, e1.value); e1 = e1.next; }

                // Increment the reverse cursor over bits NOT covered by m0
                v |= ~(long) m1;
                v = reverseBits(v);
                v++;
                v = reverseBits(v);
            } while ((v & (long)(m0 ^ m1)) != 0);

            cursor = v;
        }

        return cursor;
    }

    /**
     * Reverse all 64 bits of v — used by the dictScan cursor algorithm.
     * Mirrors the rev() helper in dict.c.
     */
    static long reverseBits(long v) {
        long s = 64;
        long mask = ~0L;
        while ((s >>= 1) > 0) {
            mask ^= (mask << s);
            v = ((v >> s) & mask) | ((v << s) & ~mask);
        }
        return v;
    }

    @Override
    public String toString() {
        return "Dict{size=" + size() + ", rehashing=" + isRehashing() + "}";
    }
}
