package com.redisimpl.core.quicklist;

import com.redisimpl.core.listpack.ListPack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * QuickList — Java port of Redis's quicklist.c.
 *
 * A doubly-linked list where each node contains a {@link ListPack}.
 * Node capacity is controlled by {@code fill} and {@code compress} parameters
 * that mirror quicklist.c exactly:
 *
 * fill (mirrors quicklistSetFill):
 *   fill > 0  → max entry count per node (clamped to FILL_MAX=32767)
 *   fill == 0 → 1 entry per node
 *   fill < 0  → size-based limit using optimization_level[] (−1..−5):
 *               −1: 4096 bytes, −2: 8192, −3: 16384, −4: 32768, −5: 65536
 *   Default: −2 (mirrors quicklistCreate() setting fill = -2)
 *
 * compress (mirrors quicklistSetCompressDepth):
 *   0 → no compression; N → compress all nodes except N from each end.
 *   Java implementation: node compression is noted but not byte-level LZF,
 *   since we don't have a C-compatible LZF node format here.
 *
 * SIZE_ESTIMATE_OVERHEAD = 8 (mirrors quicklist.c constant)
 * SIZE_SAFETY_LIMIT = 8192
 */
public final class QuickList {

    // ---- Fill / compress constants (mirrors quicklist.c) ----
    public static final int FILL_MAX     = (1 << 14) - 1; // 16383 (QL_FILL_BITS=15, max = 2^14-1)
    public static final int COMPRESS_MAX = (1 << 10) - 1; // 1023 (QL_COMP_BITS=10)
    public static final int DEFAULT_FILL = -2;             // matches quicklistCreate()
    public static final int DEFAULT_COMPRESS = 0;

    // Optimization levels for size-based fill (mirrors optimization_level[] in quicklist.c)
    private static final long[] OPTIMIZATION_LEVEL = {4096, 8192, 16384, 32768, 65536};
    private static final long SIZE_SAFETY_LIMIT    = 8192;
    private static final long SIZE_ESTIMATE_OVERHEAD = 8;

    // Legacy compatibility constants (kept for callers that use them directly)
    /** @deprecated Use fill parameter instead */
    @Deprecated
    public static final int QUICKLIST_NODE_MAX_ENTRIES = 128;
    /** @deprecated Use fill parameter instead */
    @Deprecated
    public static final int QUICKLIST_NODE_MAX_VALUE   = 64;

    // ---- Compression constants (mirrors quicklist.c) ----
    private static final int MIN_COMPRESS_BYTES   = 48;
    private static final int MIN_COMPRESS_IMPROVE = 8;

    /** Doubly-linked node — mirrors quicklistNode in quicklist.h */
    static final class Node {
        ListPack pack;          // uncompressed data (null when compressed)
        byte[] compressed;      // LZF-compressed bytes (null when raw)
        int uncompressedSize;   // original byte size (for decompression)
        boolean isCompressed;   // QUICKLIST_NODE_ENCODING_LZF
        Node prev;
        Node next;

        Node(ListPack pack) {
            this.pack = pack;
            this.isCompressed = false;
        }

        /**
         * Compress this node using LZF — mirrors __quicklistCompressNode().
         * Only compresses if uncompressedSize >= MIN_COMPRESS_BYTES and
         * compression saves >= MIN_COMPRESS_IMPROVE bytes.
         */
        void compress() {
            if (isCompressed || pack == null) return;
            byte[] raw = pack.getBytes();
            if (raw.length < MIN_COMPRESS_BYTES) return;
            try {
                byte[] lzf = com.ning.compress.lzf.LZFEncoder.encode(raw);
                if (raw.length - lzf.length >= MIN_COMPRESS_IMPROVE) {
                    compressed = lzf;
                    uncompressedSize = raw.length;
                    pack = null;
                    isCompressed = true;
                }
            } catch (Exception ignored) {
                // LZF failed (data not compressible) — keep as RAW
            }
        }

        /**
         * Decompress this node — mirrors __quicklistDecompressNode().
         */
        void decompress() {
            if (!isCompressed || compressed == null) return;
            try {
                byte[] raw = com.ning.compress.lzf.LZFDecoder.decode(compressed);
                pack = ListPack.fromBytes(raw);
                compressed = null;
                isCompressed = false;
            } catch (Exception e) {
                throw new RuntimeException("QuickList LZF decompression failed", e);
            }
        }
    }

    final Node head;
    final Node tail;
    final long size;   // total element count
    final int fill;    // node fill parameter
    final int compress; // compress depth

    private QuickList(Node head, Node tail, long size, int fill, int compress) {
        this.head = head;
        this.tail = tail;
        this.size = size;
        this.fill = fill;
        this.compress = compress;
    }

    /** Create with default fill=-2, compress=0 (mirrors quicklistCreate()). */
    public static QuickList create() {
        return new QuickList(null, null, 0, DEFAULT_FILL, DEFAULT_COMPRESS);
    }

    /** Create with explicit fill and compress parameters. */
    public static QuickList create(int fill, int compress) {
        int f = (fill > FILL_MAX) ? FILL_MAX : (fill < -5) ? -5 : fill;
        int c = (compress > COMPRESS_MAX) ? COMPRESS_MAX : (compress < 0) ? 0 : compress;
        return new QuickList(null, null, 0, f, c);
    }

    public int getFill()     { return fill; }
    public int getCompress() { return compress; }

    // ---- Node size limit logic (mirrors quicklistNodeExceedsLimit) ----

    /**
     * Calculate size limit for a node given fill.
     * Mirrors quicklistNodeNegFillLimit() and quicklistNodeLimit().
     */
    private static long nodeSizeLimit(int fill) {
        if (fill < 0) {
            int offset = (-fill) - 1;
            if (offset >= OPTIMIZATION_LEVEL.length) offset = OPTIMIZATION_LEVEL.length - 1;
            return OPTIMIZATION_LEVEL[offset];
        }
        return Long.MAX_VALUE; // count-based, not size-based
    }

    private static int nodeCountLimit(int fill) {
        if (fill >= 0) return (fill == 0) ? 1 : fill;
        return Integer.MAX_VALUE; // size-based, not count-based
    }

    /**
     * Returns true if adding an element of {@code elemSz} bytes to a node
     * with current {@code nodeByteSize} and {@code nodeCount} would exceed limits.
     * Mirrors quicklistNodeExceedsLimit().
     */
    static boolean nodeExceedsLimit(int fill, long nodeByteSize, int nodeCount, int elemSz) {
        if (fill < 0) {
            long sizeLimit = nodeSizeLimit(fill);
            long newSz = nodeByteSize + elemSz + SIZE_ESTIMATE_OVERHEAD;
            return newSz > sizeLimit;
        } else {
            int countLimit = nodeCountLimit(fill);
            if (nodeByteSize + elemSz > SIZE_SAFETY_LIMIT) return true;
            return nodeCount >= countLimit;
        }
    }

    // ---- Push ----

    public QuickList rpush(byte[] value) {
        Mut m = new Mut(this);
        m.rpush(value, fill);
        return m.build().applyCompression();
    }

    public QuickList lpush(byte[] value) {
        Mut m = new Mut(this);
        m.lpush(value, fill);
        return m.build().applyCompression();
    }

    // ---- Pop ----

    /** Result of a pop operation: the popped value and the new list. */
    public static final class PopResult {
        public final byte[] value;
        public final QuickList list;
        PopResult(byte[] value, QuickList list) {
            this.value = value;
            this.list = list;
        }
    }

    public PopResult lpopResult() {
        if (head == null) return new PopResult(null, this);
        Mut m = new Mut(this);
        byte[] val = m.lpop();
        return new PopResult(val, m.build());
    }

    public PopResult rpopResult() {
        if (tail == null) return new PopResult(null, this);
        Mut m = new Mut(this);
        byte[] val = m.rpop();
        return new PopResult(val, m.build());
    }

    /** Convenience: pop and return value only (discards new list). */
    public byte[] lpop() {
        return lpopResult().value;
    }

    /** Convenience: pop and return value only (discards new list). */
    public byte[] rpop() {
        return rpopResult().value;
    }

    // ---- Access ----

    public byte[] index(long idx) {
        if (size == 0) return null;
        long actual = idx < 0 ? size + idx : idx;
        if (actual < 0 || actual >= size) return null;
        long pos = 0;
        Node n = head;
        while (n != null) {
            Mut.ensureDecompressed(n);
            int nodeSize = n.pack.size();
            if (pos + nodeSize > actual) {
                return n.pack.get((int) (actual - pos));
            }
            pos += nodeSize;
            n = n.next;
        }
        return null;
    }

    public List<byte[]> range(long start, long stop) {
        long len = size;
        if (len == 0) return new ArrayList<>();
        if (start < 0) start = Math.max(len + start, 0);
        if (stop < 0)  stop  = len + stop;
        if (start > stop || start >= len) return new ArrayList<>();
        stop = Math.min(stop, len - 1);

        List<byte[]> result = new ArrayList<>();
        long pos = 0;
        Node n = head;
        while (n != null && pos <= stop) {
            Mut.ensureDecompressed(n);
            int nodeSize = n.pack.size();
            for (int i = 0; i < nodeSize; i++) {
                long globalIdx = pos + i;
                if (globalIdx >= start && globalIdx <= stop) {
                    result.add(n.pack.get(i));
                }
            }
            pos += nodeSize;
            n = n.next;
        }
        return result;
    }

    public long llen() {
        return size;
    }

    // ---- Mutation operations ----

    /**
     * Insert {@code value} before or after {@code pivot}.
     * Returns null if pivot not found.
     */
    public QuickList linsert(byte[] pivot, boolean before, byte[] value) {
        Mut m = new Mut(this);
        boolean found = m.linsert(pivot, before, value);
        if (!found) return null;
        return m.build();
    }

    public QuickList lset(long idx, byte[] value) {
        long actual = idx < 0 ? size + idx : idx;
        if (actual < 0 || actual >= size) {
            throw new IndexOutOfBoundsException("Index out of range: " + idx);
        }
        Mut m = new Mut(this);
        m.lset(actual, value);
        return m.build();
    }

    /** Result of lrem operation */
    public static final class LremResult {
        public final long removed;
        public final QuickList list;
        LremResult(long removed, QuickList list) {
            this.removed = removed;
            this.list = list;
        }
    }

    /**
     * Remove occurrences of {@code value} and return both the count removed
     * and the new QuickList.
     * count > 0: from head; count < 0: from tail; count == 0: all.
     */
    public LremResult lremResult(long count, byte[] value) {
        Mut m = new Mut(this);
        long removed = m.lrem(count, value);
        return new LremResult(removed, m.build());
    }

    /**
     * Convenience: remove and return count only (discards new list).
     * For use when caller manages the QuickList reference separately.
     */
    public long lrem(long count, byte[] value) {
        return lremResult(count, value).removed;
    }

    // ---- Internal Mutable Helper ----

    /**
     * Mutable builder for QuickList operations.
     */
    static final class Mut {
        Node head;
        Node tail;
        long size;
        int fill;
        int compress;

        Mut(QuickList ql) {
            this.size = ql.size;
            this.fill = ql.fill;
            this.compress = ql.compress;
            if (ql.head == null) {
                this.head = null;
                this.tail = null;
                return;
            }
            // Deep-copy the node chain (ListPack is immutable, share references)
            Node srcNode = ql.head;
            Node firstNew = null;
            Node prevNew = null;
            while (srcNode != null) {
                Node newNode = new Node(srcNode.pack);
                if (firstNew == null) firstNew = newNode;
                if (prevNew != null) {
                    prevNew.next = newNode;
                    newNode.prev = prevNew;
                }
                prevNew = newNode;
                srcNode = srcNode.next;
            }
            this.head = firstNew;
            this.tail = prevNew;
        }

        /** Ensure node is decompressed before accessing its pack. */
        static void ensureDecompressed(Node n) {
            if (n != null && n.isCompressed) n.decompress();
        }

        void rpush(byte[] value, int fill) {
            if (tail == null) {
                Node newNode = new Node(ListPack.create().append(value));
                head = tail = newNode;
                size++;
            } else {
                ensureDecompressed(tail);
                if (nodeExceedsLimit(fill, tail.pack.totalBytes(), tail.pack.size(), value.length)) {
                    Node newNode = new Node(ListPack.create().append(value));
                    newNode.prev = tail;
                    tail.next = newNode;
                    tail = newNode;
                } else {
                    tail.pack = tail.pack.append(value);
                }
                size++;
            }
        }

        // Legacy overload for callers that don't pass fill
        void rpush(byte[] value) { rpush(value, DEFAULT_FILL); }

        void lpush(byte[] value, int fill) {
            if (head == null) {
                Node newNode = new Node(ListPack.create().append(value));
                head = tail = newNode;
                size++;
            } else {
                ensureDecompressed(head);
                if (nodeExceedsLimit(fill, head.pack.totalBytes(), head.pack.size(), value.length)) {
                    Node newNode = new Node(ListPack.create().append(value));
                    newNode.next = head;
                    head.prev = newNode;
                    head = newNode;
                } else {
                    head.pack = head.pack.prepend(value);
                }
                size++;
            }
        }

        void lpush(byte[] value) { lpush(value, DEFAULT_FILL); }

        byte[] lpop() {
            if (head == null) return null;
            ensureDecompressed(head);
            byte[] val = head.pack.get(0);
            head.pack = head.pack.delete(0);
            size--;
            if (head.pack.size() == 0) unlinkNode(head);
            return val;
        }

        byte[] rpop() {
            if (tail == null) return null;
            ensureDecompressed(tail);
            int lastIdx = tail.pack.size() - 1;
            byte[] val = tail.pack.get(lastIdx);
            tail.pack = tail.pack.delete(lastIdx);
            size--;
            if (tail.pack.size() == 0) unlinkNode(tail);
            return val;
        }

        boolean linsert(byte[] pivot, boolean before, byte[] value) {
            Node n = head;
            while (n != null) {
                ensureDecompressed(n);
                int nodeSize = n.pack.size();
                for (int i = 0; i < nodeSize; i++) {
                    if (Arrays.equals(n.pack.get(i), pivot)) {
                        int insertIdx = before ? i : i + 1;
                        n.pack = n.pack.insert(insertIdx, value);
                        size++;
                        // Split if node now exceeds default fill limit
                        if (n.pack.size() > 128) {
                            splitNode(n);
                        }
                        return true;
                    }
                }
                n = n.next;
            }
            return false;
        }

        void lset(long idx, byte[] value) {
            long pos = 0;
            Node n = head;
            while (n != null) {
                ensureDecompressed(n);
                int nodeSize = n.pack.size();
                if (pos + nodeSize > idx) {
                    n.pack = n.pack.set((int) (idx - pos), value);
                    return;
                }
                pos += nodeSize;
                n = n.next;
            }
        }

        long lrem(long count, byte[] value) {
            long removed = 0;
            if (count >= 0) {
                Node n = head;
                while (n != null && (count == 0 || removed < count)) {
                    ensureDecompressed(n);
                    Node next = n.next;
                    int i = 0;
                    while (i < n.pack.size() && (count == 0 || removed < count)) {
                        if (Arrays.equals(n.pack.get(i), value)) {
                            n.pack = n.pack.delete(i);
                            size--;
                            removed++;
                        } else {
                            i++;
                        }
                    }
                    if (n.pack.size() == 0) unlinkNode(n);
                    n = next;
                }
            } else {
                long absCount = -count;
                Node n = tail;
                while (n != null && removed < absCount) {
                    ensureDecompressed(n);
                    Node prev = n.prev;
                    int i = n.pack.size() - 1;
                    while (i >= 0 && removed < absCount) {
                        if (Arrays.equals(n.pack.get(i), value)) {
                            n.pack = n.pack.delete(i);
                            size--;
                            removed++;
                        }
                        i--;
                    }
                    if (n.pack.size() == 0) unlinkNode(n);
                    n = prev;
                }
            }
            return removed;
        }

        private void unlinkNode(Node node) {
            if (node.prev != null) node.prev.next = node.next;
            else head = node.next;
            if (node.next != null) node.next.prev = node.prev;
            else tail = node.prev;
        }

        private void splitNode(Node node) {
            ensureDecompressed(node);
            int half = node.pack.size() / 2;
            List<byte[]> all = node.pack.toList();
            ListPack left = ListPack.create();
            ListPack right = ListPack.create();
            for (int i = 0; i < all.size(); i++) {
                if (i < half) left = left.append(all.get(i));
                else right = right.append(all.get(i));
            }
            node.pack = left;
            Node newNode = new Node(right);
            newNode.prev = node;
            newNode.next = node.next;
            if (node.next != null) node.next.prev = newNode;
            else tail = newNode;
            node.next = newNode;
        }

        QuickList build() {
            return new QuickList(head, tail, size, fill, compress);
        }
    }

    /**
     * Apply LZF compression to middle nodes, keeping {@code compress} nodes
     * from each end uncompressed — mirrors __quicklistCompress() in quicklist.c.
     *
     * Called after mutations when compress > 0.
     */
    public QuickList applyCompression() {
        if (compress <= 0 || head == null) return this;
        // Count total nodes
        int nodeCount = 0;
        Node n = head;
        while (n != null) { nodeCount++; n = n.next; }
        if (nodeCount <= compress * 2) {
            // Not enough nodes — decompress everything
            n = head;
            while (n != null) { n.decompress(); n = n.next; }
            return this;
        }

        // Walk from head: decompress first `compress` nodes
        n = head;
        for (int i = 0; i < compress && n != null; i++) {
            n.decompress();
            n = n.next;
        }
        // Compress middle nodes
        Node endOfHead = n;
        Node startOfTail = tail;
        Node r = tail;
        for (int i = 0; i < compress && r != null; i++) {
            r.decompress();
            startOfTail = r;
            r = r.prev;
        }
        // Compress everything between endOfHead and startOfTail
        n = endOfHead;
        while (n != null && n != startOfTail) {
            n.compress();
            n = n.next;
        }
        return this;
    }

    @Override
    public String toString() {
        return "QuickList{size=" + size + ", fill=" + fill + ", compress=" + compress + "}";
    }
}
