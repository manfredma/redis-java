package com.redisimpl.core.quicklist;

import com.redisimpl.core.listpack.ListPack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * QuickList — Java port of Redis's quicklist.c.
 *
 * <p>A doubly-linked list where each node contains a {@link ListPack}.
 * When a node's listpack exceeds the configured limits, it is split.
 *
 * <p>Operations work on a mutable copy and return a new immutable QuickList.
 */
public final class QuickList {

    /** Max elements per listpack node */
    public static final int QUICKLIST_NODE_MAX_ENTRIES = 128;
    /** Max bytes per element before forcing a new node */
    public static final int QUICKLIST_NODE_MAX_VALUE   = 64;

    /** Doubly-linked node */
    static final class Node {
        ListPack pack;
        Node prev;
        Node next;

        Node(ListPack pack) {
            this.pack = pack;
        }
    }

    final Node head;
    final Node tail;
    final long size; // total element count

    private QuickList(Node head, Node tail, long size) {
        this.head = head;
        this.tail = tail;
        this.size = size;
    }

    public static QuickList create() {
        return new QuickList(null, null, 0);
    }

    // ---- Push ----

    public QuickList rpush(byte[] value) {
        Mut m = new Mut(this);
        m.rpush(value);
        return m.build();
    }

    public QuickList lpush(byte[] value) {
        Mut m = new Mut(this);
        m.lpush(value);
        return m.build();
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

        Mut(QuickList ql) {
            this.size = ql.size;
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

        void rpush(byte[] value) {
            if (tail == null) {
                // Empty list
                Node newNode = new Node(ListPack.create().append(value));
                head = tail = newNode;
                size++;
            } else if (value.length > QUICKLIST_NODE_MAX_VALUE
                    || tail.pack.size() >= QUICKLIST_NODE_MAX_ENTRIES) {
                // Need a new node
                Node newNode = new Node(ListPack.create().append(value));
                newNode.prev = tail;
                tail.next = newNode;
                tail = newNode;
                size++;
            } else {
                tail.pack = tail.pack.append(value);
                size++;
            }
        }

        void lpush(byte[] value) {
            if (head == null) {
                Node newNode = new Node(ListPack.create().append(value));
                head = tail = newNode;
                size++;
            } else if (value.length > QUICKLIST_NODE_MAX_VALUE
                    || head.pack.size() >= QUICKLIST_NODE_MAX_ENTRIES) {
                Node newNode = new Node(ListPack.create().append(value));
                newNode.next = head;
                head.prev = newNode;
                head = newNode;
                size++;
            } else {
                head.pack = head.pack.prepend(value);
                size++;
            }
        }

        byte[] lpop() {
            if (head == null) return null;
            byte[] val = head.pack.get(0);
            head.pack = head.pack.delete(0);
            size--;
            if (head.pack.size() == 0) {
                unlinkNode(head);
            }
            return val;
        }

        byte[] rpop() {
            if (tail == null) return null;
            int lastIdx = tail.pack.size() - 1;
            byte[] val = tail.pack.get(lastIdx);
            tail.pack = tail.pack.delete(lastIdx);
            size--;
            if (tail.pack.size() == 0) {
                unlinkNode(tail);
            }
            return val;
        }

        boolean linsert(byte[] pivot, boolean before, byte[] value) {
            Node n = head;
            while (n != null) {
                int nodeSize = n.pack.size();
                for (int i = 0; i < nodeSize; i++) {
                    if (Arrays.equals(n.pack.get(i), pivot)) {
                        int insertIdx = before ? i : i + 1;
                        n.pack = n.pack.insert(insertIdx, value);
                        size++;
                        if (n.pack.size() > QUICKLIST_NODE_MAX_ENTRIES) {
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
            return new QuickList(head, tail, size);
        }
    }

    @Override
    public String toString() {
        return "QuickList{size=" + size + "}";
    }
}
