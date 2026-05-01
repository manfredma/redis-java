package com.redisimpl.core.zskiplist;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * ZSkipList — Java port of Redis's zskiplist in t_zset.c.
 *
 * <p>A skip list for sorted sets. Elements are ordered by score (ascending),
 * with ties broken by lexicographic comparison of the element bytes.
 *
 * <p>Max level: 64. Level probability: P = 0.25.
 */
public final class ZSkipList {

    /** Maximum number of levels */
    public static final int ZSKIPLIST_MAXLEVEL = 64;

    /** Probability for level promotion */
    private static final double ZSKIPLIST_P = 0.25;

    private final ZSkipListNode header;
    private ZSkipListNode tail;
    private long length;
    private int level;

    private final Random random = new Random();

    public ZSkipList() {
        this.level = 1;
        this.length = 0;
        this.header = new ZSkipListNode(ZSKIPLIST_MAXLEVEL, 0, null);
        this.tail = null;
    }

    // ---- Core operations ----

    /**
     * Insert a new element with the given score.
     * Returns the new node.
     */
    public ZSkipListNode insert(double score, byte[] ele) {
        ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
        long[] rank = new long[ZSKIPLIST_MAXLEVEL];

        ZSkipListNode x = header;
        for (int i = level - 1; i >= 0; i--) {
            rank[i] = (i == level - 1) ? 0 : rank[i + 1];
            while (x.getLevels()[i].forward != null
                    && compareNodes(x.getLevels()[i].forward.getScore(),
                    x.getLevels()[i].forward.getEle(), score, ele) < 0) {
                rank[i] += x.getLevels()[i].span;
                x = x.getLevels()[i].forward;
            }
            update[i] = x;
        }

        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                rank[i] = 0;
                update[i] = header;
                update[i].getLevels()[i].span = length;
            }
            level = newLevel;
        }

        x = new ZSkipListNode(newLevel, score, ele);
        for (int i = 0; i < newLevel; i++) {
            x.getLevels()[i].forward = update[i].getLevels()[i].forward;
            update[i].getLevels()[i].forward = x;
            // update span
            x.getLevels()[i].span = update[i].getLevels()[i].span - (rank[0] - rank[i]);
            update[i].getLevels()[i].span = (rank[0] - rank[i]) + 1;
        }
        // Increment span for untouched levels
        for (int i = newLevel; i < level; i++) {
            update[i].getLevels()[i].span++;
        }

        x.setBackward(update[0] == header ? null : update[0]);
        if (x.getLevels()[0].forward != null) {
            x.getLevels()[0].forward.setBackward(x);
        } else {
            tail = x;
        }
        length++;
        return x;
    }

    /**
     * Delete element with the given score and ele.
     * Returns true if found and deleted.
     */
    public boolean delete(double score, byte[] ele) {
        ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
        ZSkipListNode x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.getLevels()[i].forward != null
                    && compareNodes(x.getLevels()[i].forward.getScore(),
                    x.getLevels()[i].forward.getEle(), score, ele) < 0) {
                x = x.getLevels()[i].forward;
            }
            update[i] = x;
        }
        x = x.getLevels()[0].forward;
        if (x != null && x.getScore() == score && Arrays.equals(x.getEle(), ele)) {
            deleteNode(x, update);
            return true;
        }
        return false;
    }

    /**
     * Get 1-based rank of element. Returns 0 if not found.
     */
    public long rank(double score, byte[] ele) {
        long rank = 0;
        ZSkipListNode x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.getLevels()[i].forward != null
                    && compareNodes(x.getLevels()[i].forward.getScore(),
                    x.getLevels()[i].forward.getEle(), score, ele) <= 0) {
                rank += x.getLevels()[i].span;
                x = x.getLevels()[i].forward;
                if (x.getScore() == score && Arrays.equals(x.getEle(), ele)) {
                    return rank;
                }
            }
        }
        return 0;
    }

    /**
     * Get nodes in rank range [start, stop] (1-based, inclusive).
     */
    public List<ZSkipListNode> rangeByRank(long start, long stop) {
        List<ZSkipListNode> result = new ArrayList<>();
        if (start < 1 || start > length) return result;
        stop = Math.min(stop, length);

        // Advance to start
        long traversed = 0;
        ZSkipListNode x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.getLevels()[i].forward != null && traversed + x.getLevels()[i].span < start) {
                traversed += x.getLevels()[i].span;
                x = x.getLevels()[i].forward;
            }
        }
        x = x.getLevels()[0].forward;
        traversed++;

        while (x != null && traversed <= stop) {
            result.add(x);
            x = x.getLevels()[0].forward;
            traversed++;
        }
        return result;
    }

    /**
     * Get nodes with score in [min, max].
     * @param minExclusive if true, min is exclusive (score > min)
     * @param maxExclusive if true, max is exclusive (score < max)
     */
    public List<ZSkipListNode> rangeByScore(double min, double max,
                                             boolean minExclusive, boolean maxExclusive) {
        List<ZSkipListNode> result = new ArrayList<>();
        // Find first node >= min (or > min if exclusive)
        ZSkipListNode x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.getLevels()[i].forward != null) {
                double s = x.getLevels()[i].forward.getScore();
                if (minExclusive ? s <= min : s < min) {
                    x = x.getLevels()[i].forward;
                } else break;
            }
        }
        x = x.getLevels()[0].forward;
        while (x != null) {
            double s = x.getScore();
            if (maxExclusive ? s >= max : s > max) break;
            result.add(x);
            x = x.getLevels()[0].forward;
        }
        return result;
    }

    /**
     * Count elements with score in [min, max] (inclusive).
     */
    public long count(double min, double max) {
        return rangeByScore(min, max, false, false).size();
    }

    /**
     * Get nodes with element in lexicographic range [min, max].
     * min/max format: "[value" (inclusive), "(value" (exclusive), "-" (neg inf), "+" (pos inf).
     * All elements must have the same score.
     */
    public List<ZSkipListNode> rangeByLex(String min, String max) {
        List<ZSkipListNode> result = new ArrayList<>();
        boolean minInclusive = !min.equals("-") && min.startsWith("[");
        boolean maxInclusive = !max.equals("+") && max.startsWith("[");
        boolean minNegInf = min.equals("-");
        boolean maxPosInf = max.equals("+");

        byte[] minBytes = minNegInf ? null : min.substring(1).getBytes(StandardCharsets.UTF_8);
        byte[] maxBytes = maxPosInf ? null : max.substring(1).getBytes(StandardCharsets.UTF_8);

        ZSkipListNode x = header.getLevels()[0].forward;
        while (x != null) {
            byte[] ele = x.getEle();
            // Check min
            if (!minNegInf) {
                int cmp = compareBytes(ele, minBytes);
                if (minInclusive ? cmp < 0 : cmp <= 0) {
                    x = x.getLevels()[0].forward;
                    continue;
                }
            }
            // Check max
            if (!maxPosInf) {
                int cmp = compareBytes(ele, maxBytes);
                if (maxInclusive ? cmp > 0 : cmp >= 0) break;
            }
            result.add(x);
            x = x.getLevels()[0].forward;
        }
        return result;
    }

    public long length() {
        return length;
    }

    public ZSkipListNode getTail() {
        return tail;
    }

    // ---- Internal helpers ----

    private void deleteNode(ZSkipListNode x, ZSkipListNode[] update) {
        for (int i = 0; i < level; i++) {
            if (update[i].getLevels()[i].forward == x) {
                update[i].getLevels()[i].span += x.getLevels()[i].span - 1;
                update[i].getLevels()[i].forward = x.getLevels()[i].forward;
            } else {
                update[i].getLevels()[i].span--;
            }
        }
        if (x.getLevels()[0].forward != null) {
            x.getLevels()[0].forward.setBackward(x.getBackward());
        } else {
            tail = x.getBackward();
        }
        while (level > 1 && header.getLevels()[level - 1].forward == null) {
            level--;
        }
        length--;
    }

    /**
     * Compare two (score, ele) pairs. Returns negative if (s1,e1) < (s2,e2).
     * Ties in score are broken by lexicographic comparison of ele.
     */
    private static int compareNodes(double s1, byte[] e1, double s2, byte[] e2) {
        if (s1 != s2) return Double.compare(s1, s2);
        if (e1 == null && e2 == null) return 0;
        if (e1 == null) return -1;
        if (e2 == null) return 1;
        return compareBytes(e1, e2);
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return a.length - b.length;
    }

    /**
     * Generate a random level. P=0.25, max=ZSKIPLIST_MAXLEVEL.
     */
    private int randomLevel() {
        int lvl = 1;
        while (random.nextDouble() < ZSKIPLIST_P && lvl < ZSKIPLIST_MAXLEVEL) {
            lvl++;
        }
        return lvl;
    }
}
