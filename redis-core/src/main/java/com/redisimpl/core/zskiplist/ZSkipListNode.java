package com.redisimpl.core.zskiplist;

import lombok.Getter;
import lombok.Setter;

/**
 * A node in the ZSkipList.
 * Mirrors Redis's zskiplistNode in t_zset.c.
 */
@Getter
@Setter
public final class ZSkipListNode {

    /** Element (member) */
    private byte[] ele;

    /** Score */
    private double score;

    /** Backward pointer (for reverse traversal at level 1) */
    private ZSkipListNode backward;

    /** Forward pointers and span for each level */
    private final ZSkipListLevel[] levels;

    ZSkipListNode(int level, double score, byte[] ele) {
        this.score = score;
        this.ele = ele != null ? ele.clone() : null;
        this.levels = new ZSkipListLevel[level];
        for (int i = 0; i < level; i++) {
            levels[i] = new ZSkipListLevel();
        }
    }

    /** Level entry: forward pointer + span */
    public static final class ZSkipListLevel {
        /** Forward pointer */
        public ZSkipListNode forward;
        /** Number of nodes skipped by this forward pointer */
        public long span;
    }
}
