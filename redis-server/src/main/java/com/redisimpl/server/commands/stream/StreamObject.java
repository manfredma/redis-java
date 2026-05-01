package com.redisimpl.server.commands.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Stream data structure.
 * Backed by a TreeMap keyed by "millis-seq" for ordering.
 * Consumer groups are stored separately.
 */
public final class StreamObject {

    /** Entries ordered by ID: "millis-seq" -> StreamEntry */
    private final TreeMap<String, StreamEntry> entries = new TreeMap<>(StreamObject::compareIds);

    /** Last generated ID (to ensure monotonic increase) */
    private long lastMillis = 0;
    private long lastSeq = 0;

    /** Consumer groups */
    private final Map<String, StreamConsumerGroup> groups = new ConcurrentHashMap<>();

    /** Max length (0 = unlimited) */
    private long maxLen = 0;

    // ---- ID generation ----

    /**
     * Generate the next auto ID using current time.
     * Returns the generated entry after appending.
     */
    public StreamEntry add(long requestedMillis, long requestedSeq, Map<String, String> fields) {
        long millis;
        long seq;

        if (requestedMillis == -1) {
            // Auto-generate: use current time
            millis = System.currentTimeMillis();
            if (millis < lastMillis) millis = lastMillis;
            seq = (millis == lastMillis) ? lastSeq + 1 : 0;
        } else if (requestedSeq == -1) {
            // Partial: millis given, auto seq
            millis = requestedMillis;
            if (millis < lastMillis) {
                throw new IllegalArgumentException(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
            seq = (millis == lastMillis) ? lastSeq + 1 : 0;
        } else {
            // Full explicit ID
            millis = requestedMillis;
            seq = requestedSeq;
            // Validate monotonic
            int cmp = compareMillisSeq(millis, seq, lastMillis, lastSeq);
            if (cmp <= 0 && (lastMillis > 0 || lastSeq > 0)) {
                throw new IllegalArgumentException(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }

        StreamEntry entry = new StreamEntry(millis, seq, fields);
        entries.put(entry.getId(), entry);
        lastMillis = millis;
        lastSeq = seq;

        // Trim if maxLen is set
        if (maxLen > 0) {
            while (entries.size() > maxLen) {
                entries.pollFirstEntry();
            }
        }

        return entry;
    }

    public long size() { return entries.size(); }
    public long getLastMillis() { return lastMillis; }
    public long getLastSeq()    { return lastSeq; }
    public long getMaxLen()     { return maxLen; }
    public void setMaxLen(long maxLen) { this.maxLen = maxLen; }

    public Map<String, StreamConsumerGroup> getGroups() { return groups; }

    /**
     * XRANGE: return entries with id in [startId, endId], up to count.
     * Use "-" for min, "+" for max.
     */
    public List<StreamEntry> range(String startId, String endId, int count) {
        String lo = "-".equals(startId) ? null : startId;
        String hi = "+".equals(endId)   ? null : endId;

        List<StreamEntry> result = new ArrayList<>();
        for (Map.Entry<String, StreamEntry> e : entries.entrySet()) {
            String id = e.getKey();
            if (lo != null && compareIds(id, lo) < 0) continue;
            if (hi != null && compareIds(id, hi) > 0) break;
            result.add(e.getValue());
            if (count > 0 && result.size() >= count) break;
        }
        return result;
    }

    /**
     * XREVRANGE: return entries with id in [endId, startId] in reverse, up to count.
     */
    public List<StreamEntry> revrange(String endId, String startId, int count) {
        String lo = "-".equals(startId) ? null : startId;
        String hi = "+".equals(endId)   ? null : endId;

        List<StreamEntry> result = new ArrayList<>();
        for (Map.Entry<String, StreamEntry> e : entries.descendingMap().entrySet()) {
            String id = e.getKey();
            if (hi != null && compareIds(id, hi) > 0) continue;
            if (lo != null && compareIds(id, lo) < 0) break;
            result.add(e.getValue());
            if (count > 0 && result.size() >= count) break;
        }
        return result;
    }

    /**
     * XREAD: return up to count entries with id > afterId.
     */
    public List<StreamEntry> read(String afterId, int count) {
        List<StreamEntry> result = new ArrayList<>();
        for (Map.Entry<String, StreamEntry> e : entries.entrySet()) {
            if (compareIds(e.getKey(), afterId) > 0) {
                result.add(e.getValue());
                if (count > 0 && result.size() >= count) break;
            }
        }
        return result;
    }

    /** Delete entries by ID, return count deleted. */
    public long delete(List<String> ids) {
        long deleted = 0;
        for (String id : ids) {
            if (entries.remove(id) != null) deleted++;
        }
        return deleted;
    }

    /**
     * XTRIM: trim to maxLen entries (keep newest), return number removed.
     */
    public long trim(long maxLen, boolean approx) {
        long removed = 0;
        while (entries.size() > maxLen) {
            entries.pollFirstEntry();
            removed++;
        }
        this.maxLen = maxLen;
        return removed;
    }

    /** Get a single entry by exact ID. */
    public StreamEntry getEntry(String id) {
        return entries.get(id);
    }

    /** Get first entry ID (or null if empty). */
    public String firstEntryId() {
        Map.Entry<String, StreamEntry> e = entries.firstEntry();
        return e == null ? null : e.getKey();
    }

    /** Get last entry ID (or null if empty). */
    public String lastEntryId() {
        Map.Entry<String, StreamEntry> e = entries.lastEntry();
        return e == null ? null : e.getKey();
    }

    // ---- ID comparison utilities ----

    public static int compareIds(String a, String b) {
        long[] pa = parseId(a);
        long[] pb = parseId(b);
        int c = Long.compare(pa[0], pb[0]);
        return c != 0 ? c : Long.compare(pa[1], pb[1]);
    }

    private static int compareMillisSeq(long am, long as, long bm, long bs) {
        int c = Long.compare(am, bm);
        return c != 0 ? c : Long.compare(as, bs);
    }

    public static long[] parseId(String id) {
        if ("-".equals(id)) return new long[]{0, 0};
        if ("+".equals(id)) return new long[]{Long.MAX_VALUE, Long.MAX_VALUE};
        int dash = id.lastIndexOf('-');
        if (dash < 0) {
            return new long[]{Long.parseLong(id), 0};
        }
        long millis = Long.parseLong(id.substring(0, dash));
        long seq    = Long.parseLong(id.substring(dash + 1));
        return new long[]{millis, seq};
    }
}
