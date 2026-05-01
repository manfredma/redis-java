package com.redisimpl.server.commands.stream;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single entry in a Redis Stream.
 * ID format: milliseconds-sequenceNumber (e.g. "1609459200000-0").
 */
public final class StreamEntry {

    private final long millis;
    private final long seq;
    private final Map<String, String> fields;

    public StreamEntry(long millis, long seq, Map<String, String> fields) {
        this.millis = millis;
        this.seq = seq;
        this.fields = new LinkedHashMap<>(fields);
    }

    public long getMillis() { return millis; }
    public long getSeq()    { return seq; }
    public Map<String, String> getFields() { return fields; }

    /** Returns the ID string "millis-seq". */
    public String getId() {
        return millis + "-" + seq;
    }

    /** Compare two entries by (millis, seq). */
    public static int compare(StreamEntry a, StreamEntry b) {
        int c = Long.compare(a.millis, b.millis);
        return c != 0 ? c : Long.compare(a.seq, b.seq);
    }

    @Override
    public String toString() {
        return getId() + " " + fields;
    }
}
