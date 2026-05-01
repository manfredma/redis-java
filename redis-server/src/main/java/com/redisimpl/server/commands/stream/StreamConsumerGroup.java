package com.redisimpl.server.commands.stream;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a consumer group on a stream.
 */
public final class StreamConsumerGroup {

    private final String name;
    /** Last-delivered ID (millis-seq) */
    private long lastDeliveredMillis;
    private long lastDeliveredSeq;
    /** Pending Entry List: entry-id → PEL entry */
    private final Map<String, PelEntry> pel = new LinkedHashMap<>();
    /** Consumers: name → consumer state */
    private final Map<String, StreamConsumer> consumers = new ConcurrentHashMap<>();

    public StreamConsumerGroup(String name, long lastDeliveredMillis, long lastDeliveredSeq) {
        this.name = name;
        this.lastDeliveredMillis = lastDeliveredMillis;
        this.lastDeliveredSeq = lastDeliveredSeq;
    }

    public String getName() { return name; }
    public long getLastDeliveredMillis() { return lastDeliveredMillis; }
    public long getLastDeliveredSeq()    { return lastDeliveredSeq; }
    public Map<String, PelEntry> getPel()  { return pel; }
    public Map<String, StreamConsumer> getConsumers() { return consumers; }

    public void setLastDelivered(long millis, long seq) {
        this.lastDeliveredMillis = millis;
        this.lastDeliveredSeq = seq;
    }

    public StreamConsumer getOrCreateConsumer(String consumerName) {
        return consumers.computeIfAbsent(consumerName, StreamConsumer::new);
    }

    /** Represents a pending entry in the PEL. */
    public static final class PelEntry {
        private final String entryId;
        private final String consumerName;
        private long deliveryTime;
        private int deliveryCount;

        public PelEntry(String entryId, String consumerName, long deliveryTime) {
            this.entryId = entryId;
            this.consumerName = consumerName;
            this.deliveryTime = deliveryTime;
            this.deliveryCount = 1;
        }

        public String getEntryId()     { return entryId; }
        public String getConsumerName(){ return consumerName; }
        public long getDeliveryTime()  { return deliveryTime; }
        public int getDeliveryCount()  { return deliveryCount; }

        public void incrementDelivery(long now) {
            this.deliveryTime = now;
            this.deliveryCount++;
        }
    }

    /** Represents a consumer within a group. */
    public static final class StreamConsumer {
        private final String name;
        private long seenTime;
        private int pelCount;

        public StreamConsumer(String name) {
            this.name = name;
            this.seenTime = System.currentTimeMillis();
        }

        public String getName()    { return name; }
        public long getSeenTime()  { return seenTime; }
        public int getPelCount()   { return pelCount; }

        public void touch() { this.seenTime = System.currentTimeMillis(); }
        public void incPel() { pelCount++; }
        public void decPel() { if (pelCount > 0) pelCount--; }
    }
}
