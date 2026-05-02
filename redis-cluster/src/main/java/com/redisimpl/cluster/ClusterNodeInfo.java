package com.redisimpl.cluster;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Metadata about one node in the cluster — mirrors struct _clusterNode in cluster_legacy.h.
 *
 * Key fields mapped from C:
 *   name            → nodeId (40-char hex string)
 *   flags           → nodeFlags (CLUSTER_NODE_MASTER/SLAVE/PFAIL/FAIL/MYSELF/HANDSHAKE/…)
 *   configEpoch     → configEpoch
 *   slots[]         → slots (BitSet, 16384 bits)
 *   ping_sent       → pingSentMs (milliseconds)
 *   pong_received   → pongReceivedMs
 *   data_received   → dataReceivedMs
 *   fail_time       → failTimeMs
 *   ctime           → createTimeMs
 *   ip / tcp_port / cport → host / port / cport
 *   fail_reports    → failReports
 */
public final class ClusterNodeInfo {

    // ---- Node flags (mirrors cluster_legacy.h) ----
    public static final int CLUSTER_NODE_MASTER    = 1;
    public static final int CLUSTER_NODE_SLAVE     = 2;
    public static final int CLUSTER_NODE_PFAIL     = 4;
    public static final int CLUSTER_NODE_FAIL      = 8;
    public static final int CLUSTER_NODE_MYSELF    = 16;
    public static final int CLUSTER_NODE_HANDSHAKE = 32;
    public static final int CLUSTER_NODE_NOADDR    = 64;
    public static final int CLUSTER_NODE_MEET      = 128;

    private final String nodeId;       // 40-char hex (name)
    private volatile String host;
    private volatile int port;
    private volatile int cport;        // cluster bus port
    private final BitSet slots;        // 16384 bits
    private volatile int nodeFlags;    // CLUSTER_NODE_* bitmask
    private volatile long configEpoch;
    private volatile long createTimeMs;

    // Timing fields (milliseconds) — used for PFAIL detection and Gossip
    private volatile long pingSentMs;
    private volatile long pongReceivedMs;
    private volatile long dataReceivedMs;
    private volatile long failTimeMs;

    /** Failure reports from other nodes (for PFAIL→FAIL promotion) */
    private final List<FailReport> failReports = new CopyOnWriteArrayList<>();

    public ClusterNodeInfo(String nodeId, String host, int port) {
        this.nodeId       = nodeId;
        this.host         = host;
        this.port         = port;
        this.cport        = port + ClusterMsg.CLUSTER_NAMELEN;  // override below
        this.slots        = new BitSet(ClusterMsg.CLUSTER_SLOTS);
        this.nodeFlags    = CLUSTER_NODE_MASTER;
        this.configEpoch  = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.pongReceivedMs = System.currentTimeMillis();
        this.dataReceivedMs = System.currentTimeMillis();
    }

    // ---- Slot management ----

    public void addSlotsRange(int from, int to) {
        slots.set(from, to + 1);
    }

    public boolean ownsSlot(int slot) {
        return slots.get(slot);
    }

    /** Returns 2048-byte little-endian slots bitmap (matches clusterNode.slots[]). */
    public byte[] slotsBitmap() {
        byte[] b = new byte[ClusterMsg.CLUSTER_SLOTS / 8];
        for (int i = slots.nextSetBit(0); i >= 0; i = slots.nextSetBit(i + 1)) {
            b[i / 8] |= (1 << (i % 8));
        }
        return b;
    }

    /** Load slots from 2048-byte bitmap. */
    public void loadSlotsBitmap(byte[] bitmap) {
        slots.clear();
        for (int i = 0; i < bitmap.length; i++) {
            for (int bit = 0; bit < 8; bit++) {
                if ((bitmap[i] & (1 << bit)) != 0) {
                    slots.set(i * 8 + bit);
                }
            }
        }
    }

    // ---- Flag helpers ----

    public boolean isSelf()      { return (nodeFlags & CLUSTER_NODE_MYSELF) != 0; }
    public boolean isMaster()    { return (nodeFlags & CLUSTER_NODE_MASTER) != 0; }
    public boolean isSlave()     { return (nodeFlags & CLUSTER_NODE_SLAVE) != 0; }
    public boolean inHandshake() { return (nodeFlags & CLUSTER_NODE_HANDSHAKE) != 0; }
    public boolean isPfail()     { return (nodeFlags & CLUSTER_NODE_PFAIL) != 0; }
    public boolean isFailed()    { return (nodeFlags & CLUSTER_NODE_FAIL) != 0; }
    public boolean hasAddr()     { return (nodeFlags & CLUSTER_NODE_NOADDR) == 0; }

    public void setSelf(boolean v) {
        if (v) nodeFlags |= CLUSTER_NODE_MYSELF;
        else   nodeFlags &= ~CLUSTER_NODE_MYSELF;
    }
    public void setPfail(boolean v) {
        if (v) nodeFlags |= CLUSTER_NODE_PFAIL;
        else   nodeFlags &= ~CLUSTER_NODE_PFAIL;
    }
    public void setFailed(boolean v) {
        if (v) { nodeFlags |= CLUSTER_NODE_FAIL; failTimeMs = System.currentTimeMillis(); }
        else   nodeFlags &= ~CLUSTER_NODE_FAIL;
    }
    public void clearHandshake() {
        nodeFlags &= ~CLUSTER_NODE_HANDSHAKE;
    }

    // ---- Failure reports ----

    public static final class FailReport {
        public final String reporterNodeId;
        public volatile long time;
        FailReport(String id) { this.reporterNodeId = id; this.time = System.currentTimeMillis(); }
    }

    /** Add or update a failure report from the given reporter. Returns true if newly added. */
    public boolean addFailureReport(String reporterNodeId) {
        for (FailReport fr : failReports) {
            if (fr.reporterNodeId.equals(reporterNodeId)) {
                fr.time = System.currentTimeMillis();
                return false;
            }
        }
        failReports.add(new FailReport(reporterNodeId));
        return true;
    }

    /** Remove failure report from reporter. Returns true if removed. */
    public boolean removeFailureReport(String reporterNodeId) {
        return failReports.removeIf(fr -> fr.reporterNodeId.equals(reporterNodeId));
    }

    /** Remove stale failure reports older than maxAgeMs. */
    public void cleanupFailureReports(long maxAgeMs) {
        long now = System.currentTimeMillis();
        failReports.removeIf(fr -> now - fr.time > maxAgeMs);
    }

    public int failureReportCount() { return failReports.size(); }

    // ---- CLUSTER NODES output line ----

    public String toNodesLine() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeId).append(" ");
        sb.append(host).append(":").append(port)
          .append("@").append(port + 10000).append(" ");

        // flags
        boolean first = true;
        if (isSelf())      { sb.append("myself");  first = false; }
        if (isMaster())    { if (!first) sb.append(","); sb.append("master"); first = false; }
        if (isSlave())     { if (!first) sb.append(","); sb.append("slave"); first = false; }
        if (isPfail())     { if (!first) sb.append(","); sb.append("s_down"); first = false; }
        if (isFailed())    { if (!first) sb.append(","); sb.append("fail"); first = false; }
        if (inHandshake()) { if (!first) sb.append(","); sb.append("handshake"); }

        sb.append(" - 0 ");
        sb.append(pongReceivedMs > 0 ? pongReceivedMs : 0);
        sb.append(" ");
        sb.append(configEpoch).append(" ");
        sb.append(inHandshake() || isFailed() ? "disconnected" : "connected");

        // slot ranges
        int start = slots.nextSetBit(0);
        while (start >= 0) {
            int end = slots.nextClearBit(start) - 1;
            if (end < start || end > 16383) end = 16383;
            sb.append(" ").append(start).append("-").append(end);
            start = slots.nextSetBit(end + 1);
        }
        return sb.toString();
    }

    // ---- Getters / Setters ----

    public String getNodeId()                 { return nodeId; }
    public String getHost()                   { return host; }
    public void setHost(String h)             { this.host = h; }
    public int getPort()                      { return port; }
    public void setPort(int p)                { this.port = p; }
    public int getCport()                     { return port + 10000; }
    public BitSet getSlots()                  { return slots; }
    public int getNodeFlags()                 { return nodeFlags; }
    public void setNodeFlags(int f)           { this.nodeFlags = f; }
    public long getConfigEpoch()              { return configEpoch; }
    public void setConfigEpoch(long e)        { this.configEpoch = e; }
    public long getPingSentMs()               { return pingSentMs; }
    public void setPingSentMs(long t)         { this.pingSentMs = t; }
    public long getPongReceivedMs()           { return pongReceivedMs; }
    public void setPongReceivedMs(long t)     { this.pongReceivedMs = t; }
    public long getDataReceivedMs()           { return dataReceivedMs; }
    public void setDataReceivedMs(long t)     { this.dataReceivedMs = t; }
    public long getFailTimeMs()               { return failTimeMs; }
    public long getCreateTimeMs()             { return createTimeMs; }
}
