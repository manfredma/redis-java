package com.redisimpl.cluster;

import java.util.BitSet;

/**
 * Metadata about one node in the cluster (our own or a peer).
 */
public final class ClusterNodeInfo {

    public enum State { HANDSHAKE, CONNECTED, FAILED }

    private final String nodeId;          // 40-char hex
    private final String host;
    private final int port;
    private final BitSet slots;           // 16384 bits
    private volatile State state = State.CONNECTED;
    private volatile boolean isSelf = false;
    private volatile boolean isMaster = true;

    public ClusterNodeInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.slots = new BitSet(16384);
    }

    public String getNodeId()          { return nodeId; }
    public String getHost()            { return host; }
    public int getPort()               { return port; }
    public BitSet getSlots()           { return slots; }
    public State getState()            { return state; }
    public void setState(State s)      { this.state = s; }
    public boolean isSelf()            { return isSelf; }
    public void setSelf(boolean self)  { this.isSelf = self; }
    public boolean isMaster()          { return isMaster; }

    public void addSlotsRange(int from, int to) {
        slots.set(from, to + 1);
    }

    public boolean ownsSlot(int slot) {
        return slots.get(slot);
    }

    /** CLUSTER NODES output line */
    public String toNodesLine() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeId).append(" ");
        sb.append(host).append(":").append(port).append("@").append(port + 10000).append(" ");
        sb.append(isSelf ? "myself,master" : "master").append(" ");
        sb.append("- 0 0 0 connected");
        // Append slot ranges
        int start = slots.nextSetBit(0);
        while (start != -1) {
            int end = slots.nextClearBit(start) - 1;
            if (end < start) end = 16383;
            sb.append(" ").append(start).append("-").append(end);
            start = slots.nextSetBit(end + 1);
        }
        return sb.toString();
    }
}
