package com.redisimpl.cluster;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cluster-wide state: the slot table and known nodes.
 */
public final class ClusterState {

    /** Slot → owning node ID */
    private final String[] slotOwner = new String[16384];

    /** nodeId → ClusterNodeInfo */
    private final ConcurrentHashMap<String, ClusterNodeInfo> nodes = new ConcurrentHashMap<>();

    private volatile String stateStr = "ok";

    public void addNode(ClusterNodeInfo node) {
        nodes.put(node.getNodeId(), node);
    }

    public void removeNode(String nodeId) {
        nodes.remove(nodeId);
    }

    public ClusterNodeInfo getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public Collection<ClusterNodeInfo> allNodes() {
        return nodes.values();
    }

    public void assignSlots(String nodeId, int from, int to) {
        for (int i = from; i <= to; i++) slotOwner[i] = nodeId;
        ClusterNodeInfo n = nodes.get(nodeId);
        if (n != null) n.addSlotsRange(from, to);
    }

    /**
     * Returns the node info that owns the given slot, or null if unassigned.
     */
    public ClusterNodeInfo getSlotOwner(int slot) {
        String id = slotOwner[slot];
        if (id == null) return null;
        return nodes.get(id);
    }

    public String getStateStr() { return stateStr; }

    public boolean isFullyCovered() {
        for (int i = 0; i < 16384; i++) {
            if (slotOwner[i] == null) return false;
        }
        return true;
    }
}
