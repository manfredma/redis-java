package com.redisimpl.cluster;

import com.redisimpl.server.RedisServer;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.resp.RespEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;

/**
 * A Redis server node that participates in a cluster.
 *
 * Extends RedisServer with:
 * - 16384-slot ownership (via ClusterState)
 * - MOVED redirection for keys not owned by this node
 * - CLUSTER command suite
 */
public final class ClusterNode {

    private static final Logger log = LoggerFactory.getLogger(ClusterNode.class);

    private final RedisServer server;
    private final ClusterState clusterState;
    private final ClusterNodeInfo selfInfo;
    private final String nodeId;
    private final int port;
    private GossipManager gossipManager;

    /** Peer nodes added via meet() before start (legacy in-process setup) */
    private final List<ClusterNodeInfo> peers = new ArrayList<>();

    public ClusterNode(int port) throws IOException {
        this.port = port;
        this.nodeId = generateNodeId();
        this.server = new RedisServer(port);
        this.clusterState = new ClusterState();
        this.selfInfo = new ClusterNodeInfo(nodeId, "127.0.0.1", port);
        selfInfo.setSelf(true);
        clusterState.addNode(selfInfo);

        // Register CLUSTER commands
        server.getCommandTable().register(new ClusterCommands(this));
    }

    public void addSlots(int from, int to) {
        clusterState.assignSlots(nodeId, from, to);
    }

    public void meet(String host, int peerPort) {
        // Register peer node (nodeId unknown until connection, use placeholder)
        String tempId = "temp-" + host + "-" + peerPort;
        ClusterNodeInfo peer = new ClusterNodeInfo(tempId, host, peerPort);
        peers.add(peer);
        clusterState.addNode(peer);
    }

    /**
     * Register a peer node with known slot range (for in-process cluster setup).
     */
    public void registerPeer(ClusterNode peer) {
        ClusterNodeInfo info = new ClusterNodeInfo(peer.getNodeId(), "127.0.0.1", peer.getPort());
        // Copy peer's slot bitmap
        java.util.BitSet peerSlots = peer.getSelfInfo().getSlots();
        int start = peerSlots.nextSetBit(0);
        while (start != -1) {
            int end = peerSlots.nextClearBit(start) - 1;
            if (end < start) end = 16383;
            info.addSlotsRange(start, end);
            clusterState.assignSlots(peer.getNodeId(), start, end);
            start = peerSlots.nextSetBit(end + 1);
        }
        clusterState.addNode(info);
    }

    public void start() throws IOException {
        // Register legacy in-process peers
        for (ClusterNodeInfo peer : peers) {
            clusterState.addNode(peer);
        }
        // Install cluster slot checker
        server.setClusterSlotChecker(argv -> {
            if (argv.length < 2) return null;
            return checkSlot(argv[1]);
        });

        // Start Gossip manager (cluster bus on port+10000)
        gossipManager = new GossipManager(this);
        try {
            gossipManager.start();
        } catch (IOException e) {
            log.warn("Could not start cluster bus on port {}: {}",
                    selfInfo.getPort() + 10000, e.getMessage());
        }

        server.start();
    }

    public void stop() {
        if (gossipManager != null) gossipManager.stop();
        server.stop();
    }

    /** Trigger CLUSTER MEET via Gossip protocol (real network). */
    public void gossipMeet(String host, int peerPort) {
        if (gossipManager != null) {
            gossipManager.startMeet(host, peerPort);
        }
    }

    public ClusterState getClusterState() { return clusterState; }
    public ClusterNodeInfo getSelfInfo()  { return selfInfo; }
    public String getNodeId()             { return nodeId; }
    public RedisServer getServer()        { return server; }
    public int getPort()                  { return port; }

    /**
     * Check if this node owns the slot for the given key.
     * Returns null if owned, or a MOVED error bytes if not owned.
     */
    public byte[] checkSlot(byte[] key) {
        int slot = CRC16.keyHashSlot(key);
        // Check if self owns this slot
        if (selfInfo.ownsSlot(slot)) return null;

        // Not owned by self — find who does
        ClusterNodeInfo owner = clusterState.getSlotOwner(slot);
        if (owner != null && !owner.isSelf()) {
            String moved = "MOVED " + slot + " " + owner.getHost() + ":" + owner.getPort();
            return RespEncoder.encodeError(moved);
        }

        // Slot not assigned to any known node — find a peer who might own it
        // (used when peer slot info not yet propagated via gossip)
        for (ClusterNodeInfo peer : clusterState.allNodes()) {
            if (!peer.isSelf() && peer.ownsSlot(slot)) {
                String moved = "MOVED " + slot + " " + peer.getHost() + ":" + peer.getPort();
                return RespEncoder.encodeError(moved);
            }
        }

        // Last resort: slot unassigned — redirect to first non-self peer
        for (ClusterNodeInfo peer : clusterState.allNodes()) {
            if (!peer.isSelf()) {
                String moved = "MOVED " + slot + " " + peer.getHost() + ":" + peer.getPort();
                return RespEncoder.encodeError(moved);
            }
        }

        // No peers: allow (standalone mode)
        return null;
    }

    private static String generateNodeId() {
        SecureRandom rng = new SecureRandom();
        StringBuilder sb = new StringBuilder(40);
        String chars = "0123456789abcdef";
        for (int i = 0; i < 40; i++) sb.append(chars.charAt(rng.nextInt(chars.length())));
        return sb.toString();
    }

    // ---- CLUSTER command implementations ----

    public final class ClusterCommands {

        private final ClusterNode node;

        public ClusterCommands(ClusterNode node) {
            this.node = node;
        }

        @RedisCommand(name = "cluster", arity = -2, flags = "admin",
                firstKey = 0, lastKey = 0, step = 0)
        public byte[] cluster(RedisClient client, byte[][] argv) {
            if (argv.length < 2)
                return RespEncoder.encodeError("ERR syntax error");
            String sub = new String(argv[1], StandardCharsets.UTF_8).toUpperCase();
            switch (sub) {
                case "INFO":
                    return clusterInfo();
                case "NODES":
                    return clusterNodes();
                case "MYID":
                    return RespEncoder.encodeBulkString(nodeId.getBytes(StandardCharsets.UTF_8));
                case "KEYSLOT": {
                    if (argv.length < 3) return RespEncoder.encodeError("ERR syntax error");
                    int slot = CRC16.keyHashSlot(argv[2]);
                    return RespEncoder.encodeInteger(slot);
                }
                case "COUNTKEYSINSLOT": {
                    if (argv.length < 3) return RespEncoder.encodeError("ERR syntax error");
                    int slot = Integer.parseInt(new String(argv[2], StandardCharsets.UTF_8));
                    // Count keys in this slot in db[0]
                    long count = countKeysInSlot(slot);
                    return RespEncoder.encodeInteger(count);
                }
                case "ADDSLOTS": {
                    for (int i = 2; i < argv.length; i++) {
                        int slot = Integer.parseInt(new String(argv[i], StandardCharsets.UTF_8));
                        clusterState.assignSlots(nodeId, slot, slot);
                    }
                    return RespEncoder.OK;
                }
                case "DELSLOTS": {
                    for (int i = 2; i < argv.length; i++) {
                        int slot = Integer.parseInt(new String(argv[i], StandardCharsets.UTF_8));
                        selfInfo.getSlots().clear(slot);
                    }
                    return RespEncoder.OK;
                }
                case "ADDSLOTSRANGE":
                case "DELSLOTSRANGE": {
                    if (argv.length < 4) return RespEncoder.encodeError("ERR syntax error");
                    int from = Integer.parseInt(new String(argv[2], StandardCharsets.UTF_8));
                    int to = Integer.parseInt(new String(argv[3], StandardCharsets.UTF_8));
                    if (sub.equals("ADDSLOTSRANGE")) {
                        clusterState.assignSlots(nodeId, from, to);
                    } else {
                        selfInfo.getSlots().clear(from, to + 1);
                    }
                    return RespEncoder.OK;
                }
                case "MEET": {
                    if (argv.length < 4) return RespEncoder.encodeError("ERR syntax error");
                    String meetHost = new String(argv[2], StandardCharsets.UTF_8);
                    int meetPort;
                    try {
                        meetPort = Integer.parseInt(new String(argv[3], StandardCharsets.UTF_8));
                    } catch (NumberFormatException e) {
                        return RespEncoder.encodeError("ERR Invalid port");
                    }
                    // Use GossipManager for real network MEET
                    node.gossipMeet(meetHost, meetPort);
                    return RespEncoder.OK;
                }
                case "FORGET":
                    return RespEncoder.OK;
                case "REPLICATE":
                    return RespEncoder.OK;
                case "RESET":
                    return RespEncoder.OK;
                case "FAILOVER":
                    return RespEncoder.OK;
                case "SETSLOT":
                    return RespEncoder.OK;
                case "GETKEYSINSLOT": {
                    if (argv.length < 4) return RespEncoder.encodeError("ERR syntax error");
                    int slot = Integer.parseInt(new String(argv[2], StandardCharsets.UTF_8));
                    int count = Integer.parseInt(new String(argv[3], StandardCharsets.UTF_8));
                    return getKeysInSlot(slot, count);
                }
                case "SLOTS":
                    return clusterSlots();
                case "SHARDS":
                    return RespEncoder.EMPTY_ARRAY;
                case "COUNT-FAILURE-REPORTS":
                    return RespEncoder.encodeInteger(0);
                default:
                    return RespEncoder.encodeError("ERR unknown CLUSTER subcommand '" + sub + "'");
            }
        }

        private byte[] clusterInfo() {
            // State is "ok" if this node has slots assigned and is participating in the cluster
            boolean hasSlots = selfInfo.getSlots().cardinality() > 0;
            String state = hasSlots ? "ok" : "fail";
            int nodeCount = clusterState.allNodes().size();
            StringBuilder sb = new StringBuilder();
            sb.append("cluster_enabled:1\r\n");
            sb.append("cluster_state:").append(state).append("\r\n");
            sb.append("cluster_slots_assigned:16384\r\n");
            sb.append("cluster_slots_ok:16384\r\n");
            sb.append("cluster_slots_pfail:0\r\n");
            sb.append("cluster_slots_fail:0\r\n");
            sb.append("cluster_known_nodes:").append(nodeCount).append("\r\n");
            sb.append("cluster_size:").append(nodeCount).append("\r\n");
            sb.append("cluster_current_epoch:0\r\n");
            sb.append("cluster_my_epoch:0\r\n");
            sb.append("cluster_stats_messages_sent:0\r\n");
            sb.append("cluster_stats_messages_received:0\r\n");
            sb.append("total_cluster_links_buffer_limit_exceeded:0\r\n");
            return RespEncoder.encodeBulkString(sb.toString().getBytes(StandardCharsets.UTF_8));
        }

        private byte[] clusterNodes() {
            StringBuilder sb = new StringBuilder();
            // Always include self first
            sb.append(selfInfo.toNodesLine()).append("\n");
            for (ClusterNodeInfo n : clusterState.allNodes()) {
                if (n.isSelf()) continue;
                // Include all nodes including temp placeholders
                // (temp nodes have nodeId starting with "temp-", but we still emit them
                // so that CLUSTER NODES output contains port info for discovery)
                sb.append(n.toNodesLine()).append("\n");
            }
            return RespEncoder.encodeBulkString(sb.toString().getBytes(StandardCharsets.UTF_8));
        }

        private byte[] clusterSlots() {
            // *N\r\n where each entry is *3\r\n :start :end *2\r\n $host :port
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            try {
                List<ClusterNodeInfo> masters = new ArrayList<>();
                for (ClusterNodeInfo n : clusterState.allNodes()) {
                    if (!n.getNodeId().startsWith("temp-") && n.getSlots().cardinality() > 0) {
                        masters.add(n);
                    }
                }
                baos.write(("*" + masters.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
                for (ClusterNodeInfo n : masters) {
                    java.util.BitSet bs = n.getSlots();
                    int start = bs.nextSetBit(0);
                    if (start == -1) continue;
                    int end = bs.nextClearBit(start) - 1;
                    baos.write("*3\r\n".getBytes(StandardCharsets.UTF_8));
                    baos.write(RespEncoder.encodeInteger(start));
                    baos.write(RespEncoder.encodeInteger(end));
                    baos.write("*2\r\n".getBytes(StandardCharsets.UTF_8));
                    baos.write(RespEncoder.encodeBulkString(n.getHost().getBytes(StandardCharsets.UTF_8)));
                    baos.write(RespEncoder.encodeInteger(n.getPort()));
                }
                return baos.toByteArray();
            } catch (java.io.IOException e) {
                return RespEncoder.NULL_BULK;
            }
        }

        private long countKeysInSlot(int slot) {
            long count = 0;
            for (int i = 0; i < server.getNumDatabases(); i++) {
                for (byte[] key : server.getDb(i).allKeys()) {
                    if (CRC16.keyHashSlot(key) == slot) count++;
                }
            }
            return count;
        }

        private byte[] getKeysInSlot(int slot, int maxCount) {
            List<byte[]> keys = new ArrayList<>();
            for (byte[] key : server.getDb(0).allKeys()) {
                if (CRC16.keyHashSlot(key) == slot) {
                    keys.add(key);
                    if (keys.size() >= maxCount) break;
                }
            }
            List<Object> result = new ArrayList<>(keys);
            return RespEncoder.encodeArray(result);
        }
    }
}
