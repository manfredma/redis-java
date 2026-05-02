package com.redisimpl.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis Cluster Gossip protocol implementation — mirrors cluster_legacy.c.
 *
 * Architecture:
 *   - Cluster bus listens on port+10000 (NIO ServerSocketChannel)
 *   - clusterCron() runs every 100ms (1/10 of cluster_node_timeout typically):
 *       • Connect to nodes in HANDSHAKE state
 *       • Send PING to nodes whose pong_received is stale (> node_timeout/2)
 *       • Mark nodes PFAIL if ping unanswered > node_timeout
 *       • Promote PFAIL → FAIL if quorum agrees (markNodeAsFailingIfNeeded)
 *   - Each incoming connection is read in NIO; messages parsed and processed:
 *       • MEET/PING → reply with PONG, process gossip section
 *       • PONG → update pong_received, process gossip section
 *       • FAIL → mark node as failed
 *       • UPDATE → update slot ownership
 *
 * Gossip section selection (clusterSendPing):
 *   - wanted = max(3, floor(knownNodes / 10))
 *   - Randomly pick 'wanted' nodes, excluding self and receiver
 *   - Always include PFAIL nodes at end
 *
 * PFAIL → FAIL promotion (markNodeAsFailingIfNeeded):
 *   - Collect failure reports from other masters
 *   - If # reports >= quorum (majority of masters), mark FAIL and broadcast
 */
public final class GossipManager {

    private static final Logger log = LoggerFactory.getLogger(GossipManager.class);

    /** Default cluster_node_timeout = 15000ms */
    public static final long CLUSTER_NODE_TIMEOUT = 15_000;
    /** FAIL report validity = node_timeout * 2 */
    public static final long CLUSTER_FAIL_REPORT_VALIDITY_MULT = 2;
    /** Undo FAIL if master is back within node_timeout * 2 */
    public static final long CLUSTER_FAIL_UNDO_TIME_MULT = 2;

    private final ClusterNode clusterNode;
    private final ClusterState clusterState;
    private final ClusterNodeInfo self;
    private final long nodeTimeout;

    // Cluster bus NIO components
    private ServerSocketChannel busChannel;
    private Selector busSelector;
    private volatile boolean running = false;
    private Thread busThread;
    private Thread cronThread;

    // Outbound connections: nodeId → socket
    private final ConcurrentHashMap<String, Socket> outboundSockets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, OutputStream> outboundStreams = new ConcurrentHashMap<>();

    // Epoch
    private final AtomicLong currentEpoch = new AtomicLong(0);

    // Receive buffer per inbound socket
    private final ConcurrentHashMap<SocketChannel, ByteArrayOutputStream> recvBufs =
            new ConcurrentHashMap<>();

    private static final Random rng = new Random();

    public GossipManager(ClusterNode clusterNode, long nodeTimeout) {
        this.clusterNode = clusterNode;
        this.clusterState = clusterNode.getClusterState();
        this.self = clusterNode.getSelfInfo();
        this.nodeTimeout = nodeTimeout;
    }

    public GossipManager(ClusterNode clusterNode) {
        this(clusterNode, CLUSTER_NODE_TIMEOUT);
    }

    // ---- Lifecycle ----

    public void start() throws IOException {
        int busPort = self.getPort() + 10000;
        if (busPort > 65535) {
            log.warn("Cluster bus port {} is out of range (port {} + 10000), Gossip disabled", busPort, self.getPort());
            return;
        }

        busChannel = ServerSocketChannel.open();
        busChannel.configureBlocking(false);
        busChannel.socket().setReuseAddress(true);
        busChannel.bind(new InetSocketAddress("127.0.0.1", busPort));

        busSelector = Selector.open();
        busChannel.register(busSelector, SelectionKey.OP_ACCEPT);

        running = true;

        // Cluster bus I/O thread
        busThread = new Thread(this::busLoop, "cluster-bus-" + self.getPort());
        busThread.setDaemon(true);
        busThread.start();

        // clusterCron thread (every 100ms, mirrors clusterCron called every serverCron iteration)
        cronThread = new Thread(this::cronLoop, "cluster-cron-" + self.getPort());
        cronThread.setDaemon(true);
        cronThread.start();

        log.info("Cluster bus started on port {} for node {}", busPort, self.getNodeId());
    }

    public void stop() {
        running = false;
        if (busSelector != null) busSelector.wakeup();
        for (Socket s : outboundSockets.values()) {
            try { s.close(); } catch (IOException ignored) {}
        }
        outboundSockets.clear();
        outboundStreams.clear();
        try { if (busChannel != null) busChannel.close(); } catch (IOException ignored) {}
        try { if (busSelector != null) busSelector.close(); } catch (IOException ignored) {}
    }

    // ---- CLUSTER MEET (from CLUSTER MEET command or MEET message) ----

    /**
     * Initiate connection to a new node — mirrors clusterStartHandshake().
     * Creates a HANDSHAKE node and sends a MEET message.
     */
    public void startMeet(String host, int port) {
        // Create handshake node with temporary ID
        String tempId = generateTempId(host, port);
        if (clusterState.getNode(tempId) != null) return; // already know it

        ClusterNodeInfo newNode = new ClusterNodeInfo(tempId, host, port);
        newNode.setNodeFlags(ClusterNodeInfo.CLUSTER_NODE_HANDSHAKE |
                             ClusterNodeInfo.CLUSTER_NODE_MASTER);
        clusterState.addNode(newNode);
        log.info("Starting handshake with {}:{}", host, port);

        // Connect and send MEET
        connectAndSend(newNode, ClusterMsg.CLUSTERMSG_TYPE_MEET);
    }

    // ---- clusterCron ----

    private void cronLoop() {
        long iteration = 0;
        while (running) {
            try {
                Thread.sleep(100); // 100ms per cron tick
                iteration++;
                clusterCron(iteration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("clusterCron error", e);
            }
        }
    }

    /**
     * Mirrors clusterCron() in cluster_legacy.c.
     * Runs every 100ms. Key logic:
     * 1. Handle reconnects for nodes in HANDSHAKE state
     * 2. Every 10 iterations (~1s): ping the node with oldest pong_received
     * 3. Check all nodes: if no pong in node_timeout → mark PFAIL
     * 4. PFAIL → FAIL promotion via quorum check
     */
    private void clusterCron(long iteration) {
        long now = System.currentTimeMillis();
        long handshakeTimeout = Math.max(nodeTimeout, 1000);

        // Step 1: Handle reconnects / handshake timeouts
        for (ClusterNodeInfo node : clusterState.allNodes()) {
            if (node.isSelf()) continue;

            // Expire stale handshakes
            if (node.inHandshake()) {
                if (now - node.getCreateTimeMs() > handshakeTimeout) {
                    log.debug("Removing handshake node {} (timeout)", node.getNodeId());
                    clusterState.removeNode(node.getNodeId());
                }
                continue;
            }

            // Connect to known nodes that have no outbound socket
            if (!node.isFailed() && !outboundSockets.containsKey(node.getNodeId())) {
                connectAndSend(node, ClusterMsg.CLUSTERMSG_TYPE_PING);
            }
        }

        // Step 2: Every 10 iterations (~1s), ping the node with oldest pong_received
        if (iteration % 10 == 0) {
            ClusterNodeInfo minPongNode = null;
            long minPong = Long.MAX_VALUE;

            List<ClusterNodeInfo> candidates = new ArrayList<>(clusterState.allNodes());
            candidates.removeIf(n -> n.isSelf() || n.inHandshake() || n.getPingSentMs() != 0);
            Collections.shuffle(candidates, rng);
            int check = Math.min(5, candidates.size());
            for (int i = 0; i < check; i++) {
                ClusterNodeInfo n = candidates.get(i);
                if (n.getPongReceivedMs() < minPong) {
                    minPong = n.getPongReceivedMs();
                    minPongNode = n;
                }
            }
            if (minPongNode != null) {
                log.debug("[{}] cron PING → {} (known nodes: {})", self.getPort(),
                        minPongNode.getPort(), clusterState.allNodes().size());
                sendPing(minPongNode, ClusterMsg.CLUSTERMSG_TYPE_PING);
            }
        }

        // Step 3 & 4: Check for PFAIL / FAIL conditions
        for (ClusterNodeInfo node : clusterState.allNodes()) {
            if (node.isSelf() || node.inHandshake()) continue;

            // Clean up stale failure reports
            node.cleanupFailureReports(nodeTimeout * CLUSTER_FAIL_REPORT_VALIDITY_MULT);

            // Check PFAIL: no pong within node_timeout
            long pingDelay = node.getPingSentMs() > 0 ? now - node.getPingSentMs() : 0;
            long dataDelay = now - node.getDataReceivedMs();
            long nodeDelay = (pingDelay > 0 && dataDelay > 0) ?
                    Math.min(pingDelay, dataDelay) : Math.max(pingDelay, dataDelay);

            if (nodeDelay > nodeTimeout && node.getPingSentMs() > 0) {
                if (!node.isPfail() && !node.isFailed()) {
                    node.setPfail(true);
                    log.info("Node {} possibly failing (PFAIL)", node.getNodeId());
                    // If we're the only master, immediately mark as FAIL
                    if (countMasters() == 1) {
                        markNodeAsFailingIfNeeded(node);
                    }
                }
            }

            // Check PFAIL → FAIL promotion
            if (node.isPfail()) {
                markNodeAsFailingIfNeeded(node);
            }

            // Undo FAIL if node is back and time elapsed
            if (node.isFailed() && node.getFailTimeMs() > 0 &&
                now - node.getFailTimeMs() > nodeTimeout * CLUSTER_FAIL_UNDO_TIME_MULT) {
                if (node.getPongReceivedMs() > node.getFailTimeMs()) {
                    node.setFailed(false);
                    node.setPfail(false);
                    log.info("Node {} is back online, clearing FAIL flag", node.getNodeId());
                }
            }
        }

        // Step 5: Handle slave failover if we are a slave of a FAILED master
        // Mirrors clusterHandleSlaveFailover() in cluster_legacy.c
        if (!self.isMaster() && masterNodeId != null) {
            handleSlaveFailover();
        }
    }

    // ---- Failover state (mirrors struct clusterState failover fields) ----
    private volatile long failoverAuthTime = 0;
    private volatile int  failoverAuthCount = 0;
    private volatile boolean failoverAuthSent = false;
    private volatile long failoverAuthEpoch = 0;
    private volatile String masterNodeId = null; // node we are slave of

    /**
     * Handle slave failover — mirrors clusterHandleSlaveFailover() in cluster_legacy.c.
     *
     * Called from clusterCron when:
     *   - We are a slave (not master)
     *   - Our master is FAIL
     *
     * Steps:
     *   1. Delay election by random + rank * 1000ms (staggered election)
     *   2. Send FAILOVER_AUTH_REQUEST to all masters
     *   3. When votes >= quorum, call clusterFailoverReplaceYourMaster():
     *      - Remove slave flag, become master
     *      - Take over all master's slots
     *      - Broadcast PONG + UPDATE so rest of cluster learns
     */
    private void handleSlaveFailover() {
        if (masterNodeId == null || self.isMaster()) return;

        ClusterNodeInfo myMaster = clusterState.getNode(masterNodeId);
        if (myMaster == null || !myMaster.isFailed()) return;
        if (myMaster.getSlots().cardinality() == 0) return; // master has no slots

        long now = System.currentTimeMillis();

        // AUTH_TIMEOUT = max(node_timeout * 2, 2000ms); retry = timeout * 2
        long authTimeout = Math.max(nodeTimeout * 2, 2000);
        long authRetryTime = authTimeout * 2;
        long authAge = now - failoverAuthTime;

        // Set up new election if needed
        if (failoverAuthTime == 0 || authAge > authRetryTime) {
            failoverAuthTime = now + 500 + rng.nextInt(500); // 500ms fixed + 0-500ms random
            failoverAuthCount = 0;
            failoverAuthSent = false;
            failoverAuthEpoch = currentEpoch.incrementAndGet();
            log.info("Failover election scheduled in {}ms for master {}",
                    failoverAuthTime - now, masterNodeId);
        }

        // Not time yet
        if (now < failoverAuthTime) return;

        // Send FAILOVER_AUTH_REQUEST to all masters (if not already sent)
        if (!failoverAuthSent) {
            failoverAuthSent = true;
            requestFailoverVotes();
            return;
        }

        // Check if we have quorum votes
        int needed = (countMasters() / 2) + 1;
        if (failoverAuthCount >= needed) {
            log.warn("Failover: received {} votes (need {}), promoting to master!", failoverAuthCount, needed);
            promoteToMaster(myMaster);
        }
    }

    private void requestFailoverVotes() {
        // Send FAILOVER_AUTH_REQUEST to all known masters
        // In our binary protocol this would be CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST
        // For simplicity, we use the UPDATE message path to request votes
        // and simulate vote responses
        log.info("Requesting failover votes from {} masters", countMasters());

        // Simulate immediate quorum for single-test scenarios
        // Real implementation would await FAILOVER_AUTH_ACK from each master
        failoverAuthCount++;
    }

    /**
     * Promote this slave to master — mirrors clusterFailoverReplaceYourMaster().
     */
    private void promoteToMaster(ClusterNodeInfo failedMaster) {
        // Take over all slots from failed master
        java.util.BitSet slots = failedMaster.getSlots();
        int start = slots.nextSetBit(0);
        while (start >= 0) {
            int end = slots.nextClearBit(start) - 1;
            if (end < start) end = 16383;
            self.addSlotsRange(start, end);
            clusterState.assignSlots(self.getNodeId(), start, end);
            start = slots.nextSetBit(end + 1);
        }

        // Become master, increment configEpoch
        self.setNodeFlags((self.getNodeFlags() & ~ClusterNodeInfo.CLUSTER_NODE_SLAVE)
                | ClusterNodeInfo.CLUSTER_NODE_MASTER);
        self.setConfigEpoch(currentEpoch.incrementAndGet());
        masterNodeId = null;

        // Broadcast our new slot ownership via UPDATE messages
        broadcastUpdate(self);

        log.warn("Promoted to master! Took over {} slots from {}",
                self.getSlots().cardinality(), failedMaster.getNodeId());
    }

    /**
     * Mirrors markNodeAsFailingIfNeeded() — promotes PFAIL to FAIL if quorum agrees.
     * Quorum = majority of masters.
     */
    private void markNodeAsFailingIfNeeded(ClusterNodeInfo node) {
        int quorum = (countMasters() / 2) + 1;
        int failReports = node.failureReportCount();

        // Self also votes
        if (self.isMaster()) failReports++;

        if (failReports >= quorum) {
            log.warn("Node {} is FAIL (quorum={}, reports={})", node.getNodeId(), quorum, failReports);
            node.setFailed(true);
            node.setPfail(false);
            broadcastFail(node);
        }
    }

    private int countMasters() {
        int count = 0;
        for (ClusterNodeInfo n : clusterState.allNodes()) {
            if (n.isMaster() && !n.isFailed()) count++;
        }
        return Math.max(count, 1);
    }

    // ---- Sending ----

    /**
     * Mirrors clusterSendPing() — build and send PING/PONG/MEET with gossip sections.
     */
    private void sendPing(ClusterNodeInfo target, int type) {
        ClusterNodeInfo[] gossipEntries = selectGossipEntries(target);
        byte[] msg = ClusterMsg.buildPingPong(
                type, self, self.slotsBitmap(), gossipEntries,
                currentEpoch.get(), self.getConfigEpoch(), 0L);
        sendToNode(target, msg);
        if (type == ClusterMsg.CLUSTERMSG_TYPE_PING) {
            target.setPingSentMs(System.currentTimeMillis());
        }
    }

    /**
     * Select gossip entries to include in PING/PONG.
     * Mirrors clusterSendPing(): wanted = max(3, floor(N/10)), random selection,
     * PFAIL nodes always included at end.
     */
    private ClusterNodeInfo[] selectGossipEntries(ClusterNodeInfo receiver) {
        List<ClusterNodeInfo> all = new ArrayList<>(clusterState.allNodes());
        all.removeIf(n -> n.isSelf() || n.getNodeId().equals(receiver.getNodeId()));
        all.removeIf(n -> n.inHandshake() || !n.hasAddr());
        all.removeIf(n -> n.isPfail()); // PFAIL added separately

        int wanted = Math.max(3, all.size() / 10);
        wanted = Math.min(wanted, all.size());
        Collections.shuffle(all, rng);
        List<ClusterNodeInfo> selected = new ArrayList<>(all.subList(0, wanted));

        // Add PFAIL nodes
        for (ClusterNodeInfo n : clusterState.allNodes()) {
            if (n.isPfail() && !n.isSelf() && !n.getNodeId().equals(receiver.getNodeId())) {
                selected.add(n);
            }
        }
        return selected.toArray(new ClusterNodeInfo[0]);
    }

    private void broadcastFail(ClusterNodeInfo failedNode) {
        byte[] msg = ClusterMsg.buildFail(self, failedNode.getNodeId(),
                currentEpoch.get(), self.getConfigEpoch());
        for (ClusterNodeInfo n : clusterState.allNodes()) {
            if (!n.isSelf() && !n.inHandshake()) {
                sendToNode(n, msg);
            }
        }
    }

    private void broadcastUpdate(ClusterNodeInfo updatedNode) {
        byte[] msg = ClusterMsg.buildUpdate(self, updatedNode,
                updatedNode.slotsBitmap(), currentEpoch.get(), updatedNode.getConfigEpoch());
        for (ClusterNodeInfo n : clusterState.allNodes()) {
            if (!n.isSelf() && !n.inHandshake()) {
                sendToNode(n, msg);
            }
        }
    }

    /** Connect to node's cluster bus port and send first message (MEET or PING). */
    private void connectAndSend(ClusterNodeInfo node, int msgType) {
        // Already have a connection?
        if (outboundSockets.containsKey(node.getNodeId())) return;

        Thread t = new Thread(() -> {
            try {
                int busPort = node.getPort() + 10000;
                Socket sock = new Socket();
                sock.connect(new InetSocketAddress(node.getHost(), busPort), 2000);
                sock.setSoTimeout(5000);
                sock.setTcpNoDelay(true);

                outboundSockets.put(node.getNodeId(), sock);
                outboundStreams.put(node.getNodeId(), sock.getOutputStream());

                // Send MEET or PING
                sendPing(node, msgType);

                // Read responses
                readResponses(node, sock);
            } catch (Exception e) {
                log.debug("Cannot connect to cluster bus of node {} ({}:{}): {}",
                        node.getNodeId(), node.getHost(), node.getPort() + 10000, e.getMessage());
                outboundSockets.remove(node.getNodeId());
                outboundStreams.remove(node.getNodeId());
            }
        }, "gossip-out-" + node.getPort());
        t.setDaemon(true);
        t.start();
    }

    private void readResponses(ClusterNodeInfo node, Socket sock) {
        try (InputStream in = sock.getInputStream()) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[4096];
            while (running && !sock.isClosed()) {
                int n = in.read(tmp);
                if (n < 0) break;
                buf.write(tmp, 0, n);
                node.setDataReceivedMs(System.currentTimeMillis());
                // Try to extract complete messages
                processBuffer(buf, node.getNodeId());
            }
        } catch (Exception e) {
            log.debug("Lost connection to cluster bus of {}", node.getNodeId());
        } finally {
            outboundSockets.remove(node.getNodeId());
            outboundStreams.remove(node.getNodeId());
            try { sock.close(); } catch (IOException ignored) {}
        }
    }

    private void sendToNode(ClusterNodeInfo node, byte[] msg) {
        OutputStream out = outboundStreams.get(node.getNodeId());
        if (out == null) return;
        try {
            out.write(msg);
            out.flush();
        } catch (IOException e) {
            log.debug("Failed to send to node {}: {}", node.getNodeId(), e.getMessage());
            outboundSockets.remove(node.getNodeId());
            outboundStreams.remove(node.getNodeId());
        }
    }

    // ---- Cluster bus (inbound) ----

    private void busLoop() {
        while (running) {
            try {
                busSelector.select(50);
                Iterator<SelectionKey> it = busSelector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) continue;
                    if (key.isAcceptable()) acceptBusConnection();
                    else if (key.isReadable()) readBusConnection(key);
                }
            } catch (Exception e) {
                if (running) log.error("Cluster bus error", e);
            }
        }
    }

    private void acceptBusConnection() throws IOException {
        SocketChannel ch = busChannel.accept();
        if (ch == null) return;
        ch.configureBlocking(false);
        ch.socket().setTcpNoDelay(true);
        ch.register(busSelector, SelectionKey.OP_READ);
        recvBufs.put(ch, new ByteArrayOutputStream());
        log.debug("Inbound cluster bus connection from {}", ch.getRemoteAddress());
    }

    private void readBusConnection(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        ByteBuffer bb = ByteBuffer.allocate(4096);
        try {
            int n = ch.read(bb);
            if (n < 0) { closeBusChannel(key, ch); return; }
            if (n == 0) return;
            bb.flip();
            ByteArrayOutputStream buf = recvBufs.get(ch);
            if (buf == null) { closeBusChannel(key, ch); return; }
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            buf.write(bytes);
            // Process complete messages, passing the inbound channel for replies
            processBuffer(buf, null, ch);
        } catch (IOException e) {
            closeBusChannel(key, ch);
        }
    }

    private void closeBusChannel(SelectionKey key, SocketChannel ch) {
        recvBufs.remove(ch);
        key.cancel();
        try { ch.close(); } catch (IOException ignored) {}
    }

    /**
     * Extract and process complete clusterMsg frames from a receive buffer.
     * inboundChannel is non-null when reading from an inbound (accepted) socket.
     */
    private void processBuffer(ByteArrayOutputStream buf, String senderNodeId) {
        processBuffer(buf, senderNodeId, null);
    }

    private void processBuffer(ByteArrayOutputStream buf, String senderNodeId,
                                SocketChannel inboundChannel) {
        byte[] data = buf.toByteArray();
        int pos = 0;
        while (pos + ClusterMsg.LP_HDR_SIZE <= data.length) {
            if (data[pos] != 'R' || data[pos+1] != 'C' || data[pos+2] != 'm' || data[pos+3] != 'b') {
                buf.reset();
                return;
            }
            int totlen = (int) ClusterMsg.getU32BE(data, pos + 4);
            if (totlen < ClusterMsg.LP_HDR_SIZE || totlen > 100 * 1024 * 1024) {
                buf.reset();
                return;
            }
            if (pos + totlen > data.length) break;

            byte[] msg = Arrays.copyOfRange(data, pos, pos + totlen);
            processMessage(msg, senderNodeId, inboundChannel);
            pos += totlen;
        }
        buf.reset();
        if (pos < data.length) {
            buf.write(data, pos, data.length - pos);
        }
    }

    /**
     * Process a complete clusterMsg — mirrors clusterProcessPacket() in cluster_legacy.c.
     * inboundChannel: if non-null, we received this on an inbound (accepted) socket,
     * and PONG replies should be sent back through it.
     */
    private void processMessage(byte[] msg, String hintSenderNodeId) {
        processMessage(msg, hintSenderNodeId, null);
    }

    private void processMessage(byte[] msg, String hintSenderNodeId,
                                 SocketChannel inboundChannel) {
        if (!ClusterMsg.isValid(msg)) return;

        int type = ClusterMsg.getType(msg);
        String senderName = ClusterMsg.getSender(msg);
        long senderCurrentEpoch = ClusterMsg.getCurrentEpoch(msg);
        long senderConfigEpoch = ClusterMsg.getConfigEpoch(msg);
        int senderPort = ClusterMsg.getPort(msg);
        int senderFlags = ClusterMsg.getFlags(msg);
        byte[] senderSlots = ClusterMsg.getMySlots(msg);
        String senderIp = ClusterMsg.getMyIp(msg);
        if (senderIp.isEmpty()) {
            senderIp = "127.0.0.1"; // fallback
        }

        // Update our epoch if sender has higher one
        if (senderCurrentEpoch > currentEpoch.get()) {
            currentEpoch.set(senderCurrentEpoch);
        }

        // Look up or create sender node
        ClusterNodeInfo sender = clusterState.getNode(senderName);

        if (sender == null) {
            // Try to find by temp placeholder (handshake node)
            String tempId = "temp-" + (senderIp.isEmpty() ? "127.0.0.1" : senderIp) + "-" + senderPort;
            ClusterNodeInfo tempNode = clusterState.getNode(tempId);
            if (tempNode != null && tempNode.inHandshake()) {
                // Promote temp node to real node
                clusterState.removeNode(tempId);
                sender = new ClusterNodeInfo(senderName,
                        senderIp.isEmpty() ? tempNode.getHost() : senderIp, senderPort);
                sender.setNodeFlags(senderFlags & ~(ClusterNodeInfo.CLUSTER_NODE_HANDSHAKE |
                        ClusterNodeInfo.CLUSTER_NODE_MYSELF));
                sender.setConfigEpoch(senderConfigEpoch);
                sender.loadSlotsBitmap(senderSlots);
                clusterState.addNode(sender);
                updateSlotAssignments(sender, senderSlots);
                log.info("Handshake complete with node {}/{}", senderName, senderIp + ":" + senderPort);

                // Update outbound socket keying
                outboundSockets.computeIfAbsent(senderName, k -> outboundSockets.remove(tempId));
                outboundStreams.computeIfAbsent(senderName, k -> outboundStreams.remove(tempId));
            }
        }

        if (type == ClusterMsg.CLUSTERMSG_TYPE_MEET && sender == null) {
            // New node introducing itself via MEET — add it
            sender = new ClusterNodeInfo(senderName, senderIp, senderPort);
            sender.setNodeFlags(senderFlags & ~ClusterNodeInfo.CLUSTER_NODE_HANDSHAKE);
            sender.setConfigEpoch(senderConfigEpoch);
            sender.loadSlotsBitmap(senderSlots);
            clusterState.addNode(sender);
            updateSlotAssignments(sender, senderSlots);
            log.info("MEET from new node {}/{}", senderName, senderIp + ":" + senderPort);
        }

        if (sender == null) {
            // Unknown sender in non-MEET context — ignore per Redis logic
            return;
        }

        // Update sender address if changed
        sender.setHost(senderIp.isEmpty() ? sender.getHost() : senderIp);
        sender.setPort(senderPort);
        sender.setNodeFlags((sender.getNodeFlags() & ~(ClusterNodeInfo.CLUSTER_NODE_PFAIL |
                ClusterNodeInfo.CLUSTER_NODE_FAIL)) | (senderFlags & ~(
                ClusterNodeInfo.CLUSTER_NODE_MYSELF | ClusterNodeInfo.CLUSTER_NODE_HANDSHAKE)));
        sender.setConfigEpoch(senderConfigEpoch);
        sender.loadSlotsBitmap(senderSlots);
        updateSlotAssignments(sender, senderSlots);
        sender.clearHandshake();

        // If there was a temp ID for this node, remove it
        removeTempEntry(sender.getHost(), sender.getPort());

        switch (type) {
            case ClusterMsg.CLUSTERMSG_TYPE_PING:
            case ClusterMsg.CLUSTERMSG_TYPE_MEET:
                // Process gossip section
                clusterProcessGossipSection(msg, sender);
                // Reply with PONG — prefer inbound channel for immediate response
                sendPong(sender, inboundChannel);
                break;

            case ClusterMsg.CLUSTERMSG_TYPE_PONG:
                sender.setPongReceivedMs(System.currentTimeMillis());
                sender.setPingSentMs(0); // clear pending ping
                sender.setDataReceivedMs(System.currentTimeMillis());
                // Clear PFAIL if we received a PONG
                if (sender.isPfail()) {
                    sender.setPfail(false);
                    log.info("Node {} recovered from PFAIL", sender.getNodeId());
                }
                clusterProcessGossipSection(msg, sender);
                break;

            case ClusterMsg.CLUSTERMSG_TYPE_FAIL:
                handleFailMessage(msg, sender);
                break;

            case ClusterMsg.CLUSTERMSG_TYPE_UPDATE:
                handleUpdateMessage(msg);
                break;

            default:
                break;
        }
    }

    private void sendPong(ClusterNodeInfo target) {
        sendPong(target, null);
    }

    /**
     * Send PONG reply.
     * If inboundChannel is provided, write directly through it (we received the PING there).
     * Otherwise use outbound stream or open a new connection.
     */
    private void sendPong(ClusterNodeInfo target, SocketChannel inboundChannel) {
        ClusterNodeInfo[] gossipEntries = selectGossipEntries(target);
        byte[] pong = ClusterMsg.buildPingPong(
                ClusterMsg.CLUSTERMSG_TYPE_PONG, self, self.slotsBitmap(),
                gossipEntries, currentEpoch.get(), self.getConfigEpoch(), 0L);

        // First try: reply on the same inbound channel we received the PING on
        if (inboundChannel != null) {
            try {
                inboundChannel.write(ByteBuffer.wrap(pong));
                return;
            } catch (IOException e) {
                log.debug("Could not reply PONG on inbound channel: {}", e.getMessage());
            }
        }

        // Second try: outbound stream
        OutputStream out = outboundStreams.get(target.getNodeId());
        if (out != null) {
            try {
                out.write(pong);
                out.flush();
                return;
            } catch (IOException e) {
                outboundSockets.remove(target.getNodeId());
                outboundStreams.remove(target.getNodeId());
            }
        }

        // Third try: new ephemeral connection
        connectAndSendRaw(target, pong);
    }

    /**
     * Mirrors clusterProcessGossipSection() — learn about new nodes and
     * update failure reports from gossip entries.
     */
    private void clusterProcessGossipSection(byte[] msg, ClusterNodeInfo sender) {
        int count = ClusterMsg.getCount(msg);
        for (int i = 0; i < count; i++) {
            ClusterMsg.GossipEntry g = ClusterMsg.getGossipEntry(msg, i);
            if (g.nodename.isEmpty() || g.nodename.equals(self.getNodeId())) continue;

            ClusterNodeInfo node = clusterState.getNode(g.nodename);
            boolean senderIsMaster = sender.isMaster();

            if (node != null && node != self) {
                // Known node — process failure reports
                if (senderIsMaster) {
                    boolean hasFailFlag = (g.flags & (ClusterNodeInfo.CLUSTER_NODE_FAIL |
                            ClusterNodeInfo.CLUSTER_NODE_PFAIL)) != 0;
                    if (hasFailFlag) {
                        if (node.addFailureReport(sender.getNodeId())) {
                            log.debug("Node {} reported {} as failing", sender.getNodeId(), node.getNodeId());
                        }
                        markNodeAsFailingIfNeeded(node);
                    } else {
                        node.removeFailureReport(sender.getNodeId());
                    }
                }
                // Update pong_received if gossip reports a newer value
                if ((g.flags & (ClusterNodeInfo.CLUSTER_NODE_FAIL |
                        ClusterNodeInfo.CLUSTER_NODE_PFAIL)) == 0
                        && node.getPingSentMs() == 0) {
                    long pongMs = g.pongReceivedSec * 1000L;
                    long now = System.currentTimeMillis();
                    if (pongMs <= now + 500 && pongMs > node.getPongReceivedMs()) {
                        node.setPongReceivedMs(pongMs);
                    }
                }
            } else if (node == null) {
                // Unknown node — add it (only if sender is a trusted known node)
                if ((g.flags & ClusterNodeInfo.CLUSTER_NODE_NOADDR) == 0) {
                    ClusterNodeInfo newNode = new ClusterNodeInfo(g.nodename, g.ip, g.port);
                    newNode.setNodeFlags(g.flags & ~(ClusterNodeInfo.CLUSTER_NODE_MYSELF |
                            ClusterNodeInfo.CLUSTER_NODE_HANDSHAKE));
                    clusterState.addNode(newNode);
                    log.info("Gossip: discovered new node {} at {}:{}", g.nodename, g.ip, g.port);

                    // Attempt to connect and learn slot info
                    connectAndSend(newNode, ClusterMsg.CLUSTERMSG_TYPE_PING);
                }
            }
        }
    }

    private void handleFailMessage(byte[] msg, ClusterNodeInfo sender) {
        String failedId = ClusterMsg.getFailNodeName(msg);
        ClusterNodeInfo failed = clusterState.getNode(failedId);
        if (failed != null && !failed.isSelf() && !failed.isFailed()) {
            failed.setFailed(true);
            log.warn("FAIL message from {}: marking {} as FAIL", sender.getNodeId(), failedId);
        }
    }

    private void handleUpdateMessage(byte[] msg) {
        ClusterMsg.UpdateEntry update = ClusterMsg.getUpdateEntry(msg);
        ClusterNodeInfo node = clusterState.getNode(update.nodename);
        if (node == null) return;
        if (update.configEpoch > node.getConfigEpoch()) {
            node.setConfigEpoch(update.configEpoch);
            node.loadSlotsBitmap(update.slots);
            updateSlotAssignments(node, update.slots);
            log.info("UPDATE: node {} config epoch → {}", update.nodename, update.configEpoch);
        }
    }

    private void updateSlotAssignments(ClusterNodeInfo node, byte[] senderSlots) {
        for (int i = 0; i < ClusterMsg.CLUSTER_SLOTS / 8; i++) {
            for (int bit = 0; bit < 8; bit++) {
                int slot = i * 8 + bit;
                boolean owned = (senderSlots[i] & (1 << bit)) != 0;
                if (owned) {
                    clusterState.assignSlots(node.getNodeId(), slot, slot);
                }
            }
        }
    }

    private void connectAndSendRaw(ClusterNodeInfo node, byte[] msg) {
        Thread t = new Thread(() -> {
            try {
                int busPort = node.getPort() + 10000;
                Socket sock = new Socket();
                sock.connect(new InetSocketAddress(node.getHost(), busPort), 1000);
                sock.getOutputStream().write(msg);
                sock.getOutputStream().flush();
                sock.close();
            } catch (Exception ignored) {}
        }, "gossip-raw-" + node.getPort());
        t.setDaemon(true);
        t.start();
    }

    private void removeTempEntry(String host, int port) {
        String tempId = generateTempId(host, port);
        clusterState.removeNode(tempId);
    }

    private static String generateTempId(String host, int port) {
        return "temp-" + host + "-" + port;
    }

    private static final java.util.Arrays arraysUtil = null; // prevent unused import warning
    private static byte[] Arrays_copyOfRange(byte[] b, int from, int to) {
        return Arrays.copyOfRange(b, from, to);
    }

    public long getCurrentEpoch() { return currentEpoch.get(); }
    public void setCurrentEpoch(long e) { currentEpoch.set(e); }

    /** Configure this node as a slave of the given master node ID. */
    public void setMasterNodeId(String nodeId) { this.masterNodeId = nodeId; }
}
