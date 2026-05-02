package com.redisimpl.cluster;

import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Redis Cluster Gossip protocol.
 *
 * Verifies that nodes discover each other purely via the cluster bus
 * (MEET/PING/PONG messages), without any in-process registration.
 * Slot info propagates via Gossip sections in PING/PONG messages.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GossipTest {

    private static ClusterNode node1;  // slots 0-5460
    private static ClusterNode node2;  // slots 5461-10922
    private static ClusterNode node3;  // slots 10923-16383

    private static int port1, port2, port3;
    private static JedisPool pool1, pool2, pool3;

    @BeforeAll
    static void setup() throws Exception {
        port1 = freePort();
        port2 = freePort();
        port3 = freePort();

        node1 = new ClusterNode(port1);
        node2 = new ClusterNode(port2);
        node3 = new ClusterNode(port3);

        // Assign slots to each node
        node1.addSlots(0, 5460);
        node2.addSlots(5461, 10922);
        node3.addSlots(10923, 16383);

        // Start all nodes
        Thread t1 = new Thread(() -> { try { node1.start(); } catch (Exception ignored) {} });
        Thread t2 = new Thread(() -> { try { node2.start(); } catch (Exception ignored) {} });
        Thread t3 = new Thread(() -> { try { node3.start(); } catch (Exception ignored) {} });
        t1.setDaemon(true); t2.setDaemon(true); t3.setDaemon(true);
        t1.start(); t2.start(); t3.start();

        waitForPort(port1, 5000);
        waitForPort(port2, 5000);
        waitForPort(port3, 5000);

        JedisPoolConfig cfg = new JedisPoolConfig();
        pool1 = new JedisPool(cfg, "127.0.0.1", port1, 5000);
        pool2 = new JedisPool(cfg, "127.0.0.1", port2, 5000);
        pool3 = new JedisPool(cfg, "127.0.0.1", port3, 5000);

        // Send CLUSTER MEET via Redis command to bootstrap Gossip
        // node1 meets node2 and node3 via CLUSTER MEET command
        try (Jedis j1 = pool1.getResource()) {
            j1.clusterMeet("127.0.0.1", port2);
            j1.clusterMeet("127.0.0.1", port3);
        }

        // Wait for Gossip to propagate — nodes exchange PING/PONG
        // Need enough time for:
        // 1. MEET to complete handshake (MEET→PONG)
        // 2. clusterCron to detect node3 is no longer in handshake state
        // 3. clusterCron to send PING from node1 to node2 with node3 in gossip
        // 4. node2 to discover node3 and connect
        Thread.sleep(8000);
    }

    @AfterAll
    static void tearDown() {
        if (pool1 != null) pool1.close();
        if (pool2 != null) pool2.close();
        if (pool3 != null) pool3.close();
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();
    }

    @Test
    @Order(1)
    void gossip_node1_discovers_all_peers() {
        try (Jedis j = pool1.getResource()) {
            String nodes = j.clusterNodes();
            // Should contain all 3 node IDs/ports after Gossip convergence
            assertTrue(nodes.contains(String.valueOf(port1)), "node1 knows itself");
            assertTrue(nodes.contains(String.valueOf(port2)),
                    "node1 should discover node2 via Gossip: " + nodes);
            assertTrue(nodes.contains(String.valueOf(port3)),
                    "node1 should discover node3 via Gossip: " + nodes);
        }
    }

    @Test
    @Order(2)
    void gossip_node2_discovers_node3_via_node1() {
        // node2 was only MEETed by node1, but should discover node3 via Gossip sections
        try (Jedis j = pool2.getResource()) {
            String nodes = j.clusterNodes();
            assertTrue(nodes.contains(String.valueOf(port3)),
                    "node2 should discover node3 via Gossip (not direct MEET): " + nodes);
        }
    }

    @Test
    @Order(3)
    void gossip_slot_info_propagates() {
        // node2 should know node3's slot range after Gossip
        try (Jedis j = pool2.getResource()) {
            String nodes = j.clusterNodes();
            // node3's entry should contain slot range 10923-16383
            assertTrue(nodes.contains("10923-16383"),
                    "Slot range should propagate via Gossip: " + nodes);
        }
    }

    @Test
    @Order(4)
    void gossip_cluster_info_shows_correct_node_count() {
        try (Jedis j = pool1.getResource()) {
            String info = j.clusterInfo();
            assertTrue(info.contains("cluster_known_nodes:3"),
                    "Should know 3 nodes after Gossip: " + info);
        }
    }

    @Test
    @Order(5)
    void gossip_moved_redirect_uses_gossip_learned_slot_owner() {
        // node2 learns node3 owns slot 12182 via Gossip
        // SET "foo" (slot 12182) on node2 should return MOVED → node3
        try (Jedis j = pool2.getResource()) {
            try {
                j.set("foo", "bar");
                fail("Expected MOVED to node3");
            } catch (redis.clients.jedis.exceptions.JedisMovedDataException e) {
                assertEquals(12182, e.getSlot());
                assertEquals(port3, e.getTargetNode().getPort());
            }
        }
    }

    @Test
    @Order(6)
    void gossip_ping_pong_updates_last_contact() throws Exception {
        // After waiting, each node's peer records should show recent last-pong-recv
        Thread.sleep(1500); // let another Gossip round complete
        try (Jedis j = pool1.getResource()) {
            String nodes = j.clusterNodes();
            // All nodes should be in "connected" state (not "handshake" or "fail")
            assertFalse(nodes.contains(",fail"), "No node should be in fail state: " + nodes);
            assertFalse(nodes.contains(",handshake"), "No node should be in handshake: " + nodes);
        }
    }

    // ---- helpers ----

    private static int freePort() throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            int p = 10000 + new java.util.Random().nextInt(45000);
            try (ServerSocket s = new ServerSocket(p)) {
                s.setReuseAddress(true);
                return p;
            } catch (IOException ignored) {}
        }
        throw new IOException("Could not find free port in range 10000-55000");
    }

    private static void waitForPort(int port, long ms) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) { return; }
            catch (IOException e) { Thread.sleep(50); }
        }
        throw new RuntimeException("Port " + port + " not ready");
    }
}
