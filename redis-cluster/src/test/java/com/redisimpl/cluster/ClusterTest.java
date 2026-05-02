package com.redisimpl.cluster;

import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Redis Cluster: slot assignment, CLUSTER commands,
 * MOVED redirection, and basic key routing.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterTest {

    // 3 masters, each owning ~5461 slots
    private static ClusterNode node1; // slots 0-5460
    private static ClusterNode node2; // slots 5461-10922
    private static ClusterNode node3; // slots 10923-16383

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

        // Assign slots
        node1.addSlots(0, 5460);
        node2.addSlots(5461, 10922);
        node3.addSlots(10923, 16383);

        // Register peers with full slot info (simulates Gossip exchange)
        node1.registerPeer(node2);
        node1.registerPeer(node3);
        node2.registerPeer(node1);
        node2.registerPeer(node3);
        node3.registerPeer(node1);
        node3.registerPeer(node2);

        Thread t1 = new Thread(() -> { try { node1.start(); } catch (Exception e) { e.printStackTrace(); } });
        Thread t2 = new Thread(() -> { try { node2.start(); } catch (Exception e) { e.printStackTrace(); } });
        Thread t3 = new Thread(() -> { try { node3.start(); } catch (Exception e) { e.printStackTrace(); } });
        t1.setDaemon(true); t2.setDaemon(true); t3.setDaemon(true);
        t1.start(); t2.start(); t3.start();

        waitForPort(port1, 5000);
        waitForPort(port2, 5000);
        waitForPort(port3, 5000);
        Thread.sleep(500);

        JedisPoolConfig cfg = new JedisPoolConfig();
        pool1 = new JedisPool(cfg, "127.0.0.1", port1, 3000);
        pool2 = new JedisPool(cfg, "127.0.0.1", port2, 3000);
        pool3 = new JedisPool(cfg, "127.0.0.1", port3, 3000);
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
    void cluster_info_shows_cluster_enabled() {
        try (Jedis j = pool1.getResource()) {
            String info = j.clusterInfo();
            assertTrue(info.contains("cluster_enabled:1"), "Cluster should be enabled: " + info);
            assertTrue(info.contains("cluster_state:ok"), "Cluster should be ok: " + info);
        }
    }

    @Test
    @Order(2)
    void cluster_nodes_shows_all_three() {
        try (Jedis j = pool1.getResource()) {
            String nodes = j.clusterNodes();
            assertNotNull(nodes);
            // Each node should appear
            assertTrue(nodes.contains(String.valueOf(port1)), "Should contain port1");
            assertTrue(nodes.contains(String.valueOf(port2)), "Should contain port2");
            assertTrue(nodes.contains(String.valueOf(port3)), "Should contain port3");
        }
    }

    @Test
    @Order(3)
    void cluster_keyslot_is_correct() {
        try (Jedis j = pool1.getResource()) {
            // "foo" → CRC16("foo") % 16384 = 12182 → node3 (10923-16383)
            long slot = j.clusterKeySlot("foo");
            assertEquals(12182L, slot, "CLUSTER KEYSLOT foo should be 12182");

            // "hello" → 866 → node1 (0-5460)
            long slot2 = j.clusterKeySlot("hello");
            assertEquals(866L, slot2);
        }
    }

    @Test
    @Order(4)
    void set_on_correct_node_succeeds() {
        // "hello" is slot 866 → node1 (0-5460)
        try (Jedis j = pool1.getResource()) {
            assertEquals("OK", j.set("hello", "world"));
            assertEquals("world", j.get("hello"));
        }
    }

    @Test
    @Order(5)
    void set_on_wrong_node_returns_moved() {
        // "foo" is slot 12182 → node3 (10923-16383)
        // Asking node1 for "foo" should return MOVED
        try (Jedis j = pool1.getResource()) {
            try {
                j.set("foo", "bar");
                fail("Expected MOVED error");
            } catch (redis.clients.jedis.exceptions.JedisMovedDataException e) {
                // Expected: MOVED 12182 127.0.0.1:port3
                assertEquals(12182, e.getSlot());
                assertEquals(port3, e.getTargetNode().getPort());
            }
        }
    }

    @Test
    @Order(6)
    void set_on_correct_node3_succeeds() {
        // "foo" is on node3
        try (Jedis j = pool3.getResource()) {
            assertEquals("OK", j.set("foo", "bar"));
            assertEquals("bar", j.get("foo"));
        }
    }

    @Test
    @Order(7)
    void cluster_myid_returns_unique_ids() {
        try (Jedis j1 = pool1.getResource(); Jedis j2 = pool2.getResource()) {
            String id1 = j1.clusterMyId();
            String id2 = j2.clusterMyId();
            assertNotNull(id1);
            assertNotNull(id2);
            assertNotEquals(id1, id2, "Each node should have a unique ID");
            assertEquals(40, id1.length(), "Cluster node ID should be 40 chars");
        }
    }

    // ---- helpers ----

    private static int freePort() throws IOException {
        // Use ports in range 10000-55000 so bus port (port+10000) stays <= 65535
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
