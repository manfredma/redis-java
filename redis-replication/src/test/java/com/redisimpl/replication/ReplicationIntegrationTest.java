package com.redisimpl.replication;

import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for master-replica replication using PSYNC2.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicationIntegrationTest {

    private static RedisServer master;
    private static RedisServer replica;
    private static int masterPort;
    private static int replicaPort;
    private static JedisPool masterPool;
    private static JedisPool replicaPool;

    @BeforeAll
    static void startServers() throws Exception {
        masterPort  = freePort();
        replicaPort = freePort();

        master  = new RedisServer(masterPort);
        replica = new RedisServer(replicaPort);

        new ReplicationManager(master).attach();
        new ReplicationManager(replica).attach();

        Thread mt = new Thread(() -> { try { master.start();  } catch (Exception ignored) {} });
        Thread rt = new Thread(() -> { try { replica.start(); } catch (Exception ignored) {} });
        mt.setDaemon(true);
        rt.setDaemon(true);
        mt.start();
        rt.start();

        waitForPort(masterPort,  5000);
        waitForPort(replicaPort, 5000);

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(10);
        masterPool  = new JedisPool(cfg, "127.0.0.1", masterPort,  3000);
        replicaPool = new JedisPool(cfg, "127.0.0.1", replicaPort, 3000);
    }

    @AfterAll
    static void stopServers() {
        if (masterPool  != null) masterPool.close();
        if (replicaPool != null) replicaPool.close();
        if (master  != null) master.stop();
        if (replica != null) replica.stop();
    }

    @Test
    @Order(1)
    void replicaOf_connects_and_full_sync() throws Exception {
        // Write data to master before replication
        try (Jedis m = masterPool.getResource()) {
            m.flushAll();
            m.set("key1", "value1");
            m.set("key2", "value2");
            m.lpush("list1", "a", "b", "c");
        }

        // Point replica at master
        try (Jedis r = replicaPool.getResource()) {
            assertEquals("OK", r.slaveof("127.0.0.1", masterPort));
        }

        // Wait for full sync
        Thread.sleep(2500);

        // Verify replication
        try (Jedis r = replicaPool.getResource()) {
            assertEquals("value1", r.get("key1"));
            assertEquals("value2", r.get("key2"));
            assertEquals(3L, r.llen("list1"));
        }
    }

    @Test
    @Order(2)
    void write_on_master_propagates_to_replica() throws Exception {
        try (Jedis m = masterPool.getResource()) {
            m.set("propagated", "yes");
            m.incr("counter");
            m.hset("hash1", "field1", "val1");
        }

        Thread.sleep(500);

        try (Jedis r = replicaPool.getResource()) {
            assertEquals("yes", r.get("propagated"));
            assertEquals("1", r.get("counter"));
            assertEquals("val1", r.hget("hash1", "field1"));
        }
    }

    @Test
    @Order(3)
    void info_replication_master_shows_replica() throws Exception {
        try (Jedis m = masterPool.getResource()) {
            String info = m.info("replication");
            assertTrue(info.contains("role:master"), "Master should report role:master");
            assertTrue(info.contains("connected_slaves:1"), "Master should see 1 slave: " + info);
        }
    }

    @Test
    @Order(4)
    void info_replication_replica_shows_master() throws Exception {
        try (Jedis r = replicaPool.getResource()) {
            String info = r.info("replication");
            assertTrue(info.contains("role:slave"), "Replica should report role:slave, got: " + info);
            assertTrue(info.contains("master_host:127.0.0.1"), "Replica should show master_host: " + info);
            assertTrue(info.contains("master_link_status:up"), "Replica link should be up: " + info);
        }
    }

    @Test
    @Order(5)
    void replicaof_no_one_stops_replication() throws Exception {
        try (Jedis r = replicaPool.getResource()) {
            assertEquals("OK", r.replicaofNoOne());
        }

        Thread.sleep(300);

        try (Jedis m = masterPool.getResource()) {
            m.set("after_detach", "should_not_replicate");
        }

        Thread.sleep(300);

        try (Jedis r = replicaPool.getResource()) {
            String info = r.info("replication");
            assertTrue(info.contains("role:master"), "After REPLICAOF NO ONE should be master: " + info);
            assertNull(r.get("after_detach"), "Key after detach should not exist on former replica");
        }
    }

    // ---- helpers ----

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private static void waitForPort(int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) {
                return;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        throw new RuntimeException("Server did not start on port " + port);
    }
}
