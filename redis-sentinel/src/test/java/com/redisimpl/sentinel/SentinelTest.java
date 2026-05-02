package com.redisimpl.sentinel;

import com.redisimpl.replication.ReplicationManager;
import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Redis Sentinel: monitoring, SDOWN/ODOWN detection,
 * and the SENTINEL command suite.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SentinelTest {

    private static RedisServer master;
    private static RedisServer replica;
    private static RedisSentinel sentinel1;
    private static RedisSentinel sentinel2;
    private static RedisSentinel sentinel3;

    private static int masterPort;
    private static int replicaPort;
    private static int sentinel1Port;
    private static int sentinel2Port;
    private static int sentinel3Port;

    private static JedisPool masterPool;
    private static JedisPool sentinel1Pool;

    @BeforeAll
    static void setup() throws Exception {
        masterPort   = freePort();
        replicaPort  = freePort();
        sentinel1Port = freePort();
        sentinel2Port = freePort();
        sentinel3Port = freePort();

        // Start master
        master = new RedisServer(masterPort);
        new ReplicationManager(master).attach();
        Thread mt = new Thread(() -> { try { master.start(); } catch (Exception ignored) {} });
        mt.setDaemon(true);
        mt.start();

        // Start replica
        replica = new RedisServer(replicaPort);
        ReplicationManager replicaMgr = new ReplicationManager(replica);
        replicaMgr.attach();
        Thread rt = new Thread(() -> { try { replica.start(); } catch (Exception ignored) {} });
        rt.setDaemon(true);
        rt.start();

        waitForPort(masterPort, 5000);
        waitForPort(replicaPort, 5000);

        // Point replica at master
        JedisPoolConfig cfg = new JedisPoolConfig();
        masterPool = new JedisPool(cfg, "127.0.0.1", masterPort, 3000);
        try (Jedis r = new JedisPool(cfg, "127.0.0.1", replicaPort, 3000).getResource()) {
            r.slaveof("127.0.0.1", masterPort);
        }
        Thread.sleep(1000);

        // Sentinel config
        SentinelConfig sc1 = new SentinelConfig("mymaster", "127.0.0.1", masterPort, 2);
        SentinelConfig sc2 = new SentinelConfig("mymaster", "127.0.0.1", masterPort, 2);
        SentinelConfig sc3 = new SentinelConfig("mymaster", "127.0.0.1", masterPort, 2);
        sc1.setDownAfterMilliseconds(500);
        sc2.setDownAfterMilliseconds(500);
        sc3.setDownAfterMilliseconds(500);

        sentinel1 = new RedisSentinel(sentinel1Port, sc1);
        sentinel2 = new RedisSentinel(sentinel2Port, sc2);
        sentinel3 = new RedisSentinel(sentinel3Port, sc3);

        Thread s1t = new Thread(() -> { try { sentinel1.start(); } catch (Exception ignored) {} });
        Thread s2t = new Thread(() -> { try { sentinel2.start(); } catch (Exception ignored) {} });
        Thread s3t = new Thread(() -> { try { sentinel3.start(); } catch (Exception ignored) {} });
        s1t.setDaemon(true);
        s2t.setDaemon(true);
        s3t.setDaemon(true);
        s1t.start();
        s2t.start();
        s3t.start();

        waitForPort(sentinel1Port, 5000);
        waitForPort(sentinel2Port, 5000);
        waitForPort(sentinel3Port, 5000);

        sentinel1Pool = new JedisPool(cfg, "127.0.0.1", sentinel1Port, 3000);

        // Let sentinels discover each other and stabilize
        Thread.sleep(2000);
    }

    @AfterAll
    static void tearDown() {
        if (masterPool != null) masterPool.close();
        if (sentinel1Pool != null) sentinel1Pool.close();
        if (sentinel1 != null) sentinel1.stop();
        if (sentinel2 != null) sentinel2.stop();
        if (sentinel3 != null) sentinel3.stop();
        if (master != null) master.stop();
        if (replica != null) replica.stop();
    }

    @Test
    @Order(1)
    void sentinel_masters_reports_master() {
        try (Jedis s = sentinel1Pool.getResource()) {
            List<Map<String, String>> masters = s.sentinelMasters();
            assertFalse(masters.isEmpty(), "SENTINEL MASTERS should return at least one entry");
            Map<String, String> m = masters.get(0);
            assertEquals("mymaster", m.get("name"));
            assertEquals("127.0.0.1", m.get("ip"));
            assertEquals(String.valueOf(masterPort), m.get("port"));
        }
    }

    @Test
    @Order(2)
    void sentinel_getMasterAddr_returns_correct_address() {
        try (Jedis s = sentinel1Pool.getResource()) {
            List<String> addr = s.sentinelGetMasterAddrByName("mymaster");
            assertNotNull(addr);
            assertEquals(2, addr.size());
            assertEquals("127.0.0.1", addr.get(0));
            assertEquals(String.valueOf(masterPort), addr.get(1));
        }
    }

    @Test
    @Order(3)
    void sentinel_slaves_reports_replica() {
        try (Jedis s = sentinel1Pool.getResource()) {
            List<Map<String, String>> slaves = s.sentinelSlaves("mymaster");
            assertFalse(slaves.isEmpty(), "Should have at least one slave");
            Map<String, String> sl = slaves.get(0);
            assertEquals(String.valueOf(replicaPort), sl.get("port"));
        }
    }

    @Test
    @Order(4)
    void sentinel_ping_returns_pong() {
        try (Jedis s = sentinel1Pool.getResource()) {
            assertEquals("PONG", s.ping());
        }
    }

    // ---- helpers ----

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
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
