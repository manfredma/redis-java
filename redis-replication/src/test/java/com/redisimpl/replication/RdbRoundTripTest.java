package com.redisimpl.replication;

import com.redisimpl.persistence.RdbLoader;
import com.redisimpl.persistence.RdbSaver;
import com.redisimpl.persistence.RedisConfig;
import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that RDB save→load round trip preserves string values correctly.
 */
class RdbRoundTripTest {

    @Test
    void rdb_save_load_string_values() throws Exception {
        int port1 = freePort();
        int port2 = freePort();

        // Server 1: set some keys
        RedisServer src = new RedisServer(port1);
        Thread t1 = new Thread(() -> { try { src.start(); } catch (Exception ignored) {} });
        t1.setDaemon(true);
        t1.start();
        waitForPort(port1, 5000);

        JedisPoolConfig cfg = new JedisPoolConfig();
        try (JedisPool p1 = new JedisPool(cfg, "127.0.0.1", port1, 3000);
             Jedis j1 = p1.getResource()) {
            j1.set("k1", "v1");
            j1.set("k2", "v2");
            j1.lpush("list1", "a", "b");
            assertEquals("v1", j1.get("k1"));
        }

        // Save to bytes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RdbSaver saver = new RdbSaver(new RedisConfig());
        saver.saveToStream(baos, src.getDbs());
        byte[] rdbData = baos.toByteArray();
        assertTrue(rdbData.length > 10, "RDB should not be empty");

        // Server 2: load RDB
        RedisServer dst = new RedisServer(port2);
        Thread t2 = new Thread(() -> { try { dst.start(); } catch (Exception ignored) {} });
        t2.setDaemon(true);
        t2.start();
        waitForPort(port2, 5000);

        RdbLoader loader = new RdbLoader(new RedisConfig());
        loader.loadFromBytes(rdbData, dst.getDbs());
        Thread.sleep(100);

        try (JedisPool p2 = new JedisPool(cfg, "127.0.0.1", port2, 3000);
             Jedis j2 = p2.getResource()) {
            String v1 = j2.get("k1");
            assertEquals("v1", v1, "After RDB load, k1 should be v1, got: " + v1);
            assertEquals("v2", j2.get("k2"));
            assertEquals(2L, j2.llen("list1"));
        }

        src.stop();
        dst.stop();
    }

    private int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private void waitForPort(int port, long ms) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) { return; }
            catch (IOException e) { Thread.sleep(50); }
        }
        throw new RuntimeException("Port " + port + " not ready");
    }
}
