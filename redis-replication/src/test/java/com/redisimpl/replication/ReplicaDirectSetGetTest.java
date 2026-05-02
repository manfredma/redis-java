package com.redisimpl.replication;

import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct SET/GET on a replication-enabled server (no actual replication).
 */
class ReplicaDirectSetGetTest {

    @Test
    void setget_on_server_with_replication_attached() throws Exception {
        int port = freePort();
        RedisServer server = new RedisServer(port);
        new ReplicationManager(server).attach();

        Thread t = new Thread(() -> { try { server.start(); } catch (Exception ignored) {} });
        t.setDaemon(true);
        t.start();

        waitForPort(port, 5000);

        JedisPoolConfig cfg = new JedisPoolConfig();
        try (JedisPool pool = new JedisPool(cfg, "127.0.0.1", port, 3000);
             Jedis jedis = pool.getResource()) {
            assertEquals("PONG", jedis.ping());
            jedis.set("testkey", "testvalue");
            String v = jedis.get("testkey");
            assertEquals("testvalue", v, "GET should return testvalue, got: " + v);
        }

        server.stop();
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
