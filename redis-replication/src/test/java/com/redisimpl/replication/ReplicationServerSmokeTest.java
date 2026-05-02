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
 * Smoke test: verify that a server with ReplicationManager.attach() still responds normally.
 */
class ReplicationServerSmokeTest {

    @Test
    void server_with_replication_attached_responds_to_ping() throws Exception {
        int port = freePort();
        RedisServer server = new RedisServer(port);
        new ReplicationManager(server).attach();

        Thread t = new Thread(() -> { try { server.start(); } catch (Exception ignored) {} });
        t.setDaemon(true);
        t.start();

        // Wait for startup
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) { break; }
            catch (IOException e) { Thread.sleep(50); }
        }

        JedisPoolConfig cfg = new JedisPoolConfig();
        try (JedisPool pool = new JedisPool(cfg, "127.0.0.1", port, 2000);
             Jedis jedis = pool.getResource()) {
            assertEquals("PONG", jedis.ping());
            jedis.set("smoke", "test");
            assertEquals("test", jedis.get("smoke"));
        }

        server.stop();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }
}
