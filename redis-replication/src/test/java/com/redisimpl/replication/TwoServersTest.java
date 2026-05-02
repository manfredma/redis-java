package com.redisimpl.replication;

import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

class TwoServersTest {

    @Test
    void two_servers_both_respond() throws Exception {
        int port1 = freePort();
        int port2 = freePort();

        RedisServer s1 = new RedisServer(port1);
        RedisServer s2 = new RedisServer(port2);
        new ReplicationManager(s1).attach();
        new ReplicationManager(s2).attach();

        Thread t1 = new Thread(() -> { try { s1.start(); } catch (Exception e) { e.printStackTrace(); } });
        Thread t2 = new Thread(() -> { try { s2.start(); } catch (Exception e) { e.printStackTrace(); } });
        t1.setDaemon(true);
        t2.setDaemon(true);
        t1.start();
        t2.start();

        waitForPort(port1, 5000);
        waitForPort(port2, 5000);
        Thread.sleep(200);

        JedisPoolConfig cfg = new JedisPoolConfig();
        // Test server1 first
        try (JedisPool p1 = new JedisPool(cfg, "127.0.0.1", port1, 2000);
             Jedis j1 = p1.getResource()) {
            assertEquals("PONG", j1.ping(), "server1 (port " + port1 + ") should respond to PING");
        }

        // Test server2 separately
        try (JedisPool p2 = new JedisPool(cfg, "127.0.0.1", port2, 2000);
             Jedis j2 = p2.getResource()) {
            assertEquals("PONG", j2.ping(), "server2 (port " + port2 + ") should respond to PING");
        }

        s1.stop();
        s2.stop();
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
