package com.redisimpl.cluster;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

class OneNodeTest {
    @Test
    void single_node_starts_and_responds() throws Exception {
        int port = freePort();
        ClusterNode node = new ClusterNode(port);
        node.addSlots(0, 16383);

        Thread t = new Thread(() -> {
            try { node.start(); }
            catch (Exception e) { e.printStackTrace(); }
        });
        t.setDaemon(true);
        t.start();

        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) { break; }
            catch (IOException e) { Thread.sleep(50); }
        }
        assertTrue(System.currentTimeMillis() < deadline, "server not ready");

        try (JedisPool pool = new JedisPool("127.0.0.1", port);
             Jedis j = pool.getResource()) {
            assertEquals("PONG", j.ping());
        }
        node.stop();
    }

    private int freePort() throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            int p = 10000 + new java.util.Random().nextInt(45000);
            try (ServerSocket s = new ServerSocket(p)) { s.setReuseAddress(true); return p; }
            catch (IOException ignored) {}
        }
        throw new IOException("No free port in 10000-55000");
    }
}
