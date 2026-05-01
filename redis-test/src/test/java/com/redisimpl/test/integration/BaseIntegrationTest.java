package com.redisimpl.test.integration;

import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Base class for integration tests.
 * Starts a RedisServer on a random free port and provides a Jedis client.
 */
public abstract class BaseIntegrationTest {

    protected static RedisServer server;
    protected static int port;
    protected static Thread serverThread;
    protected static JedisPool jedisPool;
    protected Jedis jedis;

    @BeforeAll
    static void startServer() throws Exception {
        port = findFreePort();
        server = new RedisServer(port);
        serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                // Server stopped normally or was closed
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait until the port is actually accepting connections (max 5 seconds)
        waitForPort(port, 5000);

        // Create a connection pool
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        jedisPool = new JedisPool(config, "127.0.0.1", port, 2000);
    }

    @AfterAll
    static void stopServer() {
        if (jedisPool != null) {
            jedisPool.close();
            jedisPool = null;
        }
        if (server != null) {
            server.stop();
            server = null;
        }
        if (serverThread != null) {
            serverThread.interrupt();
            serverThread = null;
        }
    }

    @BeforeEach
    void connectJedis() {
        jedis = jedisPool.getResource();
        jedis.flushAll();
    }

    private static void waitForPort(int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket("127.0.0.1", port)) {
                return; // Connected successfully
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        throw new RuntimeException("Server did not start within " + timeoutMs + "ms on port " + port);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }
}
