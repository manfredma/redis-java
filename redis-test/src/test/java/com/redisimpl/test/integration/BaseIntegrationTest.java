package com.redisimpl.test.integration;

import com.redisimpl.server.RedisServer;
import org.junit.jupiter.api.AfterEach;
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
 *
 * Uses a single shared server for ALL integration test classes (started once per JVM).
 */
public abstract class BaseIntegrationTest {

    // ---- Shared server state (one instance for the whole test run) ----
    private static volatile RedisServer sharedServer;
    private static volatile int sharedPort;
    private static volatile Thread sharedServerThread;
    private static volatile JedisPool sharedPool;
    private static volatile boolean serverStarted = false;

    protected Jedis jedis;

    @BeforeAll
    static synchronized void ensureServerStarted() throws Exception {
        if (serverStarted) return;
        sharedPort = findFreePort();
        sharedServer = new RedisServer(sharedPort);
        sharedServerThread = new Thread(() -> {
            try {
                sharedServer.start();
            } catch (Exception e) {
                // Server stopped normally
            }
        });
        sharedServerThread.setDaemon(true);
        sharedServerThread.start();

        // Wait until the port is actually accepting connections (max 5 seconds)
        waitForPort(sharedPort, 5000);

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(10);
        sharedPool = new JedisPool(config, "127.0.0.1", sharedPort, 2000);

        // Register shutdown hook to stop the server when JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (sharedPool != null) sharedPool.close();
            if (sharedServer != null) sharedServer.stop();
        }));

        serverStarted = true;
    }

    @BeforeEach
    void connectJedis() throws Exception {
        ensureServerStarted();
        jedis = sharedPool.getResource();
        jedis.flushAll();
    }

    @AfterEach
    void closeJedis() {
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
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
