package com.redisimpl.replication;

import com.redisimpl.persistence.RdbLoader;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.resp.RespDecoder;
import com.redisimpl.server.resp.RespEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Slave-side replication client.
 *
 * Connects to master, performs PSYNC2 handshake, receives full RDB or
 * partial backlog, then streams incoming commands and executes them.
 */
public final class ReplicationClient implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ReplicationClient.class);

    private final RedisServer server;
    private final ReplicationManager manager;
    private final String masterHost;
    private final int masterPort;

    private volatile boolean running = true;
    private volatile boolean connected = false;
    private volatile Socket activeSocket = null;
    private final AtomicLong replicaOffset = new AtomicLong(0);

    public ReplicationClient(RedisServer server, ReplicationManager manager,
                             String masterHost, int masterPort) {
        this.server = server;
        this.manager = manager;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    @Override
    public void run() {
        while (running) {
            try {
                connect();
            } catch (Exception e) {
                if (running) {
                    log.warn("Replication connection lost, retrying in 5s: {}", e.getMessage());
                    try { Thread.sleep(5000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }
    }

    private void connect() throws Exception {
        log.info("Connecting to master {}:{}", masterHost, masterPort);
        try (Socket socket = new Socket()) {
            activeSocket = socket;
            socket.connect(new InetSocketAddress(masterHost, masterPort), 5000);
            socket.setSoTimeout(30000);
            socket.setTcpNoDelay(true);

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // ---- Handshake ----
            // 1. PING
            sendCommand(out, "PING");
            String pong = readLine(in);
            log.debug("PING response: {}", pong);

            // 2. REPLCONF listening-port
            sendCommand(out, "REPLCONF", "listening-port", String.valueOf(server.getPort()));
            String r1 = readLine(in);
            log.debug("REPLCONF listening-port: {}", r1);

            // 3. REPLCONF capa psync2
            sendCommand(out, "REPLCONF", "capa", "psync2");
            String r2 = readLine(in);
            log.debug("REPLCONF capa: {}", r2);

            // 4. PSYNC <replid> <offset>  (? -1 for full sync)
            sendCommand(out, "PSYNC", "?", "-1");
            String psyncReply = readLine(in);
            log.info("PSYNC reply: {}", psyncReply);

            if (psyncReply.startsWith("+FULLRESYNC")) {
                // Parse replid and offset from "+FULLRESYNC <replid> <offset>"
                String[] parts = psyncReply.substring(1).split(" ");
                long masterOffsetAtSync = Long.parseLong(parts[2]);

                // Receive RDB: $<len>\r\n<data>
                receivRdb(in);
                replicaOffset.set(masterOffsetAtSync);
                log.info("Full sync done, offset={}", masterOffsetAtSync);
            } else if (psyncReply.startsWith("+CONTINUE")) {
                log.info("Partial resync");
            }

            connected = true;
            updateReplicationInfo(true);

            // ---- Stream commands (pass OutputStream for REPLCONF ACK replies) ----
            streamCommands(in, out);
        } finally {
            connected = false;
            activeSocket = null;
            updateReplicationInfo(false);
        }
    }

    private void receivRdb(InputStream in) throws Exception {
        // Read "$<len>\r\n"
        String lenLine = readLine(in);
        if (!lenLine.startsWith("$")) throw new IOException("Expected RDB bulk, got: " + lenLine);
        int rdbLen = Integer.parseInt(lenLine.substring(1).trim());

        byte[] rdbData = new byte[rdbLen];
        int read = 0;
        while (read < rdbLen) {
            int n = in.read(rdbData, read, rdbLen - read);
            if (n < 0) throw new EOFException("Connection closed while reading RDB");
            read += n;
        }

        // Load RDB on event loop thread to maintain single-threaded model
        final byte[] finalRdbData = rdbData;
        server.getEventLoop().submit(() -> {
            try {
                com.redisimpl.persistence.RedisConfig config =
                        new com.redisimpl.persistence.RedisConfig();
                RdbLoader loader = new RdbLoader(config);
                loader.loadFromBytes(finalRdbData, server.getDbs());
                log.info("Loaded RDB from master: {} bytes", finalRdbData.length);
            } catch (IOException e) {
                log.error("Failed to load RDB from master", e);
            }
        });
    }

    /**
     * Stream incoming replication commands from master and execute them.
     * Mirrors the replication stream processing loop in replication.c.
     *
     * @param in  input stream from master
     * @param out output stream to master (for REPLCONF ACK replies)
     */
    private void streamCommands(InputStream in, OutputStream out) throws IOException {
        RespDecoder decoder = new RespDecoder();
        byte[] buf = new byte[16384];

        while (running) {
            int n = in.read(buf);
            if (n < 0) throw new IOException("Master closed connection");
            if (n == 0) continue;

            ByteBuffer bb = ByteBuffer.wrap(buf, 0, n);
            List<byte[][]> commands = decoder.decode(bb);
            for (byte[][] argv : commands) {
                if (argv.length == 0) continue;
                String cmd = new String(argv[0], StandardCharsets.UTF_8).toLowerCase();

                // REPLCONF GETACK — master requests current replication offset.
                // Mirrors replicationSendAck() in replication.c:
                //   addReplyArrayLen(c, 3); "REPLCONF" "ACK" "<offset>"
                if (cmd.equals("replconf") && argv.length >= 3
                        && new String(argv[1], StandardCharsets.UTF_8).equalsIgnoreCase("getack")) {
                    byte[] ack = encodeCommand(new String[]{
                            "REPLCONF", "ACK", String.valueOf(replicaOffset.get())});
                    try {
                        out.write(ack);
                        out.flush();
                        log.debug("Sent REPLCONF ACK {} to master", replicaOffset.get());
                    } catch (IOException e) {
                        log.warn("Failed to send REPLCONF ACK: {}", e.getMessage());
                    }
                    continue;
                }

                // Execute the replicated command on replica's event loop thread
                executeOnReplica(argv);

                // Track accumulated offset (mirrors c->reploff in replication.c)
                replicaOffset.addAndGet(encodeCommand(argv).length);
            }
        }
    }

    private void executeOnReplica(byte[][] argv) {
        server.getEventLoop().submit(() -> {
            try {
                com.redisimpl.server.client.RedisClient fakeClient =
                        new com.redisimpl.server.client.RedisClient(-1);
                fakeClient.setArgv(argv);
                server.processCommand(fakeClient);
            } catch (Exception e) {
                log.warn("Error executing replicated command {}: {}",
                        new String(argv[0]), e.getMessage());
            }
        });
    }

    // ---- I/O helpers ----

    private void sendCommand(OutputStream out, String... args) throws IOException {
        out.write(encodeCommand(args));
        out.flush();
    }

    private byte[] encodeCommand(String[] args) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (String arg : args) {
                byte[] b = arg.getBytes(StandardCharsets.UTF_8);
                baos.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                baos.write(b);
                baos.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException ignored) {}
        return baos.toByteArray();
    }

    private byte[] encodeCommand(byte[][] args) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (byte[] arg : args) {
                baos.write(("$" + arg.length + "\r\n").getBytes(StandardCharsets.UTF_8));
                baos.write(arg);
                baos.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException ignored) {}
        return baos.toByteArray();
    }

    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') break;
                sb.append((char) b);
                sb.append((char) next);
            } else {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }

    public void stop() {
        running = false;
        Socket s = activeSocket;
        if (s != null && !s.isClosed()) {
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    public boolean isConnected() { return connected; }
    public long getReplicaOffset() { return replicaOffset.get(); }

    private void updateReplicationInfo(boolean linkUp) {
        com.redisimpl.server.replication.ReplicationInfo info = server.getReplicationInfo();
        info.setMasterLinkUp(linkUp);
        info.setReplicaOffset(replicaOffset.get());
        if (linkUp) {
            info.setMasterLastIo(System.currentTimeMillis());
        }
    }
}
