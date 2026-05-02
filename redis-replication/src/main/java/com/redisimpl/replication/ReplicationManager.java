package com.redisimpl.replication;

import com.redisimpl.persistence.RdbSaver;
import com.redisimpl.persistence.RedisConfig;
import com.redisimpl.server.RedisServer;
import com.redisimpl.server.resp.RespEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Master-side replication manager.
 *
 * Tracks connected replicas, generates replid, maintains replication offset,
 * and handles PSYNC2 handshake + command propagation.
 */
public final class ReplicationManager
        implements com.redisimpl.server.replication.ReplicationPropagator {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final int BACKLOG_SIZE = 1024 * 1024; // 1 MB

    private final RedisServer server;
    private final String replId;
    private final ReplicationBacklog backlog;
    private final AtomicLong masterOffset = new AtomicLong(0);

    /** Active replicas keyed by their SocketChannel */
    private final Map<SocketChannel, ReplicaInfo> replicas = new ConcurrentHashMap<>();

    /** For slave mode: connection to master */
    private volatile ReplicationClient slaveClient;
    private volatile boolean isSlave = false;
    private volatile String masterHost;
    private volatile int masterPort;

    public ReplicationManager(RedisServer server) {
        this.server = server;
        this.replId = generateReplId();
        this.backlog = new ReplicationBacklog(BACKLOG_SIZE);
        log.info("ReplicationManager created, replid={}", replId);
    }

    /**
     * Attach this manager to the server: register replication commands and
     * set up command propagation.  Call once after server construction.
     */
    public void attach() {
        server.setReplicationPropagator(this);
        server.getCommandTable().register(new ReplicationCommands(this));
        // Initialise INFO replication with master defaults
        com.redisimpl.server.replication.ReplicationInfo info = server.getReplicationInfo();
        info.setRole(com.redisimpl.server.replication.ReplicationInfo.Role.MASTER);
        info.setReplId(replId);
        info.setMasterOffset(0);
        info.setConnectedSlaves(0);
        // Refresh slave info every second via a time event
        server.getEventLoop().aeCreateTimeEvent(1000, (id, data) -> {
            refreshReplicationInfo();
            return 1000;
        });
        log.info("ReplicationManager attached to server on port {}", server.getPort());
    }

    // ---- Master-side ----

    /**
     * Build the full-sync response: +FULLRESYNC header + $len\r\n + RDB bytes.
     *
     * This implementation uses diskless replication (repl-diskless-sync = yes):
     * the RDB is serialized directly into memory and piped to the replica socket
     * without writing a temporary file to disk.
     * Mirrors the diskless sync path in replication.c (rdbSaveToSlavesSockets).
     */
    public byte[] buildFullSyncResponse(SocketChannel ch) {
        ReplicaInfo replica = replicas.computeIfAbsent(ch, ReplicaInfo::new);
        try {
            byte[] header = RespEncoder.encodeSimpleString(
                    "FULLRESYNC " + replId + " " + masterOffset.get());

            // Diskless sync: serialize RDB to in-memory stream, no temp file
            // Mirrors rdbSaveToSlavesSockets() in replication.c
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            RdbSaver saver = new RdbSaver(new RedisConfig());
            saver.saveToStream(baos, server.getDbs());
            byte[] rdbData = baos.toByteArray();
            log.info("Full sync (diskless): {} bytes RDB in-memory", rdbData.length);

            String lenLine = "$" + rdbData.length + "\r\n";

            ByteArrayOutputStream response = new ByteArrayOutputStream();
            response.write(header);
            response.write(lenLine.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            response.write(rdbData);

            replica.setState(ReplicaInfo.State.ONLINE);
            replica.setReplicaOffset(masterOffset.get());
            log.info("Full sync prepared: {} bytes header+RDB", response.size());
            return response.toByteArray();
        } catch (IOException e) {
            log.error("Full sync failed", e);
            replicas.remove(ch);
            return RespEncoder.encodeError("ERR replication error");
        }
    }

    /** Refresh the ReplicationInfo slave lines from current replica state. */
    public void refreshReplicationInfo() {
        com.redisimpl.server.replication.ReplicationInfo info = server.getReplicationInfo();
        int count = 0;
        StringBuilder sb = new StringBuilder();
        for (ReplicaInfo replica : replicas.values()) {
            if (replica.getState() == ReplicaInfo.State.ONLINE) {
                try {
                    java.net.InetSocketAddress addr =
                            (java.net.InetSocketAddress) replica.getChannel().getRemoteAddress();
                    String host = addr != null ? addr.getAddress().getHostAddress() : "127.0.0.1";
                    int port = replica.getListeningPort() > 0 ? replica.getListeningPort()
                            : (addr != null ? addr.getPort() : 0);
                    sb.append("slave").append(count).append(":ip=").append(host)
                      .append(",port=").append(port)
                      .append(",state=online,offset=").append(replica.getReplicaOffset())
                      .append(",lag=0\r\n");
                    count++;
                } catch (Exception ignored) {}
            }
        }
        info.setSlaveLines(sb.toString());
        info.setConnectedSlaves(count);
        info.setReplId(replId);
        info.setMasterOffset(masterOffset.get());
    }

    /**
     * Propagate a write command to all online replicas.
     * Called after every write command executes on master.
     */
    public void propagate(byte[][] argv) {
        refreshReplicationInfo();
        if (replicas.isEmpty()) return;

        byte[] encoded = encodeCommand(argv);
        backlog.append(encoded);
        masterOffset.addAndGet(encoded.length);

        for (Map.Entry<SocketChannel, ReplicaInfo> entry : replicas.entrySet()) {
            ReplicaInfo replica = entry.getValue();
            if (replica.getState() == ReplicaInfo.State.ONLINE) {
                try {
                    entry.getKey().write(ByteBuffer.wrap(encoded));
                    replica.setReplicaOffset(replica.getReplicaOffset() + encoded.length);
                } catch (IOException e) {
                    log.warn("Failed to propagate to replica, removing", e);
                    replicas.remove(entry.getKey());
                }
            }
        }
    }

    private byte[] encodeCommand(byte[][] argv) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + argv.length + "\r\n").getBytes());
            for (byte[] arg : argv) {
                out.write(("$" + arg.length + "\r\n").getBytes());
                out.write(arg);
                out.write("\r\n".getBytes());
            }
        } catch (IOException ignored) {}
        return out.toByteArray();
    }

    public void addReplica(SocketChannel ch) {
        replicas.putIfAbsent(ch, new ReplicaInfo(ch));
    }

    public void removeReplica(SocketChannel ch) {
        replicas.remove(ch);
    }

    public ReplicaInfo getReplica(SocketChannel ch) {
        return replicas.get(ch);
    }

    // ---- Slave-side ----

    public synchronized void startSlaveOf(String host, int port) {
        stopSlaveOf();
        masterHost = host;
        masterPort = port;
        isSlave = true;
        slaveClient = new ReplicationClient(server, this, host, port);
        Thread t = new Thread(slaveClient, "replication-client");
        t.setDaemon(true);
        t.start();
        log.info("Started SLAVEOF {}:{}", host, port);
    }

    public synchronized void stopSlaveOf() {
        isSlave = false;
        if (slaveClient != null) {
            slaveClient.stop();
            slaveClient = null;
        }
        masterHost = null;
        masterPort = 0;
        log.info("REPLICAOF NO ONE — became master");
    }

    // ---- Accessors ----

    public String getReplId()          { return replId; }
    public long getMasterOffset()      { return masterOffset.get(); }
    public ReplicationBacklog getBacklog() { return backlog; }
    public boolean isSlave()           { return isSlave; }
    public String getMasterHost()      { return masterHost; }
    public int getMasterPort()         { return masterPort; }
    public int getReplicaCount()       { return (int) replicas.values().stream()
            .filter(r -> r.getState() == ReplicaInfo.State.ONLINE).count(); }
    public Collection<ReplicaInfo> getReplicas() { return replicas.values(); }

    public boolean isMasterLinkUp() {
        return slaveClient != null && slaveClient.isConnected();
    }

    public long getReplicaOffset() {
        return slaveClient != null ? slaveClient.getReplicaOffset() : 0;
    }

    public RedisServer getServer() { return server; }

    // ---- Helpers ----

    private static String generateReplId() {
        SecureRandom rng = new SecureRandom();
        StringBuilder sb = new StringBuilder(40);
        String chars = "0123456789abcdefghijklmnopqrstuvwxyz";
        for (int i = 0; i < 40; i++) {
            sb.append(chars.charAt(rng.nextInt(chars.length())));
        }
        return sb.toString();
    }
}
