package com.redisimpl.replication;

import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.RedisCommand;
import com.redisimpl.server.resp.RespEncoder;
import com.redisimpl.server.replication.ReplicationInfo;

import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/**
 * REPLICAOF, SLAVEOF, REPLCONF, PSYNC commands.
 * Registered into CommandTable by ReplicationManager.
 */
public final class ReplicationCommands {

    private final ReplicationManager manager;

    public ReplicationCommands(ReplicationManager manager) {
        this.manager = manager;
    }

    // REPLICAOF NO ONE  |  REPLICAOF host port
    @RedisCommand(name = "replicaof", arity = 3, flags = "admin noscript ok-loading ok-stale",
            firstKey = 0, lastKey = 0, step = 0)
    public byte[] replicaof(RedisClient client, byte[][] argv) {
        String arg1 = new String(argv[1], StandardCharsets.UTF_8);
        if (arg1.equalsIgnoreCase("NO") || arg1.equalsIgnoreCase("no")) {
            manager.stopSlaveOf();
            updateReplInfo();
            return RespEncoder.encodeSimpleString("OK");
        }
        String host = arg1;
        int port;
        try {
            port = Integer.parseInt(new String(argv[2], StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return RespEncoder.encodeError("ERR Invalid port");
        }
        manager.startSlaveOf(host, port);
        updateReplInfo();
        return RespEncoder.encodeSimpleString("OK");
    }

    // SLAVEOF — alias for REPLICAOF
    @RedisCommand(name = "slaveof", arity = 3, flags = "admin noscript ok-loading ok-stale",
            firstKey = 0, lastKey = 0, step = 0)
    public byte[] slaveof(RedisClient client, byte[][] argv) {
        return replicaof(client, argv);
    }

    // REPLCONF <option> <value> [<option> <value> ...]
    @RedisCommand(name = "replconf", arity = -3, flags = "admin noscript ok-loading ok-stale",
            firstKey = 0, lastKey = 0, step = 0)
    public byte[] replconf(RedisClient client, byte[][] argv) {
        String subCmd = new String(argv[1], StandardCharsets.UTF_8).toLowerCase();
        switch (subCmd) {
            case "listening-port": {
                int listenPort = Integer.parseInt(new String(argv[2], StandardCharsets.UTF_8));
                SocketChannel ch = client.getChannel();
                if (ch != null) {
                    ReplicaInfo replica = manager.getReplica(ch);
                    if (replica == null) {
                        manager.addReplica(ch);
                        replica = manager.getReplica(ch);
                    }
                    if (replica != null) replica.setListeningPort(listenPort);
                }
                return RespEncoder.encodeSimpleString("OK");
            }
            case "capa":
                // Acknowledge capability negotiation
                return RespEncoder.encodeSimpleString("OK");
            case "ack": {
                // Replica is reporting its offset
                if (argv.length >= 3) {
                    long offset = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
                    SocketChannel ch = client.getChannel();
                    if (ch != null) {
                        ReplicaInfo replica = manager.getReplica(ch);
                        if (replica != null) replica.setReplicaOffset(offset);
                    }
                }
                return null; // no reply for ACK
            }
            case "getack":
                // REPLCONF GETACK received by master — not expected here since
                // master sends GETACK *to* replicas, not the other way.
                // But handle gracefully.
                return RespEncoder.encodeSimpleString("OK");
            default:
                return RespEncoder.encodeSimpleString("OK");
        }
    }

    // PSYNC <replid> <offset>
    @RedisCommand(name = "psync", arity = 3, flags = "admin noscript ok-loading ok-stale",
            firstKey = 0, lastKey = 0, step = 0)
    public byte[] psync(RedisClient client, byte[][] argv) {
        String clientReplId = new String(argv[1], StandardCharsets.UTF_8);
        long clientOffset;
        try {
            clientOffset = Long.parseLong(new String(argv[2], StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return RespEncoder.encodeError("ERR Invalid offset");
        }

        SocketChannel ch = client.getChannel();
        if (ch == null) return RespEncoder.encodeError("ERR no channel");

        // Register replica
        manager.addReplica(ch);

        boolean canPartial = !clientReplId.equals("?")
                && clientReplId.equals(manager.getReplId())
                && manager.getBacklog().canPartialResync(clientOffset);

        if (canPartial) {
            manager.getReplica(ch).setState(ReplicaInfo.State.ONLINE);
            manager.getReplica(ch).setReplicaOffset(clientOffset);
            updateReplInfo();
            return RespEncoder.encodeSimpleString("CONTINUE " + manager.getReplId());
        }

        // Full resync: build response in-memory and put into client reply buffer
        byte[] fullSyncData = manager.buildFullSyncResponse(ch);
        updateReplInfo();
        return fullSyncData; // single byte[] containing header + RDB
    }

    // WAIT numreplicas timeout
    @RedisCommand(name = "wait", arity = 3, flags = "noscript",
            firstKey = 0, lastKey = 0, step = 0)
    public byte[] wait(RedisClient client, byte[][] argv) {
        // Return current number of acknowledged replicas
        return RespEncoder.encodeInteger(manager.getReplicaCount());
    }

    // ---- helpers ----

    private void updateReplInfo() {
        ReplicationInfo info = manager.getServer().getReplicationInfo();
        if (manager.isSlave()) {
            info.setRole(ReplicationInfo.Role.SLAVE);
            info.setMasterHost(manager.getMasterHost());
            info.setMasterPort(manager.getMasterPort());
            info.setMasterLinkUp(manager.isMasterLinkUp());
            info.setReplicaOffset(manager.getReplicaOffset());
        } else {
            info.setRole(ReplicationInfo.Role.MASTER);
            info.setReplId(manager.getReplId());
            info.setMasterOffset(manager.getMasterOffset());
            info.setConnectedSlaves(manager.getReplicaCount());
        }
    }
}
