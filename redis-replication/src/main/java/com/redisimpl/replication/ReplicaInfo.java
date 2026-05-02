package com.redisimpl.replication;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks state of a connected replica on the master side.
 */
public final class ReplicaInfo {

    public enum State {
        WAIT_PING,
        WAIT_REPLCONF,
        WAIT_PSYNC,
        ONLINE
    }

    private final SocketChannel channel;
    private volatile State state;
    private volatile long replicaOffset;
    private volatile int listeningPort;
    private final long connectedAt;

    public ReplicaInfo(SocketChannel channel) {
        this.channel = channel;
        this.state = State.WAIT_PING;
        this.replicaOffset = 0;
        this.connectedAt = System.currentTimeMillis();
    }

    public SocketChannel getChannel()      { return channel; }
    public State getState()                { return state; }
    public void setState(State state)      { this.state = state; }
    public long getReplicaOffset()         { return replicaOffset; }
    public void setReplicaOffset(long off) { this.replicaOffset = off; }
    public int getListeningPort()          { return listeningPort; }
    public void setListeningPort(int p)    { this.listeningPort = p; }
    public long getConnectedAt()           { return connectedAt; }
}
