package com.redisimpl.server.replication;

/**
 * Holds the current replication state as seen by RedisServer.
 * Set by ReplicationManager via RedisServer.setReplicationInfo().
 */
public final class ReplicationInfo {

    public enum Role { MASTER, SLAVE }

    private volatile Role role = Role.MASTER;
    private volatile String replId = "0000000000000000000000000000000000000000";
    private volatile long masterOffset = 0;
    private volatile int connectedSlaves = 0;
    private volatile String masterHost = null;
    private volatile int masterPort = 0;
    private volatile boolean masterLinkUp = false;
    private volatile long masterLastIo = 0;
    private volatile long replicaOffset = 0;

    public Role getRole()                     { return role; }
    public void setRole(Role role)            { this.role = role; }
    public String getReplId()                 { return replId; }
    public void setReplId(String replId)      { this.replId = replId; }
    public long getMasterOffset()             { return masterOffset; }
    public void setMasterOffset(long offset)  { this.masterOffset = offset; }
    public int getConnectedSlaves()           { return connectedSlaves; }
    public void setConnectedSlaves(int n)     { this.connectedSlaves = n; }
    public String getMasterHost()             { return masterHost; }
    public void setMasterHost(String h)       { this.masterHost = h; }
    public int getMasterPort()                { return masterPort; }
    public void setMasterPort(int p)          { this.masterPort = p; }
    public boolean isMasterLinkUp()           { return masterLinkUp; }
    public void setMasterLinkUp(boolean up)   { this.masterLinkUp = up; }
    public long getMasterLastIo()             { return masterLastIo; }
    public void setMasterLastIo(long t)       { this.masterLastIo = t; }
    public long getReplicaOffset()            { return replicaOffset; }
    public void setReplicaOffset(long off)    { this.replicaOffset = off; }
}
