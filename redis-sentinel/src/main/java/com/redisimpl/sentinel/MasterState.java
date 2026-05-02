package com.redisimpl.sentinel;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime state for a monitored master and its replicas.
 */
public final class MasterState {

    public enum Status { OK, SDOWN, ODOWN }

    private final SentinelConfig config;
    private volatile String host;
    private volatile int port;
    private volatile Status status = Status.OK;
    private volatile long lastOkPing = System.currentTimeMillis();
    private volatile long lastInfoTime = 0;
    private volatile String role = "master";
    private volatile long masterLinkDownTime = 0;
    private volatile long masterOffset = 0;

    /** Known replicas: "host:port" → SlaveInfo */
    private final Map<String, SlaveInfo> slaves = new ConcurrentHashMap<>();

    /** Other sentinels monitoring this master */
    private final Map<String, SentinelPeer> sentinelPeers = new ConcurrentHashMap<>();

    public MasterState(SentinelConfig config) {
        this.config = config;
        this.host = config.getMasterHost();
        this.port = config.getMasterPort();
    }

    public SentinelConfig getConfig()          { return config; }
    public String getHost()                    { return host; }
    public void setHost(String h)              { this.host = h; }
    public int getPort()                       { return port; }
    public void setPort(int p)                 { this.port = p; }
    public Status getStatus()                  { return status; }
    public void setStatus(Status s)            { this.status = s; }
    public long getLastOkPing()                { return lastOkPing; }
    public void setLastOkPing(long t)          { this.lastOkPing = t; }
    public long getLastInfoTime()              { return lastInfoTime; }
    public void setLastInfoTime(long t)        { this.lastInfoTime = t; }
    public long getMasterOffset()              { return masterOffset; }
    public void setMasterOffset(long o)        { this.masterOffset = o; }
    public Map<String, SlaveInfo> getSlaves()  { return slaves; }
    public Map<String, SentinelPeer> getPeers(){ return sentinelPeers; }

    public String getName() { return config.getMasterName(); }

    /** Info map for SENTINEL MASTERS output */
    public java.util.Map<String, String> toInfoMap() {
        java.util.LinkedHashMap<String, String> m = new java.util.LinkedHashMap<>();
        m.put("name", getName());
        m.put("ip", host);
        m.put("port", String.valueOf(port));
        m.put("runid", "");
        m.put("flags", status == Status.OK ? "master" :
                status == Status.SDOWN ? "master,s_down" : "master,o_down");
        m.put("link-pending-commands", "0");
        m.put("link-refcount", "1");
        m.put("last-ping-sent", "0");
        m.put("last-ok-ping-reply",
                String.valueOf(System.currentTimeMillis() - lastOkPing));
        m.put("last-ping-reply", "0");
        m.put("down-after-milliseconds",
                String.valueOf(config.getDownAfterMilliseconds()));
        m.put("info-refresh", "0");
        m.put("role-reported", role);
        m.put("role-reported-time", "0");
        m.put("config-epoch", "0");
        m.put("num-slaves", String.valueOf(slaves.size()));
        m.put("num-other-sentinels", String.valueOf(sentinelPeers.size()));
        m.put("quorum", String.valueOf(config.getQuorum()));
        m.put("failover-timeout", String.valueOf(config.getFailoverTimeout()));
        m.put("parallel-syncs", String.valueOf(config.getParallelSyncs()));
        return m;
    }
}
