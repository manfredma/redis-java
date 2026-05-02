package com.redisimpl.sentinel;

/**
 * Configuration for a single monitored master.
 */
public final class SentinelConfig {

    private final String masterName;
    private final String masterHost;
    private final int masterPort;
    private final int quorum;

    private long downAfterMilliseconds = 30_000;
    private long failoverTimeout = 180_000;
    private int parallelSyncs = 1;

    public SentinelConfig(String masterName, String masterHost, int masterPort, int quorum) {
        this.masterName = masterName;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.quorum = quorum;
    }

    public String getMasterName()               { return masterName; }
    public String getMasterHost()               { return masterHost; }
    public int getMasterPort()                  { return masterPort; }
    public int getQuorum()                      { return quorum; }
    public long getDownAfterMilliseconds()      { return downAfterMilliseconds; }
    public void setDownAfterMilliseconds(long d){ this.downAfterMilliseconds = d; }
    public long getFailoverTimeout()            { return failoverTimeout; }
    public int getParallelSyncs()               { return parallelSyncs; }
}
