package com.redisimpl.sentinel;

import java.util.LinkedHashMap;
import java.util.Map;

/** State for a replica known to this sentinel. */
public final class SlaveInfo {

    private final String host;
    private final int port;
    private volatile String flags = "slave";
    private volatile long masterLinkDownTime = 0;
    private volatile long replicaOffset = 0;
    private volatile long lastOkPing = System.currentTimeMillis();

    public SlaveInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost()                     { return host; }
    public int getPort()                        { return port; }
    public String getFlags()                    { return flags; }
    public long getReplicaOffset()              { return replicaOffset; }
    public void setReplicaOffset(long o)        { this.replicaOffset = o; }
    public long getLastOkPing()                 { return lastOkPing; }
    public void setLastOkPing(long t)           { this.lastOkPing = t; }

    public String key() { return host + ":" + port; }

    public Map<String, String> toInfoMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("name", key());
        m.put("ip", host);
        m.put("port", String.valueOf(port));
        m.put("runid", "");
        m.put("flags", flags);
        m.put("link-pending-commands", "0");
        m.put("link-refcount", "1");
        m.put("last-ping-sent", "0");
        m.put("last-ok-ping-reply", String.valueOf(System.currentTimeMillis() - lastOkPing));
        m.put("last-ping-reply", "0");
        m.put("down-after-milliseconds", "30000");
        m.put("info-refresh", "0");
        m.put("role-reported", "slave");
        m.put("role-reported-time", "0");
        m.put("master-link-down-time", String.valueOf(masterLinkDownTime));
        m.put("master-link-status", masterLinkDownTime == 0 ? "ok" : "err");
        m.put("master-host", "127.0.0.1");
        m.put("master-port", "0");
        m.put("slave-priority", "100");
        m.put("slave-repl-offset", String.valueOf(replicaOffset));
        m.put("replica-announced", "1");
        return m;
    }
}
