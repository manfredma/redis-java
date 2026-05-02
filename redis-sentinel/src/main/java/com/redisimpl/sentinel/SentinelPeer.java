package com.redisimpl.sentinel;

/** Another sentinel instance known to this sentinel. */
public final class SentinelPeer {

    private final String host;
    private final int port;
    private volatile long lastContact = System.currentTimeMillis();

    public SentinelPeer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost()           { return host; }
    public int getPort()              { return port; }
    public long getLastContact()      { return lastContact; }
    public void setLastContact(long t){ this.lastContact = t; }
    public String key()               { return host + ":" + port; }
}
