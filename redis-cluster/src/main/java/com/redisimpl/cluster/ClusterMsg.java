package com.redisimpl.cluster;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Redis Cluster bus message — binary layout identical to clusterMsg in cluster_legacy.h.
 *
 * Wire format (all multi-byte fields are big-endian / network byte order except where noted):
 *
 *   Offset  Size  Field
 *   0       4     sig "RCmb"
 *   4       4     totlen (uint32, network byte order)
 *   8       2     ver  (uint16, htons(1))
 *   10      2     port (uint16, primary client port)
 *   12      2     type (uint16, message type)
 *   14      2     count (uint16, number of gossip entries)
 *   16      8     currentEpoch (uint64)
 *   24      8     configEpoch  (uint64)
 *   32      8     offset       (uint64, replication offset)
 *   40      40    sender  (node name, 40 hex chars)
 *   80      2048  myslots (16384 bits, 2048 bytes)
 *   2128    40    slaveof (master name or zero-filled)
 *   2168    46    myip    (IP string, zero-terminated, 46 bytes NET_IP_STR_LEN)
 *   2214    2     extensions (uint16)
 *   2216    30    notused1
 *   2246    2     pport (uint16, secondary port)
 *   2248    2     cport (uint16, cluster bus port)
 *   2250    2     flags (uint16, sender node flags)
 *   2252    1     state (cluster state byte)
 *   2253    3     mflags
 *   2256    ...   data (union: gossip entries for PING/PONG/MEET, etc.)
 *
 * Each gossip entry (clusterMsgDataGossip, 106 bytes):
 *   0   40   nodename (40 bytes)
 *   40  4    ping_sent (uint32, seconds)
 *   44  4    pong_received (uint32, seconds)
 *   48  46   ip (NET_IP_STR_LEN)
 *   94  2    port (uint16)
 *   96  2    cport (uint16)
 *   98  2    flags (uint16)
 *   100 2    pport (uint16)
 *   102 4    notused1
 *             total: 106 bytes
 */
public final class ClusterMsg {

    // ---- Message types ----
    public static final int CLUSTERMSG_TYPE_PING = 0;
    public static final int CLUSTERMSG_TYPE_PONG = 1;
    public static final int CLUSTERMSG_TYPE_MEET = 2;
    public static final int CLUSTERMSG_TYPE_FAIL = 3;
    public static final int CLUSTERMSG_TYPE_UPDATE = 7;

    // ---- Node flags ----
    public static final int CLUSTER_NODE_MASTER    = 1;
    public static final int CLUSTER_NODE_SLAVE     = 2;
    public static final int CLUSTER_NODE_PFAIL     = 4;
    public static final int CLUSTER_NODE_FAIL      = 8;
    public static final int CLUSTER_NODE_MYSELF    = 16;
    public static final int CLUSTER_NODE_HANDSHAKE = 32;
    public static final int CLUSTER_NODE_NOADDR    = 64;
    public static final int CLUSTER_NODE_MEET      = 128;

    // ---- Protocol constants ----
    public static final int CLUSTER_NAMELEN  = 40;
    public static final int NET_IP_STR_LEN   = 46;
    public static final int CLUSTER_SLOTS    = 16384;
    public static final int CLUSTER_PROTO_VER = 1;
    public static final int LP_HDR_SIZE = 2256;  // offset of data field
    public static final int GOSSIP_ENTRY_SIZE = 106;

    // ---- Header field offsets ----
    private static final int OFF_TOTLEN     = 4;
    private static final int OFF_VER        = 8;
    private static final int OFF_PORT       = 10;
    private static final int OFF_TYPE       = 12;
    private static final int OFF_COUNT      = 14;
    private static final int OFF_EPOCH      = 16;
    private static final int OFF_CFGEPOCH   = 24;
    private static final int OFF_OFFSET     = 32;
    private static final int OFF_SENDER     = 40;
    private static final int OFF_MYSLOTS    = 80;
    private static final int OFF_SLAVEOF    = 2128;
    private static final int OFF_MYIP       = 2168;
    private static final int OFF_EXTENSIONS = 2214;
    private static final int OFF_NOTUSED1   = 2216;
    private static final int OFF_PPORT      = 2246;
    private static final int OFF_CPORT      = 2248;
    private static final int OFF_FLAGS      = 2250;
    private static final int OFF_STATE      = 2252;
    private static final int OFF_MFLAGS     = 2253;
    private static final int OFF_DATA       = 2256;

    // ---- Gossip entry offsets (relative to entry start) ----
    private static final int GOSSIP_OFF_NODENAME      = 0;
    private static final int GOSSIP_OFF_PING_SENT     = 40;
    private static final int GOSSIP_OFF_PONG_RECEIVED = 44;
    private static final int GOSSIP_OFF_IP            = 48;
    private static final int GOSSIP_OFF_PORT          = 94;
    private static final int GOSSIP_OFF_CPORT         = 96;
    private static final int GOSSIP_OFF_FLAGS         = 98;
    private static final int GOSSIP_OFF_PPORT         = 100;

    // ---- clusterMsgDataUpdate offsets (relative to data start) ----
    private static final int UPDATE_OFF_CONFIG_EPOCH = 0;
    private static final int UPDATE_OFF_NODENAME     = 8;
    private static final int UPDATE_OFF_SLOTS        = 48; // 40 bytes nodename + 8 epoch

    // ---- clusterMsgDataFail offsets (relative to data start) ----
    private static final int FAIL_OFF_NODENAME = 0;

    // ---- mflags bits ----
    public static final int CLUSTERMSG_FLAG0_EXT_DATA = (1 << 2);

    /**
     * Build a PING / PONG / MEET message.
     *
     * @param type        CLUSTERMSG_TYPE_PING/PONG/MEET
     * @param self        sender node info
     * @param selfSlots   2048-byte slots bitmap
     * @param gossipNodes nodes to include in gossip section (may be empty)
     * @param currentEpoch cluster's current epoch
     * @param configEpoch  sender's config epoch
     * @param replOffset  replication offset
     */
    public static byte[] buildPingPong(
            int type,
            ClusterNodeInfo self,
            byte[] selfSlots,
            ClusterNodeInfo[] gossipNodes,
            long currentEpoch,
            long configEpoch,
            long replOffset) {

        int gossipCount = gossipNodes == null ? 0 : gossipNodes.length;
        int totlen = OFF_DATA + gossipCount * GOSSIP_ENTRY_SIZE;
        byte[] buf = new byte[totlen];

        // sig
        buf[0] = 'R'; buf[1] = 'C'; buf[2] = 'm'; buf[3] = 'b';

        putU32BE(buf, OFF_TOTLEN, totlen);
        putU16BE(buf, OFF_VER, CLUSTER_PROTO_VER);
        putU16BE(buf, OFF_PORT, self.getPort());
        putU16BE(buf, OFF_TYPE, type);
        putU16BE(buf, OFF_COUNT, gossipCount);
        putU64BE(buf, OFF_EPOCH, currentEpoch);
        putU64BE(buf, OFF_CFGEPOCH, configEpoch);
        putU64BE(buf, OFF_OFFSET, replOffset);

        // sender name
        putFixedStr(buf, OFF_SENDER, self.getNodeId(), CLUSTER_NAMELEN);

        // slots
        if (selfSlots != null && selfSlots.length == CLUSTER_SLOTS / 8) {
            System.arraycopy(selfSlots, 0, buf, OFF_MYSLOTS, CLUSTER_SLOTS / 8);
        }

        // slaveof — zero if not a slave
        Arrays.fill(buf, OFF_SLAVEOF, OFF_SLAVEOF + CLUSTER_NAMELEN, (byte) 0);

        // myip
        byte[] ipBytes = self.getHost().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int ipLen = Math.min(ipBytes.length, NET_IP_STR_LEN - 1);
        System.arraycopy(ipBytes, 0, buf, OFF_MYIP, ipLen);

        // cport = port + 10000
        putU16BE(buf, OFF_CPORT, self.getPort() + 10000);
        putU16BE(buf, OFF_FLAGS, CLUSTER_NODE_MASTER | CLUSTER_NODE_MYSELF);
        buf[OFF_STATE] = 0; // CLUSTER_OK
        buf[OFF_MFLAGS] = (byte) CLUSTERMSG_FLAG0_EXT_DATA;

        // gossip entries
        for (int i = 0; i < gossipCount; i++) {
            writeGossipEntry(buf, OFF_DATA + i * GOSSIP_ENTRY_SIZE, gossipNodes[i]);
        }

        return buf;
    }

    /** Build a FAIL message. */
    public static byte[] buildFail(ClusterNodeInfo self, String failedNodeId,
                                   long currentEpoch, long configEpoch) {
        int totlen = OFF_DATA + CLUSTER_NAMELEN;
        byte[] buf = new byte[totlen];
        buf[0] = 'R'; buf[1] = 'C'; buf[2] = 'm'; buf[3] = 'b';
        putU32BE(buf, OFF_TOTLEN, totlen);
        putU16BE(buf, OFF_VER, CLUSTER_PROTO_VER);
        putU16BE(buf, OFF_TYPE, CLUSTERMSG_TYPE_FAIL);
        putU64BE(buf, OFF_EPOCH, currentEpoch);
        putU64BE(buf, OFF_CFGEPOCH, configEpoch);
        putFixedStr(buf, OFF_SENDER, self.getNodeId(), CLUSTER_NAMELEN);
        putFixedStr(buf, OFF_DATA + FAIL_OFF_NODENAME, failedNodeId, CLUSTER_NAMELEN);
        putU32BE(buf, OFF_TOTLEN, totlen);
        return buf;
    }

    /** Build an UPDATE message (inform about slot ownership change). */
    public static byte[] buildUpdate(ClusterNodeInfo self, ClusterNodeInfo updated,
                                     byte[] updatedSlots, long currentEpoch, long configEpoch) {
        int totlen = OFF_DATA + 8 + CLUSTER_NAMELEN + (CLUSTER_SLOTS / 8);
        byte[] buf = new byte[totlen];
        buf[0] = 'R'; buf[1] = 'C'; buf[2] = 'm'; buf[3] = 'b';
        putU32BE(buf, OFF_TOTLEN, totlen);
        putU16BE(buf, OFF_VER, CLUSTER_PROTO_VER);
        putU16BE(buf, OFF_TYPE, CLUSTERMSG_TYPE_UPDATE);
        putU64BE(buf, OFF_EPOCH, currentEpoch);
        putU64BE(buf, OFF_CFGEPOCH, configEpoch);
        putFixedStr(buf, OFF_SENDER, self.getNodeId(), CLUSTER_NAMELEN);
        // data: configEpoch(8) + nodename(40) + slots(2048)
        putU64BE(buf, OFF_DATA + UPDATE_OFF_CONFIG_EPOCH, updated.getConfigEpoch());
        putFixedStr(buf, OFF_DATA + UPDATE_OFF_NODENAME, updated.getNodeId(), CLUSTER_NAMELEN);
        if (updatedSlots != null) {
            System.arraycopy(updatedSlots, 0, buf, OFF_DATA + UPDATE_OFF_SLOTS,
                    Math.min(updatedSlots.length, CLUSTER_SLOTS / 8));
        }
        return buf;
    }

    // ---- Parsing ----

    public static boolean isValid(byte[] buf) {
        return buf != null && buf.length >= OFF_DATA
                && buf[0] == 'R' && buf[1] == 'C' && buf[2] == 'm' && buf[3] == 'b';
    }

    public static int getType(byte[] buf)         { return getU16BE(buf, OFF_TYPE); }
    public static int getTotlen(byte[] buf)        { return (int) getU32BE(buf, OFF_TOTLEN); }
    public static int getCount(byte[] buf)         { return getU16BE(buf, OFF_COUNT); }
    public static long getCurrentEpoch(byte[] buf) { return getU64BE(buf, OFF_EPOCH); }
    public static long getConfigEpoch(byte[] buf)  { return getU64BE(buf, OFF_CFGEPOCH); }
    public static long getOffset(byte[] buf)       { return getU64BE(buf, OFF_OFFSET); }
    public static int getPort(byte[] buf)          { return getU16BE(buf, OFF_PORT); }
    public static int getCport(byte[] buf)         { return getU16BE(buf, OFF_CPORT); }
    public static int getFlags(byte[] buf)         { return getU16BE(buf, OFF_FLAGS); }
    public static int getState(byte[] buf)         { return buf[OFF_STATE] & 0xFF; }

    public static String getSender(byte[] buf) {
        return getFixedStr(buf, OFF_SENDER, CLUSTER_NAMELEN);
    }

    public static byte[] getMySlots(byte[] buf) {
        byte[] slots = new byte[CLUSTER_SLOTS / 8];
        System.arraycopy(buf, OFF_MYSLOTS, slots, 0, CLUSTER_SLOTS / 8);
        return slots;
    }

    public static String getMyIp(byte[] buf) {
        return getFixedStr(buf, OFF_MYIP, NET_IP_STR_LEN);
    }

    public static String getSlaveOf(byte[] buf) {
        return getFixedStr(buf, OFF_SLAVEOF, CLUSTER_NAMELEN);
    }

    /** Parse gossip entry at given index. Returns [nodename, ip, port, cport, flags, pongReceived]. */
    public static GossipEntry getGossipEntry(byte[] buf, int index) {
        int base = OFF_DATA + index * GOSSIP_ENTRY_SIZE;
        String nodename  = getFixedStr(buf, base + GOSSIP_OFF_NODENAME, CLUSTER_NAMELEN);
        long pingSent    = getU32BE(buf, base + GOSSIP_OFF_PING_SENT);
        long pongRecv    = getU32BE(buf, base + GOSSIP_OFF_PONG_RECEIVED);
        String ip        = getFixedStr(buf, base + GOSSIP_OFF_IP, NET_IP_STR_LEN);
        int port         = getU16BE(buf, base + GOSSIP_OFF_PORT);
        int cport        = getU16BE(buf, base + GOSSIP_OFF_CPORT);
        int flags        = getU16BE(buf, base + GOSSIP_OFF_FLAGS);
        return new GossipEntry(nodename, ip, port, cport, flags, pingSent, pongRecv);
    }

    /** Parse FAIL message: returns the failed node's name. */
    public static String getFailNodeName(byte[] buf) {
        return getFixedStr(buf, OFF_DATA + FAIL_OFF_NODENAME, CLUSTER_NAMELEN);
    }

    /** Parse UPDATE message. */
    public static UpdateEntry getUpdateEntry(byte[] buf) {
        long cfgEpoch = getU64BE(buf, OFF_DATA + UPDATE_OFF_CONFIG_EPOCH);
        String nodename = getFixedStr(buf, OFF_DATA + UPDATE_OFF_NODENAME, CLUSTER_NAMELEN);
        byte[] slots = new byte[CLUSTER_SLOTS / 8];
        System.arraycopy(buf, OFF_DATA + UPDATE_OFF_SLOTS, slots, 0, CLUSTER_SLOTS / 8);
        return new UpdateEntry(cfgEpoch, nodename, slots);
    }

    // ---- Inner data classes ----

    public static final class GossipEntry {
        public final String nodename;
        public final String ip;
        public final int port;
        public final int cport;
        public final int flags;
        public final long pingSentSec;
        public final long pongReceivedSec;

        GossipEntry(String nodename, String ip, int port, int cport, int flags,
                    long pingSentSec, long pongReceivedSec) {
            this.nodename = nodename;
            this.ip = ip;
            this.port = port;
            this.cport = cport;
            this.flags = flags;
            this.pingSentSec = pingSentSec;
            this.pongReceivedSec = pongReceivedSec;
        }
    }

    public static final class UpdateEntry {
        public final long configEpoch;
        public final String nodename;
        public final byte[] slots;

        UpdateEntry(long configEpoch, String nodename, byte[] slots) {
            this.configEpoch = configEpoch;
            this.nodename = nodename;
            this.slots = slots;
        }
    }

    // ---- Write helpers ----

    private static void writeGossipEntry(byte[] buf, int base, ClusterNodeInfo n) {
        putFixedStr(buf, base + GOSSIP_OFF_NODENAME, n.getNodeId(), CLUSTER_NAMELEN);
        putU32BE(buf, base + GOSSIP_OFF_PING_SENT,     n.getPingSentMs() / 1000);
        putU32BE(buf, base + GOSSIP_OFF_PONG_RECEIVED, n.getPongReceivedMs() / 1000);
        byte[] ipBytes = n.getHost().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int ipLen = Math.min(ipBytes.length, NET_IP_STR_LEN - 1);
        Arrays.fill(buf, base + GOSSIP_OFF_IP, base + GOSSIP_OFF_IP + NET_IP_STR_LEN, (byte) 0);
        System.arraycopy(ipBytes, 0, buf, base + GOSSIP_OFF_IP, ipLen);
        putU16BE(buf, base + GOSSIP_OFF_PORT,  n.getPort());
        putU16BE(buf, base + GOSSIP_OFF_CPORT, n.getPort() + 10000);
        putU16BE(buf, base + GOSSIP_OFF_FLAGS, n.getNodeFlags());
    }

    // ---- Byte-level utilities (big-endian / network byte order) ----

    private static void putU16BE(byte[] b, int off, int v) {
        b[off]   = (byte)((v >> 8) & 0xFF);
        b[off+1] = (byte)(v & 0xFF);
    }

    private static void putU32BE(byte[] b, int off, long v) {
        b[off]   = (byte)((v >> 24) & 0xFF);
        b[off+1] = (byte)((v >> 16) & 0xFF);
        b[off+2] = (byte)((v >> 8) & 0xFF);
        b[off+3] = (byte)(v & 0xFF);
    }

    private static void putU64BE(byte[] b, int off, long v) {
        b[off]   = (byte)((v >> 56) & 0xFF);
        b[off+1] = (byte)((v >> 48) & 0xFF);
        b[off+2] = (byte)((v >> 40) & 0xFF);
        b[off+3] = (byte)((v >> 32) & 0xFF);
        b[off+4] = (byte)((v >> 24) & 0xFF);
        b[off+5] = (byte)((v >> 16) & 0xFF);
        b[off+6] = (byte)((v >> 8) & 0xFF);
        b[off+7] = (byte)(v & 0xFF);
    }

    private static void putFixedStr(byte[] b, int off, String s, int maxLen) {
        Arrays.fill(b, off, off + maxLen, (byte) 0);
        if (s == null) return;
        byte[] sb = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int len = Math.min(sb.length, maxLen);
        System.arraycopy(sb, 0, b, off, len);
    }

    // ---- Read helpers ----

    static int getU16BE(byte[] b, int off) {
        return ((b[off] & 0xFF) << 8) | (b[off+1] & 0xFF);
    }

    static long getU32BE(byte[] b, int off) {
        return ((long)(b[off] & 0xFF) << 24)
             | ((long)(b[off+1] & 0xFF) << 16)
             | ((long)(b[off+2] & 0xFF) << 8)
             | (long)(b[off+3] & 0xFF);
    }

    static long getU64BE(byte[] b, int off) {
        return ((long)(b[off] & 0xFF) << 56)
             | ((long)(b[off+1] & 0xFF) << 48)
             | ((long)(b[off+2] & 0xFF) << 40)
             | ((long)(b[off+3] & 0xFF) << 32)
             | ((long)(b[off+4] & 0xFF) << 24)
             | ((long)(b[off+5] & 0xFF) << 16)
             | ((long)(b[off+6] & 0xFF) << 8)
             | (long)(b[off+7] & 0xFF);
    }

    static String getFixedStr(byte[] b, int off, int maxLen) {
        int end = off;
        while (end < off + maxLen && b[end] != 0) end++;
        return new String(b, off, end - off, java.nio.charset.StandardCharsets.UTF_8);
    }
}
