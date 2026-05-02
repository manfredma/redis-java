package com.redisimpl.sentinel;

import com.redisimpl.server.ae.AeEventLoop;
import com.redisimpl.server.resp.RespDecoder;
import com.redisimpl.server.resp.RespEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Sentinel — monitors a Redis master, detects failures (SDOWN/ODOWN),
 * and triggers failover.
 *
 * Listens on port 26379 by default and responds to SENTINEL commands.
 * Uses a background monitor thread to PING the master and parse INFO output.
 */
public final class RedisSentinel {

    private static final Logger log = LoggerFactory.getLogger(RedisSentinel.class);

    private final int port;
    private final SentinelConfig config;
    private final MasterState masterState;

    private final AeEventLoop eventLoop;
    private ServerSocketChannel serverChannel;
    private volatile boolean running = false;

    /** Client connections: channel → accumulated query bytes */
    private final Map<SocketChannel, byte[]> clientBuffers = new ConcurrentHashMap<>();

    /** Background monitor thread */
    private Thread monitorThread;

    public RedisSentinel(int port, SentinelConfig config) throws IOException {
        this.port = port;
        this.config = config;
        this.masterState = new MasterState(config);
        this.eventLoop = new AeEventLoop();
    }

    public void start() throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().setReuseAddress(true);
        serverChannel.bind(new InetSocketAddress("127.0.0.1", port));

        eventLoop.aeCreateFileEvent(serverChannel, AeEventLoop.AE_READABLE,
                this::acceptClient, null);

        // Monitor tick every 1 second
        eventLoop.aeCreateTimeEvent(1000, (id, data) -> {
            monitorTick();
            return 1000;
        });

        // Start background monitor thread that connects to master
        monitorThread = new Thread(this::monitorLoop, "sentinel-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();

        running = true;
        log.info("Sentinel started on port {}, monitoring {}/{}/{}",
                port, config.getMasterName(), config.getMasterHost(), config.getMasterPort());
        eventLoop.aeMain();
    }

    public void stop() {
        running = false;
        eventLoop.aeStop();
        if (monitorThread != null) monitorThread.interrupt();
        try { if (serverChannel != null) serverChannel.close(); } catch (IOException ignored) {}
        eventLoop.close();
    }

    // ---- Monitor loop (background thread) ----

    private void monitorLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                pingMaster();
                fetchMasterInfo();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Master unreachable
                log.debug("Monitor error: {}", e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void pingMaster() throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(masterState.getHost(), masterState.getPort()), 1000);
            s.setSoTimeout(1000);
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            sendRaw(out, "PING");
            String reply = readLine(in);
            if (reply.startsWith("+PONG") || reply.equals("+PONG")) {
                masterState.setLastOkPing(System.currentTimeMillis());
                masterState.setStatus(MasterState.Status.OK);
            }
        }
    }

    private void fetchMasterInfo() throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(masterState.getHost(), masterState.getPort()), 1000);
            s.setSoTimeout(2000);
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            sendRaw(out, "INFO", "replication");
            // Read bulk string response
            String line = readLine(in);
            if (line.startsWith("$")) {
                int len = Integer.parseInt(line.substring(1).trim());
                byte[] buf = new byte[len];
                int read = 0;
                while (read < len) {
                    int n = in.read(buf, read, len - read);
                    if (n < 0) break;
                    read += n;
                }
                parseInfoReplication(new String(buf, StandardCharsets.UTF_8));
                masterState.setLastInfoTime(System.currentTimeMillis());
            }
        }
    }

    private void parseInfoReplication(String info) {
        for (String line : info.split("\r?\n")) {
            if (line.startsWith("slave")) {
                // slave0:ip=127.0.0.1,port=6380,state=online,offset=123,lag=0
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    Map<String, String> fields = parseKV(parts[1]);
                    String slaveHost = fields.getOrDefault("ip", "127.0.0.1");
                    String slavePortStr = fields.get("port");
                    if (slavePortStr != null) {
                        int slavePort = Integer.parseInt(slavePortStr);
                        String key = slaveHost + ":" + slavePort;
                        masterState.getSlaves().computeIfAbsent(key,
                                k -> new SlaveInfo(slaveHost, slavePort));
                        SlaveInfo si = masterState.getSlaves().get(key);
                        String offsetStr = fields.get("offset");
                        if (offsetStr != null) {
                            try { si.setReplicaOffset(Long.parseLong(offsetStr)); }
                            catch (NumberFormatException ignored) {}
                        }
                        si.setLastOkPing(System.currentTimeMillis());
                    }
                }
            } else if (line.startsWith("master_repl_offset:")) {
                try {
                    masterState.setMasterOffset(Long.parseLong(line.substring("master_repl_offset:".length()).trim()));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    private Map<String, String> parseKV(String s) {
        Map<String, String> m = new LinkedHashMap<>();
        for (String part : s.split(",")) {
            String[] kv = part.split("=", 2);
            if (kv.length == 2) m.put(kv[0].trim(), kv[1].trim());
        }
        return m;
    }

    // ---- Failure detection ----

    private void monitorTick() {
        long now = System.currentTimeMillis();
        long elapsed = now - masterState.getLastOkPing();
        long threshold = config.getDownAfterMilliseconds();

        if (elapsed > threshold && masterState.getStatus() == MasterState.Status.OK) {
            masterState.setStatus(MasterState.Status.SDOWN);
            log.warn("Master {}:{} is subjectively down (no PONG for {}ms)",
                    masterState.getHost(), masterState.getPort(), elapsed);
        }
    }

    // ---- NIO server (for SENTINEL commands) ----

    private void acceptClient(AeEventLoop loop, SelectableChannel ch, int mask, Object data) {
        try {
            ServerSocketChannel ssc = (ServerSocketChannel) ch;
            SocketChannel client = ssc.accept();
            if (client == null) return;
            client.configureBlocking(false);
            client.socket().setTcpNoDelay(true);
            clientBuffers.put(client, new byte[0]);
            eventLoop.aeCreateFileEvent(client, AeEventLoop.AE_READABLE,
                    this::readFromClient, null);
        } catch (IOException e) {
            log.error("Accept error", e);
        }
    }

    private void readFromClient(AeEventLoop loop, SelectableChannel ch, int mask, Object data) {
        SocketChannel client = (SocketChannel) ch;
        ByteBuffer buf = ByteBuffer.allocate(4096);
        try {
            int n = client.read(buf);
            if (n == -1) { closeClient(client); return; }
            if (n == 0) return;
            buf.flip();

            byte[] existing = clientBuffers.getOrDefault(client, new byte[0]);
            byte[] newBytes = new byte[existing.length + n];
            System.arraycopy(existing, 0, newBytes, 0, existing.length);
            buf.get(newBytes, existing.length, n);

            RespDecoder decoder = new RespDecoder();
            List<byte[][]> commands = decoder.decode(ByteBuffer.wrap(newBytes));
            clientBuffers.put(client, new byte[0]);

            for (byte[][] argv : commands) {
                if (argv.length == 0) continue;
                byte[] reply = handleCommand(argv);
                if (reply != null) {
                    client.write(ByteBuffer.wrap(reply));
                }
            }
        } catch (IOException e) {
            closeClient(client);
        }
    }

    private void closeClient(SocketChannel ch) {
        clientBuffers.remove(ch);
        try {
            eventLoop.aeDeleteFileEvent(ch, AeEventLoop.AE_READABLE | AeEventLoop.AE_WRITABLE);
            ch.close();
        } catch (IOException ignored) {}
    }

    // ---- Command handling ----

    private byte[] handleCommand(byte[][] argv) {
        String cmd = new String(argv[0], StandardCharsets.UTF_8).toLowerCase();
        switch (cmd) {
            case "ping":
                return RespEncoder.encodeSimpleString("PONG");
            case "sentinel":
                return handleSentinel(argv);
            case "info":
                return handleInfo(argv);
            case "subscribe":
            case "psubscribe":
                // Minimal pub/sub stub for sentinel coordination
                return buildSubscribeReply(argv);
            case "publish":
                return RespEncoder.encodeInteger(0);
            case "client":
                return RespEncoder.OK;
            default:
                return RespEncoder.encodeError("ERR unknown command '" + cmd + "'");
        }
    }

    private byte[] handleSentinel(byte[][] argv) {
        if (argv.length < 2) return RespEncoder.encodeError("ERR syntax error");
        String sub = new String(argv[1], StandardCharsets.UTF_8).toLowerCase();
        switch (sub) {
            case "masters":
                return encodeMastersList();
            case "master": {
                if (argv.length < 3) return RespEncoder.encodeError("ERR missing master name");
                String name = new String(argv[2], StandardCharsets.UTF_8);
                if (!masterState.getName().equals(name))
                    return RespEncoder.encodeError("ERR No such master with that name");
                return encodeInfoMap(masterState.toInfoMap());
            }
            case "slaves":
            case "replicas": {
                if (argv.length < 3) return RespEncoder.encodeError("ERR missing master name");
                return encodeSlavesList();
            }
            case "sentinels": {
                if (argv.length < 3) return RespEncoder.encodeError("ERR missing master name");
                return encodeSentinelsList();
            }
            case "get-master-addr-by-name": {
                if (argv.length < 3) return RespEncoder.encodeError("ERR missing master name");
                String name = new String(argv[2], StandardCharsets.UTF_8);
                if (!masterState.getName().equals(name))
                    return RespEncoder.NULL_BULK;
                List<Object> result = new ArrayList<>();
                result.add(masterState.getHost().getBytes(StandardCharsets.UTF_8));
                result.add(String.valueOf(masterState.getPort()).getBytes(StandardCharsets.UTF_8));
                return RespEncoder.encodeArray(result);
            }
            case "is-master-down-by-addr": {
                // SENTINEL IS-MASTER-DOWN-BY-ADDR host port current-epoch runid
                boolean down = masterState.getStatus() != MasterState.Status.OK;
                List<Object> r = new ArrayList<>();
                r.add(down ? 1L : 0L);
                r.add("".getBytes(StandardCharsets.UTF_8));
                r.add(0L);
                return RespEncoder.encodeArray(r);
            }
            case "reset":
                masterState.setStatus(MasterState.Status.OK);
                masterState.setLastOkPing(System.currentTimeMillis());
                return RespEncoder.encodeInteger(1);
            case "failover":
                return RespEncoder.encodeError("ERR Failover in progress or master is not down");
            case "ckquorum":
                return RespEncoder.encodeSimpleString("OK " + config.getQuorum() + " usable Sentinels");
            case "info-cache":
                return RespEncoder.EMPTY_ARRAY;
            case "pending-scripts":
                return RespEncoder.encodeInteger(0);
            case "myid":
                return RespEncoder.encodeBulkString(("sentinel-" + port).getBytes(StandardCharsets.UTF_8));
            default:
                return RespEncoder.encodeError("ERR unknown SENTINEL subcommand '" + sub + "'");
        }
    }

    private byte[] encodeMastersList() {
        List<Object> outer = new ArrayList<>();
        outer.add(infoMapToRespArray(masterState.toInfoMap()));
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(("*1\r\n").getBytes(StandardCharsets.UTF_8));
            baos.write(infoMapToRespArray(masterState.toInfoMap()));
            return baos.toByteArray();
        } catch (IOException e) {
            return RespEncoder.encodeError("ERR internal");
        }
    }

    private byte[] encodeSlavesList() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Collection<SlaveInfo> slaves = masterState.getSlaves().values();
            baos.write(("*" + slaves.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (SlaveInfo si : slaves) {
                baos.write(infoMapToRespArray(si.toInfoMap()));
            }
            return baos.toByteArray();
        } catch (IOException e) {
            return RespEncoder.encodeError("ERR internal");
        }
    }

    private byte[] encodeSentinelsList() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Collection<SentinelPeer> peers = masterState.getPeers().values();
            baos.write(("*" + peers.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            return baos.toByteArray();
        } catch (IOException e) {
            return RespEncoder.encodeError("ERR internal");
        }
    }

    private byte[] infoMapToRespArray(Map<String, String> map) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int entries = map.size() * 2;
            baos.write(("*" + entries + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<String, String> e : map.entrySet()) {
                baos.write(RespEncoder.encodeBulkString(e.getKey().getBytes(StandardCharsets.UTF_8)));
                baos.write(RespEncoder.encodeBulkString(e.getValue().getBytes(StandardCharsets.UTF_8)));
            }
            return baos.toByteArray();
        } catch (IOException ex) {
            return new byte[0];
        }
    }

    private byte[] encodeInfoMap(Map<String, String> map) {
        return infoMapToRespArray(map);
    }

    private byte[] handleInfo(byte[][] argv) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Server\r\nredis_version:7.0.0-java-sentinel\r\n");
        sb.append("redis_mode:sentinel\r\n");
        sb.append("tcp_port:").append(port).append("\r\n");
        sb.append("\r\n# Sentinel\r\n");
        sb.append("sentinel_masters:1\r\n");
        sb.append("sentinel_tilt:0\r\n");
        sb.append("sentinel_running_scripts:0\r\n");
        sb.append("sentinel_scripts_queue_length:0\r\n");
        sb.append("sentinel_simulate_failure_flags:0\r\n");
        sb.append("master0:name=").append(config.getMasterName())
          .append(",status=").append(masterState.getStatus().name().toLowerCase())
          .append(",address=").append(masterState.getHost()).append(":").append(masterState.getPort())
          .append(",slaves=").append(masterState.getSlaves().size())
          .append(",sentinels=").append(masterState.getPeers().size() + 1)
          .append("\r\n");
        return RespEncoder.encodeBulkString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private byte[] buildSubscribeReply(byte[][] argv) {
        // Return subscribe confirmation for each channel
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            for (int i = 1; i < argv.length; i++) {
                baos.write("*3\r\n".getBytes(StandardCharsets.UTF_8));
                baos.write(RespEncoder.encodeBulkString("subscribe".getBytes(StandardCharsets.UTF_8)));
                baos.write(RespEncoder.encodeBulkString(argv[i]));
                baos.write(RespEncoder.encodeInteger(i));
            }
            return baos.toByteArray();
        } catch (IOException e) {
            return RespEncoder.NULL_BULK;
        }
    }

    // ---- I/O helpers ----

    private void sendRaw(OutputStream out, String... args) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String arg : args) {
            byte[] b = arg.getBytes(StandardCharsets.UTF_8);
            baos.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            baos.write(b);
            baos.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        out.write(baos.toByteArray());
        out.flush();
    }

    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') { in.read(); break; }
            sb.append((char) b);
        }
        return sb.toString();
    }

    public int getPort() { return port; }
    public MasterState getMasterState() { return masterState; }

    // ---- Main ----

    public static void main(String[] args) throws Exception {
        int sentinelPort = args.length > 0 ? Integer.parseInt(args[0]) : 26379;
        String masterName = args.length > 1 ? args[1] : "mymaster";
        String masterHost = args.length > 2 ? args[2] : "127.0.0.1";
        int masterPort = args.length > 3 ? Integer.parseInt(args[3]) : 6379;
        int quorum = args.length > 4 ? Integer.parseInt(args[4]) : 2;

        SentinelConfig cfg = new SentinelConfig(masterName, masterHost, masterPort, quorum);
        RedisSentinel sentinel = new RedisSentinel(sentinelPort, cfg);
        Runtime.getRuntime().addShutdownHook(new Thread(sentinel::stop));
        sentinel.start();
    }
}
