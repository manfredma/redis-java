package com.redisimpl.server;

import com.redisimpl.server.ae.AeEventLoop;
import com.redisimpl.server.ae.AeFileProc;
import com.redisimpl.server.bio.BioThread;
import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.command.CommandEntry;
import com.redisimpl.server.command.CommandTable;
import com.redisimpl.server.command.RedisException;
import com.redisimpl.server.commands.generic.GenericCommands;
import com.redisimpl.server.commands.geo.GeoCommands;
import com.redisimpl.server.commands.hash.HashCommands;
import com.redisimpl.server.commands.hyperloglog.HyperLogLogCommands;
import com.redisimpl.server.commands.list.ListCommands;
import com.redisimpl.server.commands.pubsub.PubSubCommands;
import com.redisimpl.server.commands.scripting.ScriptingCommands;
import com.redisimpl.server.commands.server.ServerCommands;
import com.redisimpl.server.commands.set.SetCommands;
import com.redisimpl.server.commands.stream.StreamCommands;
import com.redisimpl.server.commands.string.StringCommands;
import com.redisimpl.server.commands.transaction.TransactionCommands;
import com.redisimpl.server.commands.zset.ZSetCommands;
import com.redisimpl.server.db.RedisDb;
import com.redisimpl.server.pubsub.PubSubManager;
import com.redisimpl.server.resp.RespDecoder;
import com.redisimpl.server.resp.RespEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RedisServer — the main server class.
 *
 * <p>Manages:
 * <ul>
 *   <li>TCP accept loop (NIO)</li>
 *   <li>Client read/write via AeEventLoop</li>
 *   <li>Command dispatch</li>
 *   <li>Multiple databases</li>
 *   <li>serverCron (every 100ms)</li>
 * </ul>
 */
public final class RedisServer {

    private static final Logger log = LoggerFactory.getLogger(RedisServer.class);

    public static final int DEFAULT_PORT = 6399;
    public static final int DEFAULT_DATABASES = 16;
    public static final int ACTIVE_EXPIRE_SAMPLE = 20;

    // ---- Configuration ----
    private final int port;
    private final String bindAddr;
    private final int numDatabases;

    // ---- Runtime state ----
    private final AeEventLoop eventLoop;
    private final CommandTable commandTable;
    private final RedisDb[] dbs;
    private final Map<SocketChannel, RedisClient> clients = new ConcurrentHashMap<>();
    private final AtomicLong totalCommandsProcessed = new AtomicLong(0);
    private final AtomicLong totalConnectionsReceived = new AtomicLong(0);
    private final long startTime = System.currentTimeMillis();

    // ---- Blocking operations ----
    /** Keys being waited on by blocked clients: key → list of waiting clients */
    private final Map<String, List<RedisClient>> blockedKeys = new ConcurrentHashMap<>();

    // ---- Pub/Sub ----
    private final PubSubManager pubSubManager = new PubSubManager();

    // ---- WATCH: key → set of watching clients ----
    private final Map<String, Set<RedisClient>> watchedKeyClients = new ConcurrentHashMap<>();

    // ---- Replication ----
    private volatile com.redisimpl.server.replication.ReplicationPropagator replicationPropagator;
    private final com.redisimpl.server.replication.ReplicationInfo replicationInfo =
            new com.redisimpl.server.replication.ReplicationInfo();

    private ServerSocketChannel serverChannel;
    private volatile boolean running = false;

    public RedisServer() throws IOException {
        this(DEFAULT_PORT);
    }

    public RedisServer(int port) throws IOException {
        this(port, "127.0.0.1", DEFAULT_DATABASES);
    }

    public RedisServer(int port, String bindAddr, int numDatabases) throws IOException {
        this.port = port;
        this.bindAddr = bindAddr;
        this.numDatabases = numDatabases;
        this.eventLoop = new AeEventLoop();
        this.dbs = new RedisDb[numDatabases];
        for (int i = 0; i < numDatabases; i++) {
            dbs[i] = new RedisDb(i);
        }
        this.commandTable = new CommandTable();
        registerCommands();
    }

    private void registerCommands() {
        commandTable.register(new StringCommands(this));
        commandTable.register(new ListCommands(this));
        commandTable.register(new HashCommands(this));
        commandTable.register(new SetCommands(this));
        commandTable.register(new ZSetCommands(this));
        commandTable.register(new GenericCommands(this));
        commandTable.register(new ServerCommands(this));
        // Phase 3 commands
        commandTable.register(new StreamCommands(this));
        commandTable.register(new PubSubCommands(this, pubSubManager));
        commandTable.register(new TransactionCommands(this));
        commandTable.register(new ScriptingCommands(this));
        commandTable.register(new HyperLogLogCommands(this));
        commandTable.register(new GeoCommands(this));
    }

    // ---- Start/Stop ----

    public void start() throws IOException {
        // Open server socket
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().setReuseAddress(true);
        serverChannel.bind(new InetSocketAddress(bindAddr, port));

        // Register accept handler
        eventLoop.aeCreateFileEvent(serverChannel, AeEventLoop.AE_READABLE,
                this::acceptClient, null);

        // Register serverCron
        eventLoop.aeCreateTimeEvent(100, (id, data) -> {
            serverCron();
            return 100; // repeat every 100ms
        });

        // Start BIO threads
        BioThread.BioManager.getInstance().start();

        // Register beforeSleep — mirrors server.c:beforeSleep()
        // Key responsibilities: flush pending client output buffers, fast active-expire
        eventLoop.aeSetBeforeSleepProc(el -> beforeSleep());

        running = true;
        log.info("Redis server started on {}:{}", bindAddr, port);

        // Run event loop (blocks until stop())
        eventLoop.aeMain();
    }

    public void stop() {
        running = false;
        eventLoop.aeStop();
        BioThread.BioManager.getInstance().stop();
        try {
            if (serverChannel != null) serverChannel.close();
        } catch (IOException e) {
            log.error("Error closing server channel", e);
        }
        // Close all client connections
        for (SocketChannel ch : clients.keySet()) {
            try { ch.close(); } catch (IOException ignored) {}
        }
        clients.clear();
        eventLoop.close();
        log.info("Redis server stopped");
    }

    // ---- Accept ----

    private void acceptClient(AeEventLoop loop, SelectableChannel channel, int mask, Object data) {
        try {
            ServerSocketChannel ssc = (ServerSocketChannel) channel;
            SocketChannel clientChannel = ssc.accept();
            if (clientChannel == null) return;
            clientChannel.configureBlocking(false);
            clientChannel.socket().setTcpNoDelay(true);

            int fd = System.identityHashCode(clientChannel);
            RedisClient client = new RedisClient(fd);
            client.setChannel(clientChannel);
            clients.put(clientChannel, client);
            totalConnectionsReceived.incrementAndGet();

            // Register read handler
            eventLoop.aeCreateFileEvent(clientChannel, AeEventLoop.AE_READABLE,
                    this::readFromClient, client);

            log.debug("New client connected: fd={}", fd);
        } catch (IOException e) {
            log.error("Error accepting client", e);
        }
    }

    // ---- Read ----

    private void readFromClient(AeEventLoop loop, SelectableChannel channel, int mask, Object data) {
        RedisClient client = (RedisClient) data;
        SocketChannel sc = (SocketChannel) channel;
        ByteBuffer buf = ByteBuffer.allocate(16384);

        try {
            int nread = sc.read(buf);
            if (nread == -1) {
                freeClient(client, sc);
                return;
            }
            if (nread == 0) return;

            buf.flip();
            // Pass only the newly-read bytes directly to the decoder
            processInputBuffer(client, buf);
        } catch (IOException e) {
            freeClient(client, sc);
        }
    }

    private void processInputBuffer(RedisClient client, ByteBuffer newData) {
        RespDecoder decoder = getOrCreateDecoder(client);
        List<byte[][]> commands = decoder.decode(newData);

        for (byte[][] argv : commands) {
            if (argv.length == 0) continue;
            client.setArgv(argv);
            processCommand(client);
        }

        // Send any pending output
        flushClient(client);
    }

    private final Map<SocketChannel, RespDecoder> decoders = new HashMap<>();

    private RespDecoder getOrCreateDecoder(RedisClient client) {
        SocketChannel sc = client.getChannel();
        return decoders.computeIfAbsent(sc, k -> new RespDecoder());
    }

    // ---- Command dispatch ----

    public void processCommand(RedisClient client) {
        byte[][] argv = client.getArgv();
        if (argv == null || argv.length == 0) return;

        String cmdName = new String(argv[0], StandardCharsets.UTF_8).toLowerCase();
        CommandEntry cmd = commandTable.lookup(cmdName);

        if (cmd == null) {
            client.addReply(RespEncoder.encodeError(
                    "ERR unknown command '" + cmdName + "', with args beginning with: "
                            + (argv.length > 1 ? "'" + new String(argv[1], StandardCharsets.UTF_8) + "'" : "")));
            return;
        }

        if (!cmd.isArityValid(argv.length)) {
            client.addReply(RespEncoder.encodeError(
                    "ERR wrong number of arguments for '" + cmdName + "' command"));
            return;
        }

        // If in MULTI, queue commands (except EXEC, DISCARD, MULTI, WATCH)
        if (client.isInMulti()
                && !cmdName.equals("exec")
                && !cmdName.equals("discard")
                && !cmdName.equals("multi")
                && !cmdName.equals("watch")) {
            client.getTxQueue().add(argv);
            client.addReply(RespEncoder.encodeSimpleString("QUEUED"));
            totalCommandsProcessed.incrementAndGet();
            return;
        }

        // Cluster slot check: for commands with a key argument, verify we own the slot
        if (clusterSlotChecker != null && cmd.getFirstKey() >= 1 && argv.length > cmd.getFirstKey()) {
            byte[] movedError = clusterSlotChecker.check(argv);
            if (movedError != null) {
                client.addReply(movedError);
                return;
            }
        }

        client.setCmd(cmd);
        try {
            byte[] reply = cmd.execute(client, argv);
            if (reply != null) {
                client.addReply(reply);
            }
            // Notify WATCH observers if this is a write command
            if (cmd.getFlags().contains("write")) {
                notifyWatchedKeys(argv, client.getDb());
                // Propagate write command to replicas
                if (replicationPropagator != null) {
                    replicationPropagator.propagate(argv);
                }
            }
        } catch (RedisException e) {
            client.addReply(RespEncoder.encodeError(e.getMessage()));
        } catch (Exception e) {
            log.error("Command error [{}]: {}", cmdName, e.getMessage(), e);
            client.addReply(RespEncoder.encodeError("ERR internal error"));
        }

        totalCommandsProcessed.incrementAndGet();
    }

    /**
     * Execute a single command argv for a client and return the raw RESP reply.
     * Used by EXEC to run queued commands.
     */
    public byte[] executeCommand(RedisClient client, byte[][] argv) {
        String cmdName = new String(argv[0], StandardCharsets.UTF_8).toLowerCase();
        CommandEntry cmd = commandTable.lookup(cmdName);
        if (cmd == null) {
            return RespEncoder.encodeError("ERR unknown command '" + cmdName + "'");
        }
        if (!cmd.isArityValid(argv.length)) {
            return RespEncoder.encodeError("ERR wrong number of arguments for '" + cmdName + "' command");
        }
        try {
            byte[] reply = cmd.execute(client, argv);
            if (cmd.getFlags().contains("write")) {
                notifyWatchedKeys(argv, client.getDb());
            }
            return reply != null ? reply : RespEncoder.NULL_BULK;
        } catch (RedisException e) {
            return RespEncoder.encodeError(e.getMessage());
        } catch (Exception e) {
            log.error("Command error in EXEC [{}]: {}", cmdName, e.getMessage(), e);
            return RespEncoder.encodeError("ERR internal error");
        }
    }

    // ---- WATCH support ----

    public void registerWatch(RedisClient client, String key) {
        watchedKeyClients
            .computeIfAbsent(key, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
            .add(client);
    }

    public void unregisterWatches(RedisClient client) {
        for (String key : client.getWatchedKeys()) {
            Set<RedisClient> watchers = watchedKeyClients.get(key);
            if (watchers != null) watchers.remove(client);
        }
    }

    /** Called after any write command; marks watching clients as dirty. */
    private void notifyWatchedKeys(byte[][] argv, int dbIndex) {
        // Keys are at positions firstKey..lastKey (simplified: check all argv[1..])
        for (int i = 1; i < argv.length; i++) {
            String key = new String(argv[i], StandardCharsets.UTF_8);
            Set<RedisClient> watchers = watchedKeyClients.get(key);
            if (watchers != null) {
                for (RedisClient watcher : watchers) {
                    if (watcher.getDb() == dbIndex) {
                        watcher.setTxDirty(true);
                    }
                }
            }
        }
    }

    // ---- Write ----

    public void flushClient(RedisClient client) {
        if (!client.hasPendingOutput()) return;
        SocketChannel sc = client.getChannel();
        if (sc == null || !sc.isOpen()) return;

        try {
            byte[] output = client.getPendingOutput();
            ByteBuffer buf = ByteBuffer.wrap(output);
            while (buf.hasRemaining()) {
                sc.write(buf);
            }
            client.resetOutputBuffers();
        } catch (IOException e) {
            freeClient(client, sc);
        }
    }

    // ---- Client cleanup ----

    private void freeClient(RedisClient client, SocketChannel sc) {
        try {
            eventLoop.aeDeleteFileEvent(sc, AeEventLoop.AE_READABLE | AeEventLoop.AE_WRITABLE);
            sc.close();
        } catch (IOException ignored) {}
        clients.remove(sc);
        decoders.remove(sc);
        log.debug("Client disconnected: fd={}", client.getFd());
    }

    // ---- serverCron ----

    private void serverCron() {
        // Active expiry: slow cycle — sample 20 keys per DB (mirrors activeExpireCycle SLOW)
        for (RedisDb db : dbs) {
            db.activeExpireCycle(ACTIVE_EXPIRE_SAMPLE);
        }
    }

    // ---- beforeSleep (mirrors server.c beforeSleep()) ----

    /**
     * Called before each aeApiPoll() — mirrors beforeSleep() in server.c.
     *
     * Key responsibilities:
     * 1. Flush pending client output buffers (handleClientsWithPendingWrites)
     * 2. Fast active-expire cycle (ACTIVE_EXPIRE_CYCLE_FAST)
     * 3. Flush AOF buffer if applicable
     */
    private void beforeSleep() {
        // 1. Flush all pending client output (mirrors handleClientsWithPendingWrites)
        for (Map.Entry<SocketChannel, RedisClient> entry : clients.entrySet()) {
            RedisClient client = entry.getValue();
            if (client.hasPendingOutput()) {
                try {
                    flushClient(client);
                } catch (Exception e) {
                    log.debug("Error flushing client in beforeSleep: {}", e.getMessage());
                }
            }
        }

        // 2. Fast active-expire cycle: sample 5 keys per DB (lighter than slow cycle)
        // Mirrors activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST) in server.c
        for (RedisDb db : dbs) {
            db.activeExpireCycle(5);
        }
    }

    // ---- Accessors ----

    public RedisDb getDb(int index) {
        if (index < 0 || index >= dbs.length) {
            throw RedisException.dbIndex();
        }
        return dbs[index];
    }

    public RedisDb[] getDbs() { return dbs; }
    public int getNumDatabases() { return numDatabases; }
    public CommandTable getCommandTable() { return commandTable; }
    public long getTotalCommandsProcessed() { return totalCommandsProcessed.get(); }
    public long getTotalConnectionsReceived() { return totalConnectionsReceived.get(); }
    public long getStartTime() { return startTime; }
    public int getPort() { return port; }
    public int getConnectedClients() { return clients.size(); }
    public Map<String, List<RedisClient>> getBlockedKeys() { return blockedKeys; }

    // ---- Cluster ----
    private volatile com.redisimpl.server.cluster.ClusterSlotChecker clusterSlotChecker;

    public void setClusterSlotChecker(com.redisimpl.server.cluster.ClusterSlotChecker c) {
        this.clusterSlotChecker = c;
    }

    // ---- Replication ----
    public void setReplicationPropagator(
            com.redisimpl.server.replication.ReplicationPropagator p) {
        this.replicationPropagator = p;
    }
    public com.redisimpl.server.replication.ReplicationInfo getReplicationInfo() {
        return replicationInfo;
    }
    public PubSubManager getPubSubManager() { return pubSubManager; }
    public AeEventLoop getEventLoop() { return eventLoop; }

    // ---- Main ----

    public static void main(String[] args) throws IOException {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[0]);
                System.exit(1);
            }
        }
        RedisServer server = new RedisServer(port);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}
