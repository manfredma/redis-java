package com.redisimpl.server.ae;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AeEventLoop — Java port of Redis's ae.c event loop.
 *
 * <p>Supports:
 * <ul>
 *   <li>File events (I/O) via Java NIO Selector</li>
 *   <li>Time events (scheduled callbacks)</li>
 * </ul>
 *
 * <p>The main loop calls {@link #aeMain()} which blocks until {@link #aeStop()} is called.
 */
public final class AeEventLoop {

    private static final Logger log = LoggerFactory.getLogger(AeEventLoop.class);

    /** Return value from AeTimeProc to cancel the event */
    public static final long AE_NOMORE = -1;

    /** Event mask: readable */
    public static final int AE_READABLE = 1;
    /** Event mask: writable */
    public static final int AE_WRITABLE = 2;

    /** File event registration */
    private static final class FileEvent {
        SelectableChannel channel;
        int mask;
        AeFileProc rfileProc;
        AeFileProc wfileProc;
        Object clientData;
    }

    /** Time event */
    private static final class TimeEvent {
        long id;
        long when; // absolute time in ms when this should fire
        AeTimeProc timeProc;
        Object clientData;
        volatile boolean deleted;
    }

    private final Selector selector;
    private final Map<SelectableChannel, FileEvent> fileEvents = new HashMap<>();
    private final List<TimeEvent> timeEvents = new ArrayList<>();
    private final AtomicLong timeEventNextId = new AtomicLong(0);
    private volatile boolean stop = false;

    /** Queue for operations submitted from other threads */
    private final ConcurrentLinkedQueue<Runnable> pendingOps = new ConcurrentLinkedQueue<>();

    public AeEventLoop() throws IOException {
        this.selector = Selector.open();
    }

    // ---- File Events ----

    public synchronized void aeCreateFileEvent(SelectableChannel channel, int mask,
                                                AeFileProc proc, Object clientData) throws IOException {
        FileEvent fe = fileEvents.computeIfAbsent(channel, k -> new FileEvent());
        fe.channel = channel;
        fe.mask |= mask;
        fe.clientData = clientData;
        if ((mask & AE_READABLE) != 0) fe.rfileProc = proc;
        if ((mask & AE_WRITABLE) != 0) fe.wfileProc = proc;

        int interestOps = 0;
        if ((fe.mask & AE_READABLE) != 0) {
            // ServerSocketChannel uses OP_ACCEPT; SocketChannel uses OP_READ
            if (channel instanceof java.nio.channels.ServerSocketChannel) {
                interestOps |= SelectionKey.OP_ACCEPT;
            } else {
                interestOps |= SelectionKey.OP_READ;
            }
        }
        if ((fe.mask & AE_WRITABLE) != 0) interestOps |= SelectionKey.OP_WRITE;

        SelectionKey key = channel.keyFor(selector);
        if (key == null) {
            channel.configureBlocking(false);
            channel.register(selector, interestOps, fe);
        } else {
            key.interestOps(interestOps);
        }
        selector.wakeup();
    }

    public synchronized void aeDeleteFileEvent(SelectableChannel channel, int mask) {
        FileEvent fe = fileEvents.get(channel);
        if (fe == null) return;
        fe.mask &= ~mask;
        if ((mask & AE_READABLE) != 0) fe.rfileProc = null;
        if ((mask & AE_WRITABLE) != 0) fe.wfileProc = null;

        SelectionKey key = channel.keyFor(selector);
        if (key != null) {
            if (fe.mask == 0) {
                key.cancel();
                fileEvents.remove(channel);
            } else {
                int interestOps = 0;
                if ((fe.mask & AE_READABLE) != 0) {
                    if (channel instanceof java.nio.channels.ServerSocketChannel) {
                        interestOps |= SelectionKey.OP_ACCEPT;
                    } else {
                        interestOps |= SelectionKey.OP_READ;
                    }
                }
                if ((fe.mask & AE_WRITABLE) != 0) interestOps |= SelectionKey.OP_WRITE;
                key.interestOps(interestOps);
            }
        }
        selector.wakeup();
    }

    // ---- Time Events ----

    public long aeCreateTimeEvent(long ms, AeTimeProc proc) {
        return aeCreateTimeEvent(ms, proc, null);
    }

    public long aeCreateTimeEvent(long ms, AeTimeProc proc, Object clientData) {
        TimeEvent te = new TimeEvent();
        te.id = timeEventNextId.getAndIncrement();
        te.when = System.currentTimeMillis() + ms;
        te.timeProc = proc;
        te.clientData = clientData;
        te.deleted = false;
        synchronized (timeEvents) {
            timeEvents.add(te);
        }
        selector.wakeup();
        return te.id;
    }

    public void aeDeleteTimeEvent(long id) {
        synchronized (timeEvents) {
            for (TimeEvent te : timeEvents) {
                if (te.id == id) {
                    te.deleted = true;
                    break;
                }
            }
        }
        selector.wakeup();
    }

    // ---- Before/After Sleep hooks (mirrors aeSetBeforeSleepProc/aeSetAfterSleepProc) ----

    @FunctionalInterface
    public interface SleepProc {
        void run(AeEventLoop el);
    }

    private volatile SleepProc beforeSleepProc;
    private volatile SleepProc afterSleepProc;

    /** Set callback invoked before each aeApiPoll() — mirrors aeSetBeforeSleepProc(). */
    public void aeSetBeforeSleepProc(SleepProc proc) { this.beforeSleepProc = proc; }
    /** Set callback invoked after each aeApiPoll() — mirrors aeSetAfterSleepProc(). */
    public void aeSetAfterSleepProc(SleepProc proc) { this.afterSleepProc = proc; }

    // ---- Main Loop ----

    public void aeMain() {
        stop = false;
        while (!stop) {
            aeProcessEvents();
        }
    }

    public void aeStop() {
        stop = true;
        selector.wakeup();
    }

    /**
     * Process pending file and time events.
     * Mirrors aeProcessEvents() in ae.c with beforeSleep/afterSleep hooks.
     */
    public void aeProcessEvents() {
        // Process pending operations (submitted via submit())
        Runnable op;
        while ((op = pendingOps.poll()) != null) {
            op.run();
        }

        // ---- beforeSleep (mirrors call before aeApiPoll in ae.c) ----
        SleepProc before = beforeSleepProc;
        if (before != null) {
            try { before.run(this); } catch (Exception e) { log.error("beforeSleep error", e); }
        }

        // Calculate timeout: time until next time event
        long timeout = calculateTimeout();

        // aeApiPoll — wait for I/O events or timeout
        try {
            if (timeout <= 0) {
                selector.selectNow();
            } else {
                selector.select(Math.min(timeout, 100)); // max 100ms to check stop flag
            }
        } catch (IOException e) {
            log.error("Selector error", e);
        }

        // ---- afterSleep (mirrors call after aeApiPoll in ae.c) ----
        SleepProc after = afterSleepProc;
        if (after != null) {
            try { after.run(this); } catch (Exception e) { log.error("afterSleep error", e); }
        }

        // Process file events
        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        Iterator<SelectionKey> it = selectedKeys.iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();
            if (!key.isValid()) continue;
            FileEvent fe = (FileEvent) key.attachment();
            if (fe == null) continue;
            if ((key.isReadable() || key.isAcceptable()) && fe.rfileProc != null) {
                fe.rfileProc.process(this, fe.channel, AE_READABLE, fe.clientData);
            }
            if (key.isValid() && key.isWritable() && fe.wfileProc != null) {
                fe.wfileProc.process(this, fe.channel, AE_WRITABLE, fe.clientData);
            }
        }

        // Process time events
        processTimeEvents();
    }

    private void processTimeEvents() {
        long now = System.currentTimeMillis();
        List<TimeEvent> toProcess;
        synchronized (timeEvents) {
            toProcess = new ArrayList<>(timeEvents);
        }

        List<TimeEvent> toRemove = new ArrayList<>();
        for (TimeEvent te : toProcess) {
            if (te.deleted) {
                toRemove.add(te);
                continue;
            }
            if (te.when <= now) {
                long retval = te.timeProc.process(te.id, te.clientData);
                if (retval == AE_NOMORE || te.deleted) {
                    toRemove.add(te);
                } else {
                    te.when = now + retval;
                }
            }
        }

        synchronized (timeEvents) {
            timeEvents.removeAll(toRemove);
        }
    }

    private long calculateTimeout() {
        long now = System.currentTimeMillis();
        long minWait = 100; // default poll interval
        synchronized (timeEvents) {
            for (TimeEvent te : timeEvents) {
                if (!te.deleted) {
                    long wait = te.when - now;
                    if (wait < minWait) minWait = wait;
                }
            }
        }
        return Math.max(0, minWait);
    }

    /**
     * Submit an operation to be executed on the event loop thread.
     */
    public void submit(Runnable op) {
        pendingOps.offer(op);
        selector.wakeup();
    }

    public void close() {
        try {
            selector.close();
        } catch (IOException e) {
            log.error("Error closing selector", e);
        }
    }
}
