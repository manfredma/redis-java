package com.redisimpl.server.bio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BIO (Background I/O) worker thread.
 *
 * <p>Redis uses 3 BIO threads:
 * <ul>
 *   <li>BIO_CLOSE_FILE — close file descriptors asynchronously</li>
 *   <li>BIO_AOF_FSYNC — fsync AOF file asynchronously</li>
 *   <li>BIO_LAZY_FREE — lazy free of large data structures</li>
 * </ul>
 */
public final class BioThread {

    private static final Logger log = LoggerFactory.getLogger(BioThread.class);

    private final BioJob.Type type;
    private final BlockingQueue<BioJob> queue;
    private final Thread thread;
    private volatile boolean running;
    private final AtomicLong pendingJobs = new AtomicLong(0);

    public BioThread(BioJob.Type type) {
        this.type = type;
        this.queue = new LinkedBlockingQueue<>();
        this.running = true;
        this.thread = new Thread(this::run, "bio-" + type.name().toLowerCase());
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    public void submit(Runnable task) {
        queue.offer(new BioJob(type, task));
        pendingJobs.incrementAndGet();
    }

    public long pendingJobCount() {
        return pendingJobs.get();
    }

    public void stop() {
        running = false;
        thread.interrupt();
    }

    public void join(long timeoutMs) throws InterruptedException {
        thread.join(timeoutMs);
    }

    private void run() {
        log.debug("BIO thread {} started", type);
        while (running) {
            try {
                BioJob job = queue.poll(100, TimeUnit.MILLISECONDS);
                if (job != null) {
                    try {
                        job.getTask().run();
                    } catch (Exception e) {
                        log.error("BIO job error [{}]: {}", type, e.getMessage(), e);
                    } finally {
                        pendingJobs.decrementAndGet();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.debug("BIO thread {} stopped", type);
    }

    /** Singleton manager for all 3 BIO threads */
    public static final class BioManager {
        private static final BioManager INSTANCE = new BioManager();

        private final BioThread closeFileBio;
        private final BioThread aofFsyncBio;
        private final BioThread lazyFreeBio;

        private BioManager() {
            closeFileBio = new BioThread(BioJob.Type.CLOSE_FILE);
            aofFsyncBio  = new BioThread(BioJob.Type.AOF_FSYNC);
            lazyFreeBio  = new BioThread(BioJob.Type.LAZY_FREE);
        }

        public static BioManager getInstance() { return INSTANCE; }

        public void start() {
            closeFileBio.start();
            aofFsyncBio.start();
            lazyFreeBio.start();
        }

        public void stop() {
            closeFileBio.stop();
            aofFsyncBio.stop();
            lazyFreeBio.stop();
        }

        public void submitCloseFile(Runnable task) { closeFileBio.submit(task); }
        public void submitAofFsync(Runnable task)  { aofFsyncBio.submit(task); }
        public void submitLazyFree(Runnable task)  { lazyFreeBio.submit(task); }

        public long pendingCloseFile() { return closeFileBio.pendingJobCount(); }
        public long pendingAofFsync()  { return aofFsyncBio.pendingJobCount(); }
        public long pendingLazyFree()  { return lazyFreeBio.pendingJobCount(); }
    }
}
