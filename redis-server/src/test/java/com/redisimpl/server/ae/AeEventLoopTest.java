package com.redisimpl.server.ae;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AeEventLoopTest {

    private AeEventLoop loop;

    @BeforeEach
    void setUp() throws Exception {
        loop = new AeEventLoop();
    }

    @AfterEach
    void tearDown() {
        loop.aeStop();
    }

    @Test
    void createAndStop_noException() {
        assertNotNull(loop);
        loop.aeStop();
    }

    @Test
    void timeEvent_firesOnce() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        loop.aeCreateTimeEvent(50, (id, clientData) -> {
            count.incrementAndGet();
            latch.countDown();
            return AeEventLoop.AE_NOMORE; // fire once
        });

        Thread loopThread = new Thread(() -> loop.aeMain());
        loopThread.setDaemon(true);
        loopThread.start();

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        loop.aeStop();
        loopThread.join(1000);
        assertEquals(1, count.get());
    }

    @Test
    void timeEvent_firesRepeatedly() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        loop.aeCreateTimeEvent(30, (id, clientData) -> {
            count.incrementAndGet();
            latch.countDown();
            return 30; // reschedule in 30ms
        });

        Thread loopThread = new Thread(() -> loop.aeMain());
        loopThread.setDaemon(true);
        loopThread.start();

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        loop.aeStop();
        loopThread.join(1000);
        assertTrue(count.get() >= 3);
    }

    @Test
    void deleteTimeEvent_stopsExecution() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        long[] idHolder = new long[1];

        idHolder[0] = loop.aeCreateTimeEvent(20, (id, clientData) -> {
            count.incrementAndGet();
            loop.aeDeleteTimeEvent(idHolder[0]);
            return AeEventLoop.AE_NOMORE;
        });

        Thread loopThread = new Thread(() -> loop.aeMain());
        loopThread.setDaemon(true);
        loopThread.start();

        Thread.sleep(300);
        loop.aeStop();
        loopThread.join(1000);
        // Should fire at most once
        assertTrue(count.get() <= 1);
    }

    @Test
    void multipleTimeEvents_allFire() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            loop.aeCreateTimeEvent(50 + i * 10, (id, clientData) -> {
                count.incrementAndGet();
                latch.countDown();
                return AeEventLoop.AE_NOMORE;
            });
        }

        Thread loopThread = new Thread(() -> loop.aeMain());
        loopThread.setDaemon(true);
        loopThread.start();

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        loop.aeStop();
        loopThread.join(1000);
        assertEquals(3, count.get());
    }

    @Test
    void aeStop_terminatesLoop() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);

        Thread loopThread = new Thread(() -> {
            started.countDown();
            loop.aeMain();
            stopped.countDown();
        });
        loopThread.setDaemon(true);
        loopThread.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        loop.aeStop();
        assertTrue(stopped.await(2, TimeUnit.SECONDS));
    }
}
