package com.redisimpl.test.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Pub/Sub integration tests")
class PubSubIntegrationTest extends BaseIntegrationTest {

    @Test
    @DisplayName("PUBLISH returns number of subscribers that received the message")
    void publish_returnsReceiverCount() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        CountDownLatch received = new CountDownLatch(1);
        List<String> messages = new ArrayList<>();

        // Subscribe in a background thread using a fresh connection
        Jedis subscriber = new Jedis("127.0.0.1", sharedPort);
        Thread subThread = new Thread(() -> subscriber.subscribe(new JedisPubSub() {
            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                subscribed.countDown();
            }
            @Override
            public void onMessage(String channel, String message) {
                messages.add(message);
                received.countDown();
                unsubscribe(channel);
            }
        }, "test-channel"));
        subThread.setDaemon(true);
        subThread.start();

        // Wait for subscription to be established
        assertTrue(subscribed.await(3, TimeUnit.SECONDS), "Subscribe timed out");

        // Publish
        long receivers = jedis.publish("test-channel", "hello");
        assertEquals(1L, receivers);

        // Wait for message receipt
        assertTrue(received.await(3, TimeUnit.SECONDS), "Message not received");
        assertEquals("hello", messages.get(0));

        subscriber.close();
    }

    @Test
    @DisplayName("PUBLISH to channel with no subscribers returns 0")
    void publish_noSubscribers() {
        long count = jedis.publish("empty-channel-" + System.currentTimeMillis(), "msg");
        assertEquals(0L, count);
    }

    @Test
    @DisplayName("PUBSUB NUMPAT returns non-negative count")
    void pubsub_numpat() {
        long count = jedis.pubsubNumPat();
        assertTrue(count >= 0);
    }
}
