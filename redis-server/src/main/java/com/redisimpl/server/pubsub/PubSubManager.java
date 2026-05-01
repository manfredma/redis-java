package com.redisimpl.server.pubsub;

import com.redisimpl.server.client.RedisClient;
import com.redisimpl.server.resp.RespEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Manages Pub/Sub subscriptions and message delivery.
 * Thread-safe: uses ConcurrentHashMap for channel/pattern maps.
 */
public final class PubSubManager {

    /** channel name -> set of subscribed clients */
    private final Map<String, Set<RedisClient>> channels = new ConcurrentHashMap<>();

    /** pattern string -> set of subscribed clients */
    private final Map<String, Set<RedisClient>> patterns = new ConcurrentHashMap<>();

    // ---- Subscribe ----

    /**
     * Subscribe a client to one or more channels.
     * Returns a list of RESP-encoded subscribe replies (one per channel).
     */
    public List<byte[]> subscribe(RedisClient client, String... channelNames) {
        List<byte[]> replies = new ArrayList<>();
        for (String ch : channelNames) {
            channels.computeIfAbsent(ch, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                    .add(client);
            client.getSubscribedChannels().add(ch);
            replies.add(encodeSubscribeReply("subscribe", ch, client.getSubscribedChannels().size()
                + client.getSubscribedPatterns().size()));
        }
        return replies;
    }

    /**
     * Unsubscribe a client from one or more channels (or all if none given).
     */
    public List<byte[]> unsubscribe(RedisClient client, String... channelNames) {
        List<byte[]> replies = new ArrayList<>();
        Collection<String> toUnsub = channelNames.length > 0
            ? Arrays.asList(channelNames)
            : new ArrayList<>(client.getSubscribedChannels());

        for (String ch : toUnsub) {
            Set<RedisClient> subs = channels.get(ch);
            if (subs != null) subs.remove(client);
            client.getSubscribedChannels().remove(ch);
            replies.add(encodeSubscribeReply("unsubscribe", ch,
                client.getSubscribedChannels().size() + client.getSubscribedPatterns().size()));
        }
        return replies;
    }

    // ---- PSubscribe ----

    public List<byte[]> psubscribe(RedisClient client, String... patternStrs) {
        List<byte[]> replies = new ArrayList<>();
        for (String pat : patternStrs) {
            patterns.computeIfAbsent(pat, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                    .add(client);
            client.getSubscribedPatterns().add(pat);
            replies.add(encodeSubscribeReply("psubscribe", pat, client.getSubscribedChannels().size()
                + client.getSubscribedPatterns().size()));
        }
        return replies;
    }

    public List<byte[]> punsubscribe(RedisClient client, String... patternStrs) {
        List<byte[]> replies = new ArrayList<>();
        Collection<String> toUnsub = patternStrs.length > 0
            ? Arrays.asList(patternStrs)
            : new ArrayList<>(client.getSubscribedPatterns());

        for (String pat : toUnsub) {
            Set<RedisClient> subs = patterns.get(pat);
            if (subs != null) subs.remove(client);
            client.getSubscribedPatterns().remove(pat);
            replies.add(encodeSubscribeReply("punsubscribe", pat,
                client.getSubscribedChannels().size() + client.getSubscribedPatterns().size()));
        }
        return replies;
    }

    // ---- Publish ----

    /**
     * Publish a message to a channel.
     * Returns the number of clients that received the message.
     * The flushCallback is called for each recipient so the message is written to the socket.
     */
    public long publish(String channel, byte[] message, Consumer<RedisClient> flushCallback) {
        long count = 0;

        // Direct channel subscribers
        Set<RedisClient> subs = channels.get(channel);
        if (subs != null) {
            byte[] msg = encodeMessage("message", channel, message);
            for (RedisClient c : subs) {
                c.addReply(msg);
                if (flushCallback != null) flushCallback.accept(c);
                count++;
            }
        }

        // Pattern subscribers
        for (Map.Entry<String, Set<RedisClient>> e : patterns.entrySet()) {
            if (matchPattern(e.getKey(), channel)) {
                byte[] msg = encodePMessage(e.getKey(), channel, message);
                for (RedisClient c : e.getValue()) {
                    c.addReply(msg);
                    if (flushCallback != null) flushCallback.accept(c);
                    count++;
                }
            }
        }

        return count;
    }

    // ---- PUBSUB subcommands ----

    /** PUBSUB CHANNELS [pattern] */
    public List<String> pubsubChannels(String pattern) {
        List<String> result = new ArrayList<>();
        for (String ch : channels.keySet()) {
            if (pattern == null || pattern.isEmpty() || matchPattern(pattern, ch)) {
                if (!channels.get(ch).isEmpty()) result.add(ch);
            }
        }
        return result;
    }

    /** PUBSUB NUMSUB [channel ...] */
    public Map<String, Long> pubsubNumsub(String... channelNames) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (String ch : channelNames) {
            Set<RedisClient> subs = channels.get(ch);
            result.put(ch, subs == null ? 0L : (long) subs.size());
        }
        return result;
    }

    /** PUBSUB NUMPAT */
    public long pubsubNumpat() {
        return patterns.values().stream().mapToLong(Set::size).sum();
    }

    /** Remove a client from all subscriptions (on disconnect). */
    public void removeClient(RedisClient client) {
        for (String ch : new ArrayList<>(client.getSubscribedChannels())) {
            Set<RedisClient> subs = channels.get(ch);
            if (subs != null) subs.remove(client);
        }
        client.getSubscribedChannels().clear();
        for (String pat : new ArrayList<>(client.getSubscribedPatterns())) {
            Set<RedisClient> subs = patterns.get(pat);
            if (subs != null) subs.remove(client);
        }
        client.getSubscribedPatterns().clear();
    }

    // ---- Encoding helpers ----

    private static byte[] encodeSubscribeReply(String type, String channel, int count) {
        List<Object> items = new ArrayList<>();
        items.add(type);
        items.add(channel);
        items.add((long) count);
        return RespEncoder.encodeArray(items);
    }

    private static byte[] encodeMessage(String type, String channel, byte[] message) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write("*3\r\n".getBytes(StandardCharsets.UTF_8));
            out.write(RespEncoder.encodeBulkString(type.getBytes(StandardCharsets.UTF_8)));
            out.write(RespEncoder.encodeBulkString(channel.getBytes(StandardCharsets.UTF_8)));
            out.write(RespEncoder.encodeBulkString(message));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    private static byte[] encodePMessage(String pattern, String channel, byte[] message) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write("*4\r\n".getBytes(StandardCharsets.UTF_8));
            out.write(RespEncoder.encodeBulkString("pmessage".getBytes(StandardCharsets.UTF_8)));
            out.write(RespEncoder.encodeBulkString(pattern.getBytes(StandardCharsets.UTF_8)));
            out.write(RespEncoder.encodeBulkString(channel.getBytes(StandardCharsets.UTF_8)));
            out.write(RespEncoder.encodeBulkString(message));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    /** Redis glob pattern matching. */
    public static boolean matchPattern(String pattern, String str) {
        return globMatch(pattern, 0, str, 0);
    }

    private static boolean globMatch(String pat, int pi, String str, int si) {
        while (pi < pat.length()) {
            char p = pat.charAt(pi);
            if (p == '*') {
                while (pi < pat.length() && pat.charAt(pi) == '*') pi++;
                if (pi == pat.length()) return true;
                while (si < str.length()) {
                    if (globMatch(pat, pi, str, si)) return true;
                    si++;
                }
                return false;
            } else if (p == '?') {
                if (si >= str.length()) return false;
                si++;
                pi++;
            } else if (p == '[') {
                pi++;
                if (si >= str.length()) return false;
                char sc = str.charAt(si);
                boolean negate = pi < pat.length() && pat.charAt(pi) == '^';
                if (negate) pi++;
                boolean matched = false;
                while (pi < pat.length() && pat.charAt(pi) != ']') {
                    if (pi + 2 < pat.length() && pat.charAt(pi + 1) == '-') {
                        if (sc >= pat.charAt(pi) && sc <= pat.charAt(pi + 2)) matched = true;
                        pi += 3;
                    } else {
                        if (sc == pat.charAt(pi)) matched = true;
                        pi++;
                    }
                }
                if (pi < pat.length()) pi++; // skip ']'
                if (matched == negate) return false;
                si++;
            } else {
                if (si >= str.length() || p != str.charAt(si)) return false;
                pi++;
                si++;
            }
        }
        return si == str.length();
    }
}
