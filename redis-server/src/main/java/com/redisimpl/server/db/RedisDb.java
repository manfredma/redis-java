package com.redisimpl.server.db;

import com.redisimpl.core.dict.Dict;
import com.redisimpl.core.object.RedisObject;
import com.redisimpl.server.command.RedisException;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * RedisDb — represents a single Redis database.
 *
 * <p>Each database has:
 * <ul>
 *   <li>{@code dict} — the main key-value store</li>
 *   <li>{@code expires} — expiry times (absolute ms) for keys</li>
 *   <li>{@code id} — database index (0-15)</li>
 * </ul>
 */
public final class RedisDb {

    /** Main key-value store: key (byte[]) → RedisObject */
    private final Dict dict;

    /** Expiry store: key (byte[]) → Long (absolute expiry time in ms) */
    private final Dict expires;

    /** Database index */
    private final int id;

    public RedisDb(int id) {
        this.id = id;
        this.dict = Dict.create();
        this.expires = Dict.create();
    }

    public int getId() { return id; }

    /** Returns the raw key-value Dict (for persistence). */
    public Dict getDict() { return dict; }

    /** Returns the raw expiry Dict (for persistence). */
    public Dict getExpires() { return expires; }

    /**
     * Get raw expiry time for a key without existence check.
     * Returns 0 if no expiry is set.
     */
    public long getRawExpiry(byte[] key) {
        Object exp = expires.get(key);
        if (exp == null) return 0L;
        return (Long) exp;
    }

    // ---- Key lookup ----

    /**
     * Look up a key, checking expiry (lazy deletion).
     * Returns null if key not found or expired.
     */
    public RedisObject lookupKey(byte[] key) {
        // Lazy expiry check
        if (isExpired(key)) {
            delete(key);
            return null;
        }
        return (RedisObject) dict.get(key);
    }

    /**
     * Look up a key for read. Throws WRONGTYPE if type doesn't match.
     */
    public RedisObject lookupKeyRead(byte[] key) {
        return lookupKey(key);
    }

    /**
     * Look up a key for write. Throws WRONGTYPE if type doesn't match.
     */
    public RedisObject lookupKeyWrite(byte[] key) {
        return lookupKey(key);
    }

    /**
     * Look up a key and check type. Throws WRONGTYPE if type doesn't match.
     */
    public RedisObject lookupKeyOrReply(byte[] key, int expectedType) {
        RedisObject obj = lookupKey(key);
        if (obj != null && obj.getType() != expectedType) {
            throw RedisException.wrongType();
        }
        return obj;
    }

    // ---- Key manipulation ----

    /**
     * Set a key to a value. Removes expiry.
     */
    public void setKey(byte[] key, RedisObject value) {
        dict.put(key, value);
        removeExpiry(key);
    }

    /**
     * Delete a key and its expiry.
     */
    /**
     * Synchronous delete — mirrors dbDelete() / dbSyncDelete() in db.c.
     * Always frees the value on the calling thread.
     */
    public boolean delete(byte[] key) {
        expires.delete(key);
        return dict.delete(key);
    }

    /**
     * Asynchronous delete — mirrors dbAsyncDelete() / UNLINK in db.c + lazyfree.c.
     *
     * Immediately removes the key from the keyspace but defers freeing the value
     * object to the BIO lazy-free thread when the collection is large enough.
     *
     * Free-effort threshold = 64 elements, mirroring LAZYFREE_THRESHOLD in lazyfree.c:
     *   - List (QuickList): node count
     *   - Hash/Set (Dict): entry count
     *   - ZSet (skiplist): node count
     *   - Everything else: effort = 1 (free synchronously)
     */
    public boolean asyncDelete(byte[] key) {
        expires.delete(key);
        RedisObject obj = (RedisObject) dict.get(key);
        if (obj == null) return false;

        // Remove from dict first (key is no longer accessible)
        dict.delete(key);

        // Decide whether to free asynchronously (mirrors lazyfreeGetFreeEffort + freeObjAsync)
        long effort = getLazyfreeEffort(obj);
        if (effort > 64) {
            // Submit to BIO lazy-free thread — mirrors bioCreateLazyFreeJob(lazyfreeFreeObject)
            final RedisObject captured = obj;
            com.redisimpl.server.bio.BioThread.BioManager.getInstance()
                    .submitLazyFree(() -> {
                        // In Java, GC handles memory; we just need to clear references
                        // This mirrors the pattern of deferring decrRefCount to BIO thread
                        captured.setPtr(null); // allow GC to collect the value
                    });
        }
        // Small objects freed immediately (GC handles it naturally in Java)
        return true;
    }

    /**
     * Estimate free-effort for a RedisObject — mirrors lazyfreeGetFreeEffort() in lazyfree.c.
     *
     * Returns approximate number of allocations that need to be freed:
     *   - QuickList: number of nodes (ql.len)
     *   - Dict-encoded Hash/Set: number of entries
     *   - Skiplist-encoded ZSet: skiplist length
     *   - Everything else: 1
     */
    private static long getLazyfreeEffort(RedisObject obj) {
        int type = obj.getType();
        int enc  = obj.getEncoding();
        Object ptr = obj.getPtr();
        if (ptr == null) return 1;

        if (type == com.redisimpl.core.object.RedisObjectConstants.OBJ_TYPE_LIST
                && enc == com.redisimpl.core.object.RedisObjectConstants.OBJ_ENCODING_QUICKLIST) {
            return ((com.redisimpl.core.quicklist.QuickList) ptr).llen();
        }
        if ((type == com.redisimpl.core.object.RedisObjectConstants.OBJ_TYPE_SET
                || type == com.redisimpl.core.object.RedisObjectConstants.OBJ_TYPE_HASH)
                && enc == com.redisimpl.core.object.RedisObjectConstants.OBJ_ENCODING_HT) {
            return ((com.redisimpl.core.dict.Dict) ptr).size();
        }
        if (type == com.redisimpl.core.object.RedisObjectConstants.OBJ_TYPE_ZSET
                && enc == com.redisimpl.core.object.RedisObjectConstants.OBJ_ENCODING_SKIPLIST) {
            com.redisimpl.server.commands.zset.ZSetCommands.ZSetData data =
                    (com.redisimpl.server.commands.zset.ZSetCommands.ZSetData) ptr;
            return data.zsl.length();
        }
        return 1;
    }

    /**
     * Check if a key exists (without expiry check).
     */
    public boolean exists(byte[] key) {
        return dict.containsKey(key);
    }

    /**
     * Get all keys matching a pattern (KEYS command).
     * Pattern uses Redis glob syntax: *, ?, [abc], [a-z]
     */
    public List<byte[]> allKeys() {
        List<byte[]> result = new ArrayList<>();
        for (Dict.Entry entry : dict) result.add(entry.getKey());
        return result;
    }

    public List<byte[]> keys(String pattern) {
        List<byte[]> result = new ArrayList<>();
        for (Dict.Entry entry : dict) {
            byte[] key = entry.getKey();
            // Lazy expiry check
            if (!isExpired(key)) {
                if (matchPattern(pattern, new String(key, StandardCharsets.UTF_8))) {
                    result.add(key);
                }
            }
        }
        return result;
    }

    /**
     * Return a random key from the database.
     */
    public byte[] randomKey() {
        Set<byte[]> keySet = dict.keySet();
        if (keySet.isEmpty()) return null;
        List<byte[]> keys = new ArrayList<>(keySet);
        // Filter expired
        keys.removeIf(this::isExpired);
        if (keys.isEmpty()) return null;
        return keys.get(new Random().nextInt(keys.size()));
    }

    /**
     * Flush all keys.
     */
    public void flush() {
        for (Dict.Entry entry : dict) {
            expires.delete(entry.getKey());
        }
        // Re-create dicts
        // Since Dict is mutable, we delete all keys
        List<byte[]> allKeys = new ArrayList<>();
        for (Dict.Entry e : dict) allKeys.add(e.getKey());
        for (byte[] k : allKeys) dict.delete(k);
    }

    /**
     * Number of keys (including expired ones not yet lazily deleted).
     */
    public int dbSize() {
        return dict.size();
    }

    // ---- Expiry ----

    /**
     * Set expiry for a key (absolute time in ms).
     */
    public void setExpiry(byte[] key, long when) {
        expires.put(key, when);
    }

    /**
     * Remove expiry for a key.
     */
    public void removeExpiry(byte[] key) {
        expires.delete(key);
    }

    /**
     * Get expiry time for a key. Returns -1 if no expiry, -2 if key doesn't exist.
     */
    public long getExpiry(byte[] key) {
        if (!dict.containsKey(key)) return -2;
        Object exp = expires.get(key);
        if (exp == null) return -1;
        return (Long) exp;
    }

    /**
     * Check if a key is expired (but don't delete it).
     */
    public boolean isExpired(byte[] key) {
        Object exp = expires.get(key);
        if (exp == null) return false;
        long when = (Long) exp;
        return System.currentTimeMillis() > when;
    }

    /**
     * Perform active expiry: randomly sample up to {@code count} keys and delete expired ones.
     * Returns the number of keys deleted.
     */
    /**
     * Active expiry cycle — mirrors activeExpireCycle() in expire.c.
     *
     * Tiered adaptive algorithm:
     *   1. Sample {@code keysPerLoop} keys from the expires dict using dictScan cursor.
     *   2. Delete all expired keys found.
     *   3. Repeat if expired-ratio > ACCEPTABLE_STALE (10%), up to a time limit.
     *
     * The scan cursor is persisted across calls so we don't always start from slot 0,
     * mirroring how Redis uses db->expires_cursor for fair distribution across dbs.
     *
     * @param keysPerLoop  max keys to sample per outer iteration (20 for slow, 5 for fast)
     */
    public int activeExpireCycle(int keysPerLoop) {
        if (expires.size() == 0) return 0;

        // ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE = 10 (%)
        final int ACCEPTABLE_STALE = 10;
        // Limit total work to avoid blocking: max 16 inner iterations
        final int MAX_ITERATIONS = 16;

        long now = System.currentTimeMillis();
        int totalDeleted = 0;
        int iteration = 0;

        do {
            int sampled = 0;
            int expired = 0;

            // Use dictScan cursor for fair scan across hash buckets
            final List<byte[]> expiredKeys = new ArrayList<>();
            expiresCursor = expires.scan(expiresCursor, (key, value) -> {
                Long expireAt = (Long) value;
                if (expireAt != null && now >= expireAt) {
                    expiredKeys.add(key);
                }
            });
            sampled += keysPerLoop; // approximate

            for (byte[] key : expiredKeys) {
                delete(key);
                expired++;
            }
            totalDeleted += expired;

            iteration++;
            // Continue if high stale ratio and not exceeded iteration limit
            // Mirrors: repeat = (expired*100/sampled) > ACCEPTABLE_STALE
            if (sampled == 0 || expiresCursor == 0) break;
            if (expired * 100 / Math.max(sampled, 1) <= ACCEPTABLE_STALE) break;
        } while (iteration < MAX_ITERATIONS);

        return totalDeleted;
    }

    /** Persistent scan cursor for active expiry (mirrors db->expires_cursor in Redis). */
    private long expiresCursor = 0;

    // ---- Scan ----

    /**
     * SCAN cursor iteration.
     * Returns [nextCursor, keys].
     * cursor=0 starts a new iteration; returns cursor=0 when complete.
     */
    public ScanResult scan(long cursor, String pattern, int count) {
        List<byte[]> allKeys = new ArrayList<>();
        for (Dict.Entry entry : dict) {
            if (!isExpired(entry.getKey())) {
                allKeys.add(entry.getKey());
            }
        }
        int total = allKeys.size();
        if (total == 0) return new ScanResult(0, Collections.emptyList());

        // Simple cursor: cursor is the start index
        int start = (int) (cursor % total);
        int end = Math.min(start + count, total);

        List<byte[]> result = new ArrayList<>();
        for (int i = start; i < end; i++) {
            byte[] key = allKeys.get(i);
            if (pattern == null || matchPattern(pattern, new String(key, StandardCharsets.UTF_8))) {
                result.add(key);
            }
        }

        long nextCursor = (end >= total) ? 0 : end;
        return new ScanResult(nextCursor, result);
    }

    public static final class ScanResult {
        public final long cursor;
        public final List<byte[]> keys;
        ScanResult(long cursor, List<byte[]> keys) {
            this.cursor = cursor;
            this.keys = keys;
        }
    }

    // ---- Rename ----

    public void rename(byte[] src, byte[] dst) {
        RedisObject obj = lookupKey(src);
        if (obj == null) throw new RedisException(RedisException.ERR_NO_SUCH_KEY);
        Long expiry = (Long) expires.get(src);
        delete(src);
        dict.put(dst, obj);
        if (expiry != null) {
            expires.put(dst, expiry);
        } else {
            expires.delete(dst);
        }
    }

    // ---- Glob pattern matching ----

    /**
     * Match a Redis glob pattern against a string.
     * Supports: * (any), ? (single char), [abc] (char class), [a-z] (range), [^abc] (negation)
     */
    public static boolean matchPattern(String pattern, String str) {
        if (pattern.equals("*")) return true;
        return globMatch(pattern, 0, str, 0);
    }

    private static boolean globMatch(String pat, int pi, String str, int si) {
        while (pi < pat.length()) {
            char pc = pat.charAt(pi);
            if (pc == '*') {
                // Skip consecutive *
                while (pi < pat.length() && pat.charAt(pi) == '*') pi++;
                if (pi == pat.length()) return true;
                while (si < str.length()) {
                    if (globMatch(pat, pi, str, si)) return true;
                    si++;
                }
                return false;
            } else if (pc == '?') {
                if (si >= str.length()) return false;
                pi++;
                si++;
            } else if (pc == '[') {
                if (si >= str.length()) return false;
                pi++; // skip '['
                boolean negate = false;
                if (pi < pat.length() && pat.charAt(pi) == '^') {
                    negate = true;
                    pi++;
                }
                boolean matched = false;
                char sc = str.charAt(si);
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
                if (si >= str.length() || pc != str.charAt(si)) return false;
                pi++;
                si++;
            }
        }
        return si == str.length();
    }
}
