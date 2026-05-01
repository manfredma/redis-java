package com.redisimpl.persistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * RedisConfig — persistence-related configuration.
 *
 * <p>Mirrors the relevant subset of redis.conf options for RDB and AOF.
 */
public final class RedisConfig {

    // ---- RDB save conditions ----
    /** List of (seconds, changes) pairs that trigger an automatic BGSAVE. */
    private final List<SaveCondition> saveConditions;

    // ---- RDB file settings ----
    /** Working directory for persistence files. */
    private String dir;

    /** RDB dump filename. */
    private String dbfilename;

    // ---- AOF settings ----
    /** Whether AOF is enabled. */
    private boolean appendonly;

    /** AOF filename. */
    private String appendfilename;

    /** fsync strategy: "always", "everysec", "no". */
    private String appendfsync;

    /** Whether to use RDB preamble in AOF rewrites. */
    private boolean aofUseRdbPreamble;

    public RedisConfig() {
        this.saveConditions = new ArrayList<>();
        // Default save conditions: 900/1, 300/10, 60/10000
        this.saveConditions.add(new SaveCondition(900, 1));
        this.saveConditions.add(new SaveCondition(300, 10));
        this.saveConditions.add(new SaveCondition(60, 10000));

        this.dir = "./";
        this.dbfilename = "dump.rdb";
        this.appendonly = false;
        this.appendfilename = "appendonly.aof";
        this.appendfsync = "everysec";
        this.aofUseRdbPreamble = true;
    }

    // ---- Accessors ----

    public List<SaveCondition> getSaveConditions() {
        return Collections.unmodifiableList(saveConditions);
    }

    public void addSaveCondition(int seconds, int changes) {
        saveConditions.add(new SaveCondition(seconds, changes));
    }

    public void clearSaveConditions() {
        saveConditions.clear();
    }

    public String getDir() { return dir; }
    public void setDir(String dir) { this.dir = dir; }

    public String getDbfilename() { return dbfilename; }
    public void setDbfilename(String dbfilename) { this.dbfilename = dbfilename; }

    public boolean isAppendonly() { return appendonly; }
    public void setAppendonly(boolean appendonly) { this.appendonly = appendonly; }

    public String getAppendfilename() { return appendfilename; }
    public void setAppendfilename(String appendfilename) { this.appendfilename = appendfilename; }

    public String getAppendfsync() { return appendfsync; }
    public void setAppendfsync(String appendfsync) { this.appendfsync = appendfsync; }

    public boolean isAofUseRdbPreamble() { return aofUseRdbPreamble; }
    public void setAofUseRdbPreamble(boolean aofUseRdbPreamble) {
        this.aofUseRdbPreamble = aofUseRdbPreamble;
    }

    /**
     * Returns the full path to the RDB file.
     */
    public String getRdbFilePath() {
        return resolveFilePath(dbfilename);
    }

    /**
     * Returns the full path to the AOF file.
     */
    public String getAofFilePath() {
        return resolveFilePath(appendfilename);
    }

    private String resolveFilePath(String filename) {
        if (filename == null || filename.isEmpty()) return filename;
        String d = dir;
        if (d == null || d.isEmpty()) d = "./";
        if (!d.endsWith("/") && !d.endsWith("\\")) d = d + "/";
        return d + filename;
    }

    // ---- Inner types ----

    /**
     * A single save condition: trigger BGSAVE if {@code changes} writes happened
     * within the last {@code seconds} seconds.
     */
    public static final class SaveCondition {
        public final int seconds;
        public final int changes;

        public SaveCondition(int seconds, int changes) {
            this.seconds = seconds;
            this.changes = changes;
        }

        @Override
        public String toString() {
            return "save " + seconds + " " + changes;
        }
    }
}
