package com.redisimpl.server.bio;

import lombok.Getter;

/**
 * A job submitted to a BIO worker thread.
 */
@Getter
public final class BioJob {

    public enum Type {
        /** Close a file descriptor */
        CLOSE_FILE,
        /** AOF fsync */
        AOF_FSYNC,
        /** Lazy free (deferred memory release) */
        LAZY_FREE
    }

    private final Type type;
    private final Runnable task;

    public BioJob(Type type, Runnable task) {
        this.type = type;
        this.task = task;
    }
}
