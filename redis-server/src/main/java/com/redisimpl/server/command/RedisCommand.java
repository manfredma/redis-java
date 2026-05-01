package com.redisimpl.server.command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a Redis command handler.
 *
 * <p>Methods annotated with {@code @RedisCommand} are discovered by {@link CommandTable}
 * during registration and added to the command dispatch table.
 *
 * <p>Example:
 * <pre>
 * {@literal @}RedisCommand(name = "get", arity = 2, flags = "read-only fast", firstKey = 1, lastKey = 1, step = 1)
 * public byte[] get(RedisClient client, byte[][] argv) { ... }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RedisCommand {

    /**
     * Command name (lowercase).
     */
    String name();

    /**
     * Arity:
     * <ul>
     *   <li>Positive: exact number of arguments (including command name)</li>
     *   <li>Negative: minimum number of arguments (including command name), e.g., -2 means >= 2</li>
     * </ul>
     */
    int arity();

    /**
     * Space-separated flags, e.g., "write denyoom", "read-only fast".
     * Common flags: write, read-only, denyoom, admin, pubsub, noscript, random, sort_for_script,
     *               loading, stale, skip_monitor, asking, fast, no-auth, may-replicate
     */
    String flags();

    /**
     * Index of the first key in argv (1-based). 0 means no keys.
     */
    int firstKey();

    /**
     * Index of the last key in argv (1-based). 0 means no keys.
     */
    int lastKey();

    /**
     * Step between keys (for commands like MSET where keys are at positions 1, 3, 5, ...).
     */
    int step();
}
