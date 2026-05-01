package com.redisimpl.server.command;

import com.redisimpl.server.client.RedisClient;
import lombok.Getter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A registered command entry in the command table.
 */
@Getter
public final class CommandEntry {

    private final String name;
    private final int arity;
    private final Set<String> flags;
    private final int firstKey;
    private final int lastKey;
    private final int step;
    private final Object handler;
    private final Method method;

    CommandEntry(RedisCommand annotation, Object handler, Method method) {
        this.name = annotation.name().toLowerCase();
        this.arity = annotation.arity();
        this.flags = new HashSet<>(Arrays.asList(annotation.flags().split("\\s+")));
        this.firstKey = annotation.firstKey();
        this.lastKey = annotation.lastKey();
        this.step = annotation.step();
        this.handler = handler;
        this.method = method;
    }

    /**
     * Execute this command.
     *
     * @param client the client
     * @param argv   the argument vector (argv[0] is the command name)
     * @return the response bytes
     */
    public byte[] execute(RedisClient client, byte[][] argv) {
        try {
            return (byte[]) method.invoke(handler, client, argv);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RedisException) {
                return com.redisimpl.server.resp.RespEncoder.encodeError(cause.getMessage());
            }
            return com.redisimpl.server.resp.RespEncoder.encodeError("ERR internal error: " + cause.getMessage());
        }
    }

    /**
     * Check if the argument count is valid.
     */
    public boolean isArityValid(int argc) {
        if (arity > 0) return argc == arity;
        return argc >= -arity;
    }

    public boolean hasFlag(String flag) {
        return flags.contains(flag);
    }

    @Override
    public String toString() {
        return "CommandEntry{name=" + name + ", arity=" + arity + ", flags=" + flags + "}";
    }
}
