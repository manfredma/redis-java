package com.redisimpl.server.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Command dispatch table.
 *
 * <p>Scans registered handler objects for methods annotated with {@link RedisCommand}
 * and builds a lookup map from command name → {@link CommandEntry}.
 */
public final class CommandTable {

    private static final Logger log = LoggerFactory.getLogger(CommandTable.class);

    private final Map<String, CommandEntry> commands = new HashMap<>();

    /**
     * Register all {@link RedisCommand}-annotated methods in the given handler object.
     */
    public void register(Object handler) {
        Class<?> clazz = handler.getClass();
        for (Method method : clazz.getMethods()) {
            RedisCommand annotation = method.getAnnotation(RedisCommand.class);
            if (annotation == null) continue;

            // Validate method signature: (RedisClient, byte[][]) -> byte[]
            Class<?>[] params = method.getParameterTypes();
            if (params.length != 2
                    || !params[0].isAssignableFrom(com.redisimpl.server.client.RedisClient.class)
                    || !params[1].equals(byte[][].class)
                    || !method.getReturnType().equals(byte[].class)) {
                log.warn("Skipping @RedisCommand method {} — wrong signature", method.getName());
                continue;
            }

            String name = annotation.name().toLowerCase();
            CommandEntry entry = new CommandEntry(annotation, handler, method);
            commands.put(name, entry);
            log.debug("Registered command: {}", name);
        }
    }

    /**
     * Look up a command by name (case-insensitive).
     * Returns null if not found.
     */
    public CommandEntry lookup(String name) {
        if (name == null) return null;
        return commands.get(name.toLowerCase());
    }

    /**
     * Number of registered commands.
     */
    public int commandCount() {
        return commands.size();
    }

    /**
     * All registered command entries.
     */
    public Collection<CommandEntry> getAll() {
        return Collections.unmodifiableCollection(commands.values());
    }

    /**
     * Check if a command exists.
     */
    public boolean exists(String name) {
        return lookup(name) != null;
    }
}
