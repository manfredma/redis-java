package com.redisimpl.server.cluster;

/**
 * Hook called before each command to check if this node owns the key's slot.
 * Returns null if the command can proceed, or a MOVED/ASK error bytes if not.
 */
@FunctionalInterface
public interface ClusterSlotChecker {
    /** @return null to proceed, or error bytes to return to client */
    byte[] check(byte[][] argv);
}
