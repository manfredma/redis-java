package com.redisimpl.server.replication;

/**
 * Callback interface used by RedisServer to propagate write commands to replicas.
 * Implemented by ReplicationManager in the redis-replication module.
 */
public interface ReplicationPropagator {
    void propagate(byte[][] argv);
}
