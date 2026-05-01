package com.redisimpl.server.ae;

/**
 * Callback for time events.
 */
@FunctionalInterface
public interface AeTimeProc {
    /**
     * Called when the time event fires.
     *
     * @param id         the event ID
     * @param clientData user-supplied data
     * @return milliseconds until next firing, or {@link AeEventLoop#AE_NOMORE} to cancel
     */
    long process(long id, Object clientData);
}
