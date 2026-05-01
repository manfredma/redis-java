package com.redisimpl.server.ae;

import java.nio.channels.SelectableChannel;

/**
 * Callback for file (I/O) events.
 */
@FunctionalInterface
public interface AeFileProc {
    /**
     * Called when the file event fires.
     *
     * @param loop      the event loop
     * @param channel   the channel that became ready
     * @param mask      the event mask (AE_READABLE | AE_WRITABLE)
     * @param clientData user-supplied data
     */
    void process(AeEventLoop loop, SelectableChannel channel, int mask, Object clientData);
}
