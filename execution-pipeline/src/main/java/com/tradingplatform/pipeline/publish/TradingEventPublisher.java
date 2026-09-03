package com.tradingplatform.pipeline.publish;

import com.tradingplatform.pipeline.events.TradingEvent;

/**
 * Accepts events from the matching path.
 *
 * <p>The contract that matters: implementations must not block the caller. Events are handed over
 * from a matching worker thread, and a matching worker that waits on a broker has stopped matching
 * every symbol it owns, not just the one that produced the event.
 */
public interface TradingEventPublisher {

    void publish(TradingEvent event);
}
