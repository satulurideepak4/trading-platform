package com.tradingplatform.pipeline.publish;

import com.tradingplatform.pipeline.events.TradingEvent;

/**
 * Used when the pipeline is switched off, which is how the matching and gateway tests run without
 * a broker. Trading still works; nothing downstream observes it.
 */
public final class NoOpTradingEventPublisher implements TradingEventPublisher {

    @Override
    public void publish(TradingEvent event) {
        // Intentionally does nothing: with the pipeline disabled there is no downstream to tell.
    }
}
