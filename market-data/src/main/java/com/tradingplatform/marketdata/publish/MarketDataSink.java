package com.tradingplatform.marketdata.publish;

import com.tradingplatform.marketdata.process.NormalizedUpdate;

/**
 * Where {@code MarketDataProcessor} sends what it produces. Kept as an interface, implemented by
 * {@link MarketDataHub}, so the processor never depends on how fan-out or backpressure work — the
 * same separation {@code TradingEventPublisher} draws between the matching path and Kafka.
 */
public interface MarketDataSink {
    void publish(NormalizedUpdate update);
}
