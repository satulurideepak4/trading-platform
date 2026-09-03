package com.tradingplatform.marketdata.publish;

import com.tradingplatform.marketdata.process.NormalizedUpdate;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fans normalized updates out to every interested subscriber.
 *
 * <p>{@link #publish} never blocks on a subscriber: it hands each interested one a non-blocking
 * offer into that subscriber's own queue and moves on. A subscriber that cannot keep up only ever
 * costs its own updates, never another subscriber's and never the feed. See
 * {@link QueuedMarketDataSubscription} and ADR-013.
 *
 * <p>Deliberately knows nothing about {@code MarketDataProcessor} or current symbol state — it is
 * pure fan-out. A caller that wants a subscriber to see the current state immediately on
 * subscribing (the gateway's WebSocket handler does) reads it from the processor and delivers it
 * itself; see {@code SnapshotUpdates} for the conversion. That keeps this class's only dependency
 * the event type it moves, not the component that produces it.
 */
public final class MarketDataHub implements MarketDataSink {
    private final ConcurrentHashMap<Long, QueuedMarketDataSubscription> subscriptions =
            new ConcurrentHashMap<>();
    private final AtomicLong nextSubscriptionId = new AtomicLong();
    private final int subscriberQueueCapacity;
    private final int maxConsecutiveDrops;

    public MarketDataHub(int subscriberQueueCapacity, int maxConsecutiveDrops, MeterRegistry meters) {
        this.subscriberQueueCapacity = subscriberQueueCapacity;
        this.maxConsecutiveDrops = maxConsecutiveDrops;
        Gauge.builder("trading.marketdata.subscribers", subscriptions, ConcurrentHashMap::size)
                .description("Active market-data subscriptions")
                .register(meters);
    }

    public Subscription subscribe(Set<String> symbols, MarketDataListener listener) {
        long id = nextSubscriptionId.incrementAndGet();
        QueuedMarketDataSubscription subscription = new QueuedMarketDataSubscription(
                id, symbols, listener, subscriberQueueCapacity, maxConsecutiveDrops, subscriptions::remove);
        subscriptions.put(id, subscription);
        subscription.start();
        return subscription;
    }

    @Override
    public void publish(NormalizedUpdate update) {
        subscriptions.values().stream()
                .filter(subscription -> subscription.interestedIn(update.symbol()))
                .forEach(subscription -> subscription.offer(update));
    }

    public int subscriberCount() {
        return subscriptions.size();
    }
}
