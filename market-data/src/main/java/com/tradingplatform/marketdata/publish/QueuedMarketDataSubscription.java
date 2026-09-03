package com.tradingplatform.marketdata.publish;

import com.tradingplatform.marketdata.process.NormalizedUpdate;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One subscriber's bounded buffer and drain thread.
 *
 * <p>Ingestion offers into this subscriber's queue and never waits on it: a full queue means this
 * one subscriber cannot keep up, and the fix is to drop this subscriber's oldest queued update, not
 * to slow down every other subscriber and the feed itself. That is the whole point of a queue per
 * subscriber rather than one shared queue with synchronous fan-out — see ADR-013.
 *
 * <p>Dropping is bounded too: a subscriber stuck for {@link #maxConsecutiveDrops} updates in a row
 * is not merely bursty, it is gone or broken, and is disconnected so its queue and thread stop
 * costing anything.
 */
final class QueuedMarketDataSubscription implements Subscription {
    private static final Logger LOG = LoggerFactory.getLogger(QueuedMarketDataSubscription.class);

    private final long id;
    private final MarketDataListener listener;
    private final BlockingQueue<NormalizedUpdate> queue;
    private final int maxConsecutiveDrops;
    private final LongConsumer onClosed;
    private final Thread drainThread;

    private volatile Set<String> symbols;
    private volatile boolean stopping;
    private final AtomicInteger consecutiveDrops = new AtomicInteger();
    private final AtomicLong dropped = new AtomicLong();

    QueuedMarketDataSubscription(
            long id,
            Set<String> symbols,
            MarketDataListener listener,
            int queueCapacity,
            int maxConsecutiveDrops,
            LongConsumer onClosed) {
        this.id = id;
        this.symbols = Set.copyOf(symbols);
        this.listener = listener;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxConsecutiveDrops = maxConsecutiveDrops;
        this.onClosed = onClosed;
        this.drainThread =
                Thread.ofPlatform().name("marketdata-subscriber-" + id).daemon(true).unstarted(this::drainLoop);
    }

    void start() {
        drainThread.start();
    }

    boolean interestedIn(String symbol) {
        return symbols.contains(symbol);
    }

    /**
     * Non-blocking. On overflow, evicts the oldest queued update to make room for the newest one:
     * for a live feed, the newest state is worth more than the one it would otherwise displace.
     */
    void offer(NormalizedUpdate update) {
        if (queue.offer(update)) {
            consecutiveDrops.set(0);
            return;
        }
        queue.poll();
        if (queue.offer(update)) {
            dropped.incrementAndGet();
            if (consecutiveDrops.incrementAndGet() >= maxConsecutiveDrops) {
                LOG.warn(
                        "event=marketdata_subscriber_disconnected subscriptionId={} reason=sustained_overflow",
                        id);
                close("sustained overflow");
            }
        }
        // If the second offer also fails, another thread raced this eviction; the next update will
        // retry. Losing one update to that race is cheaper than blocking to synchronize it away.
    }

    @Override
    public void updateSymbols(Set<String> symbols) {
        this.symbols = Set.copyOf(symbols);
    }

    @Override
    public long droppedCount() {
        return dropped.get();
    }

    @Override
    public void close() {
        close("closed by caller");
    }

    private void close(String reason) {
        if (stopping) {
            return;
        }
        stopping = true;
        onClosed.accept(id);
        drainThread.interrupt();
    }

    private void drainLoop() {
        try {
            while (!stopping) {
                NormalizedUpdate update = queue.poll(200, TimeUnit.MILLISECONDS);
                if (update != null) {
                    deliver(update);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            listener.onDisconnected("subscription closed");
        }
    }

    private void deliver(NormalizedUpdate update) {
        try {
            listener.onUpdate(update);
        } catch (RuntimeException failure) {
            // A listener failure (a broken socket, say) must not kill this drain thread before it
            // has a chance to notice the subscription closing and unregister cleanly.
            LOG.debug("event=marketdata_listener_failed subscriptionId={}", id, failure);
            close("listener failed");
        }
    }
}
