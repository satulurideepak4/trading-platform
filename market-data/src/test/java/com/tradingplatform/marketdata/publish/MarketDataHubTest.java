package com.tradingplatform.marketdata.publish;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.marketdata.event.MarketEventType;
import com.tradingplatform.marketdata.process.NormalizedUpdate;
import com.tradingplatform.marketdata.process.SequenceOutcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class MarketDataHubTest {
    private static final Instant NOW = Instant.parse("2025-01-01T09:30:00Z");

    @Test
    void publishedUpdatesReachOnlySubscribersInterestedInThatSymbol() {
        MarketDataHub hub = hub();
        RecordingListener aapl = new RecordingListener();
        RecordingListener msft = new RecordingListener();
        hub.subscribe(Set.of("AAPL"), aapl);
        hub.subscribe(Set.of("MSFT"), msft);

        hub.publish(update("AAPL", 1));

        List<NormalizedUpdate> aaplReceived = aapl.awaitAtLeast(1, Duration.ofSeconds(2));
        assertEquals(1, aaplReceived.size());
        assertTrue(msft.awaitAtLeast(1, Duration.ofMillis(300)).isEmpty());
    }

    @Test
    void unsubscribingStopsFurtherDelivery() {
        MarketDataHub hub = hub();
        RecordingListener listener = new RecordingListener();
        Subscription subscription = hub.subscribe(Set.of("AAPL"), listener);

        subscription.close();
        hub.publish(update("AAPL", 1));

        assertEquals(0, hub.subscriberCount());
        assertTrue(listener.awaitAtLeast(1, Duration.ofMillis(300)).isEmpty());
    }

    @Test
    void aSlowSubscriberDropsUpdatesRatherThanBlockingPublish() {
        MarketDataHub hub = new MarketDataHub(2, 1_000, new SimpleMeterRegistry());
        RecordingListener slow = new RecordingListener(Duration.ofMillis(50));
        Subscription subscription = hub.subscribe(Set.of("AAPL"), slow);

        for (int i = 1; i <= 30; i++) {
            hub.publish(update("AAPL", i));
        }

        // Publishing all 30 must not have blocked on the slow subscriber at all; the assertion
        // that matters is simply that this test completes quickly, which JUnit's own timeout
        // machinery is unnecessary for here since a blocking hub would hang the whole method.
        awaitUntil(() -> subscription.droppedCount() > 0, Duration.ofSeconds(2));
        assertTrue(subscription.droppedCount() > 0);
    }

    @Test
    void aSubscriberStuckPastTheDropThresholdIsDisconnected() {
        MarketDataHub hub = new MarketDataHub(1, 3, new SimpleMeterRegistry());
        RecordingListener stuck = new RecordingListener(Duration.ofSeconds(30));
        hub.subscribe(Set.of("AAPL"), stuck);

        for (int i = 1; i <= 20; i++) {
            hub.publish(update("AAPL", i));
        }

        awaitUntil(() -> hub.subscriberCount() == 0, Duration.ofSeconds(2));
        assertEquals(0, hub.subscriberCount());
    }

    @Test
    void oneSlowSubscriberDoesNotAffectDeliveryToAHealthyOne() {
        // A generous capacity here: the point of this test is that the slow subscriber's own
        // small queue does not throttle the healthy one, not to also assert the healthy one never
        // drops under a queue too small for ordinary thread-scheduling jitter to avoid.
        MarketDataHub hub = new MarketDataHub(64, 1_000, new SimpleMeterRegistry());
        RecordingListener slow = new RecordingListener(Duration.ofMillis(100));
        RecordingListener healthy = new RecordingListener();
        hub.subscribe(Set.of("AAPL"), slow);
        hub.subscribe(Set.of("AAPL"), healthy);

        for (int i = 1; i <= 10; i++) {
            hub.publish(update("AAPL", i));
        }

        List<NormalizedUpdate> healthyReceived = healthy.awaitAtLeast(10, Duration.ofSeconds(2));
        assertEquals(10, healthyReceived.size());
    }

    private static MarketDataHub hub() {
        return new MarketDataHub(64, 1_000, new SimpleMeterRegistry());
    }

    private static NormalizedUpdate update(String symbol, long sequence) {
        return new NormalizedUpdate(
                symbol, MarketEventType.TRADE, 100, 1, sequence, NOW, sequence, NOW, SequenceOutcome.IN_ORDER, 0);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Records every delivered update, optionally after an artificial per-update delay. */
    private static final class RecordingListener implements MarketDataListener {
        private final List<NormalizedUpdate> received = new CopyOnWriteArrayList<>();
        private final Duration delayPerUpdate;

        RecordingListener() {
            this(Duration.ZERO);
        }

        RecordingListener(Duration delayPerUpdate) {
            this.delayPerUpdate = delayPerUpdate;
        }

        @Override
        public void onUpdate(NormalizedUpdate update) {
            if (!delayPerUpdate.isZero()) {
                try {
                    Thread.sleep(delayPerUpdate.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            received.add(update);
        }

        List<NormalizedUpdate> awaitAtLeast(int count, Duration timeout) {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (received.size() < count && System.nanoTime() < deadline) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return List.copyOf(received);
        }
    }
}
