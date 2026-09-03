package com.tradingplatform.simulator;

import com.tradingplatform.marketdata.process.MarketDataMetrics;
import com.tradingplatform.marketdata.process.MarketDataProcessor;
import com.tradingplatform.marketdata.simulator.FeedImperfectionPolicy;
import com.tradingplatform.marketdata.simulator.MarketSimulator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Locale;

/**
 * Command-line smoke test for the market-data engineering built in Stage 6: runs {@link
 * MarketSimulator} straight into a {@link MarketDataProcessor} and prints what came out, including
 * how many duplicate/out-of-order/gap events the deliberately imperfect feed produced and the
 * processor caught. Nothing here is a controlled benchmark — see {@code OrderFlowSimulator} for the
 * same disclaimer applied to order flow.
 */
public final class MarketDataFeedSimulator {
    private static final long DEFAULT_EVENT_COUNT = 20_000;
    private static final long DEFAULT_SEED = 7_921L;
    private static final int EVENTS_PER_SECOND = 20_000;
    private static final List<String> SYMBOLS = List.of("AAPL", "MSFT", "NVDA", "EURUSD");

    private MarketDataFeedSimulator() {}

    public static void main(String[] args) {
        long eventCount = parseEventCount(args);
        long seed = args.length > 1 ? Long.parseLong(args[1]) : DEFAULT_SEED;

        MarketDataMetrics metrics = new MarketDataMetrics(new SimpleMeterRegistry());
        MarketDataProcessor processor = new MarketDataProcessor(update -> {}, metrics);
        MarketSimulator simulator = new MarketSimulator(
                SYMBOLS, EVENTS_PER_SECOND, seed, FeedImperfectionPolicy.DEFAULT, processor);

        long startedAt = System.nanoTime();
        simulator.runFor(eventCount);
        double elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

        System.out.printf(
                Locale.ROOT,
                "event=simulation_complete events=%d received=%d invalid=%d duplicates=%d "
                        + "outOfOrder=%d sequenceNumbersSkipped=%d symbols=%d seed=%d elapsedMs=%.3f%n",
                eventCount,
                metrics.receivedCount(),
                metrics.invalidCount(),
                metrics.duplicateCount(),
                metrics.outOfOrderCount(),
                metrics.gapCount(),
                processor.symbols().size(),
                seed,
                elapsedMillis);
        System.out.println(
                "note=This is a functional workload smoke test, not a controlled benchmark result.");
    }

    private static long parseEventCount(String[] args) {
        if (args.length == 0) {
            return DEFAULT_EVENT_COUNT;
        }
        long count = Long.parseLong(args[0]);
        if (count <= 0) {
            throw new IllegalArgumentException("event count must be positive");
        }
        return count;
    }
}
