package com.tradingplatform.marketdata.process;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Feed-quality counters. Each one answers a question an operator would otherwise have to infer
 * from logs: is the upstream feed healthy, and is this processor keeping up with it.
 */
public final class MarketDataMetrics {
    private final Counter received;
    private final Counter invalid;
    private final Counter duplicate;
    private final Counter outOfOrder;
    private final Counter gap;

    public MarketDataMetrics(MeterRegistry meters) {
        this.received = Counter.builder("trading.marketdata.received")
                .description("Raw market events ingested")
                .register(meters);
        this.invalid = Counter.builder("trading.marketdata.invalid")
                .description("Raw market events rejected by validation")
                .register(meters);
        this.duplicate = Counter.builder("trading.marketdata.duplicate")
                .description("Events at or before the last applied sequence for their symbol")
                .register(meters);
        this.outOfOrder = Counter.builder("trading.marketdata.out_of_order")
                .description("Events that arrived after a later sequence had already been applied")
                .register(meters);
        this.gap = Counter.builder("trading.marketdata.gap")
                .description("Sequence numbers skipped, summed across every gap detected")
                .register(meters);
    }

    public void recordReceived() {
        received.increment();
    }

    public void recordInvalid() {
        invalid.increment();
    }

    public void record(SequenceOutcome outcome, int gapSize) {
        switch (outcome) {
            case DUPLICATE -> duplicate.increment();
            case OUT_OF_ORDER -> outOfOrder.increment();
            case GAP -> gap.increment(gapSize);
            case IN_ORDER, SNAPSHOT -> {
                // Nothing distinguishing to count; "received" already covers the common case.
            }
        }
    }

    // Read accessors below exist for the CLI smoke-test summary in MarketDataFeedSimulator, which
    // has no metrics backend of its own to scrape. Not used by anything on the live serving path.

    public long receivedCount() {
        return (long) received.count();
    }

    public long invalidCount() {
        return (long) invalid.count();
    }

    public long duplicateCount() {
        return (long) duplicate.count();
    }

    public long outOfOrderCount() {
        return (long) outOfOrder.count();
    }

    public long gapCount() {
        return (long) gap.count();
    }
}
