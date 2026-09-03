package com.tradingplatform.pipeline.consume;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * The three numbers every consumer in this pipeline needs: how long processing takes, how much of
 * the traffic was redelivery, and how much was refused outright.
 *
 * <p>Duplicates are counted rather than ignored silently. A consumer that suddenly starts seeing
 * duplicates is usually a consumer that is being rebalanced or is failing to commit offsets, and
 * that is worth an alert even though correctness is unaffected.
 */
public final class ProcessingMetrics {
    private final Timer processing;
    private final Counter applied;
    private final Counter duplicates;
    private final Counter invalid;

    public ProcessingMetrics(MeterRegistry meters, String consumer) {
        this.processing = Timer.builder("trading.consumer.processing")
                .tag("consumer", consumer)
                .description("Time to apply one event")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .register(meters);
        this.applied = Counter.builder("trading.consumer.applied")
                .tag("consumer", consumer)
                .description("Events that changed state")
                .register(meters);
        this.duplicates = Counter.builder("trading.consumer.duplicates")
                .tag("consumer", consumer)
                .description("Redelivered events that had already been applied")
                .register(meters);
        this.invalid = Counter.builder("trading.consumer.invalid")
                .tag("consumer", consumer)
                .description("Events refused as unusable")
                .register(meters);
    }

    /**
     * @param action returns true when it changed state, false when the event had already been
     *     applied
     */
    public void time(java.util.function.BooleanSupplier action) {
        Timer.Sample sample = Timer.start();
        try {
            if (action.getAsBoolean()) {
                applied.increment();
            } else {
                duplicates.increment();
            }
        } finally {
            sample.stop(processing);
        }
    }

    public void recordInvalid() {
        invalid.increment();
    }
}
