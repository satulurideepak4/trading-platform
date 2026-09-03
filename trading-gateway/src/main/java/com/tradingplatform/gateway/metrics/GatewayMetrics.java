package com.tradingplatform.gateway.metrics;

import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.matching.routing.RoutedResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Bridges what the matching layer already computes — {@link RoutedResult}'s timings, {@link
 * CommandResult}'s outcome — into Micrometer, from the one place in {@code trading-gateway} that
 * sees every command's result: {@code OrderIngressService}.
 *
 * <p>Deliberately not inside {@code matching-engine} itself. That module stays free of Spring and
 * Micrometer the same way it stays free of Kafka and Postgres — a Stage 1 invariant Stage 7's
 * journal preserved and this class preserves too. {@link Timer#record(long, TimeUnit)} accepts an
 * already-measured duration, which is what makes recording real percentile histograms possible from
 * out here rather than needing to wrap the matching call itself.
 *
 * <p>Timers are cached per operation tag rather than rebuilt per call: {@code Timer.builder(...)
 * .register(...)} is idempotent for an identical id, but doing that lookup on every single order is
 * needless work on a path this class exists specifically not to slow down.
 */
public final class GatewayMetrics {
    private final MeterRegistry meters;
    private final Map<String, Timer> matchingLatency = new ConcurrentHashMap<>();
    private final Map<String, Timer> queueWait = new ConcurrentHashMap<>();

    public GatewayMetrics(MeterRegistry meters) {
        this.meters = meters;
    }

    /** How long a command spent queued, and how long the worker took to apply it, by operation. */
    public void recordRouted(String operation, RoutedResult<?> routed) {
        timerFor(matchingLatency, "trading.matching.latency",
                "Time a matching worker spent applying one command, from dequeue to result", operation)
                .record(routed.processingNanos(), TimeUnit.NANOSECONDS);
        timerFor(queueWait, "trading.matching.queue.wait",
                "Time a command spent in a worker's admission queue before being processed", operation)
                .record(routed.queueWaitNanos(), TimeUnit.NANOSECONDS);
    }

    /** Accepted vs. rejected by the matching engine, and executions produced, by operation/symbol. */
    public void recordOutcome(String operation, String symbol, CommandResult result) {
        if (result.accepted()) {
            meters.counter("trading.orders.accepted", "operation", operation, "symbol", symbol)
                    .increment();
            int executionCount = result.executions().size();
            if (executionCount > 0) {
                // Not "trading.executions.created": Micrometer's Prometheus naming convention
                // treats a Counter name ending in "created" as colliding with OpenMetrics' own
                // reserved per-counter "_created" timestamp series and silently strips the word,
                // exposing this as trading_executions_total instead of
                // trading_executions_created_total. Found by scraping the real endpoint, not by
                // reading Micrometer's source — see docs/observability.md.
                meters.counter("trading.executions.recorded", "symbol", symbol)
                        .increment(executionCount);
            }
            return;
        }
        String reason = result.rejectionReason().map(Enum::name).orElse("UNKNOWN");
        meters.counter(
                        "trading.orders.rejected",
                        "operation", operation,
                        "stage", "matching",
                        "reason", reason)
                .increment();
    }

    /** A rejection that never reached the matching engine at all — refused by pre-trade risk. */
    public void recordRiskRejection(String operation, String reason) {
        meters.counter(
                        "trading.orders.rejected",
                        "operation", operation,
                        "stage", "risk",
                        "reason", reason)
                .increment();
    }

    private Timer timerFor(Map<String, Timer> cache, String name, String description, String operation) {
        return cache.computeIfAbsent(operation, op -> Timer.builder(name)
                .description(description)
                .tag("operation", op)
                // p50/p95/p99/p99.9, the same set every latency-sensitive timer in this codebase
                // publishes (ProcessingMetrics, BufferedKafkaEventPublisher) — one consistent shape
                // for every dashboard and alert that reads a trading.* latency series.
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .register(meters));
    }
}
