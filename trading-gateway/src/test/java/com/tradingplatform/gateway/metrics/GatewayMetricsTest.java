package com.tradingplatform.gateway.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.RejectionReason;
import com.tradingplatform.domain.Side;
import com.tradingplatform.matching.routing.RoutedResult;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class GatewayMetricsTest {
    private static final Instant NOW = Instant.parse("2025-01-01T09:30:00Z");

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final GatewayMetrics metrics = new GatewayMetrics(registry);

    @Test
    void recordRoutedPublishesMatchingLatencyAndQueueWaitTimersWithPercentiles() {
        RoutedResult<CommandResult> routed = new RoutedResult<>(
                1, 1, 0, CommandResult.rejected(1, RejectionReason.ORDER_NOT_FOUND),
                5_000_000L, 250_000L);

        metrics.recordRouted("submit", routed);

        Timer latency = registry.find("trading.matching.latency").tag("operation", "submit").timer();
        assertEquals(1, latency.count());
        assertEquals(250_000L, latency.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS), 1.0);
        assertPercentilesConfigured(latency);

        Timer wait = registry.find("trading.matching.queue.wait").tag("operation", "submit").timer();
        assertEquals(1, wait.count());
        assertEquals(5_000_000L, wait.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS), 1.0);
        assertPercentilesConfigured(wait);
    }

    @Test
    void recordRoutedReusesTheSameTimerAcrossCalls() {
        RoutedResult<CommandResult> a = routedResult(100, 10);
        RoutedResult<CommandResult> b = routedResult(200, 20);

        metrics.recordRouted("cancel", a);
        metrics.recordRouted("cancel", b);

        Timer latency = registry.find("trading.matching.latency").tag("operation", "cancel").timer();
        assertEquals(2, latency.count());
    }

    @Test
    void acceptedOutcomeIncrementsAcceptedAndExecutionsCounters() {
        OrderSnapshot order = order();
        CommandResult accepted = CommandResult.accepted(order, List.of(execution(), execution()));

        metrics.recordOutcome("submit", "AAPL", accepted);

        assertEquals(
                1.0, registry.find("trading.orders.accepted")
                        .tag("operation", "submit").tag("symbol", "AAPL").counter().count());
        assertEquals(
                2.0, registry.find("trading.executions.recorded").tag("symbol", "AAPL").counter().count());
    }

    @Test
    void rejectedOutcomeIncrementsRejectedCounterTaggedWithStageAndReason() {
        CommandResult rejected = CommandResult.rejected(1, RejectionReason.INVALID_QUANTITY);

        metrics.recordOutcome("submit", "AAPL", rejected);

        assertEquals(
                1.0, registry.find("trading.orders.rejected")
                        .tag("operation", "submit")
                        .tag("stage", "matching")
                        .tag("reason", "INVALID_QUANTITY")
                        .counter()
                        .count());
    }

    @Test
    void acceptedOutcomeWithNoExecutionsDoesNotRegisterAnExecutionsCounter() {
        metrics.recordOutcome("cancel", "AAPL", CommandResult.accepted(order(), List.of()));

        assertEquals(
                Set.of(),
                registry.find("trading.executions.recorded").meters().stream()
                        .map(m -> m.getId().getName())
                        .collect(Collectors.toSet()));
    }

    @Test
    void riskRejectionIsTaggedWithStageRisk() {
        metrics.recordRiskRejection("replace", "MAX_ORDER_NOTIONAL_EXCEEDED");

        assertEquals(
                1.0, registry.find("trading.orders.rejected")
                        .tag("operation", "replace")
                        .tag("stage", "risk")
                        .tag("reason", "MAX_ORDER_NOTIONAL_EXCEEDED")
                        .counter()
                        .count());
    }

    /**
     * A {@code SimpleMeterRegistry} test cannot catch this: Micrometer's Prometheus naming
     * convention silently strips a trailing "created" from a Counter name — it collides with
     * OpenMetrics' own reserved per-counter "_created" timestamp series — so
     * {@code trading.executions.created} would have scraped as {@code trading_executions_total},
     * not {@code trading_executions_created_total}, with no exception anywhere to catch. Only
     * exercising the real Prometheus exposition path catches it. See the comment on
     * {@code GatewayMetrics.recordOutcome} and docs/observability.md.
     */
    @Test
    void executionsMetricSurvivesPrometheusNamingWithoutCollision() {
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        GatewayMetrics prometheusMetrics = new GatewayMetrics(prometheus);

        prometheusMetrics.recordOutcome("submit", "AAPL", CommandResult.accepted(order(), List.of(execution())));

        String scraped = prometheus.scrape();
        assertTrue(
                scraped.contains("trading_executions_recorded_total"),
                "expected trading_executions_recorded_total in:\n" + scraped);
    }

    private static void assertPercentilesConfigured(Timer timer) {
        HistogramSnapshot snapshot = timer.takeSnapshot();
        assertTrue(snapshot.percentileValues().length >= 4, "expected p50/p95/p99/p99.9 to be configured");
    }

    private static RoutedResult<CommandResult> routedResult(long queueWaitNanos, long processingNanos) {
        return new RoutedResult<>(
                1, 1, 0, CommandResult.rejected(1, RejectionReason.ORDER_NOT_FOUND),
                queueWaitNanos, processingNanos);
    }

    private static OrderSnapshot order() {
        return new OrderSnapshot(
                1, "client-1", "AAPL", Side.BUY, OrderType.LIMIT, OrderStatus.NEW,
                10, 10, 19_000, NOW, NOW, 1);
    }

    private static Execution execution() {
        return new Execution(1, "AAPL", 19_000, 5, 1, "buy-client", 2, "sell-client", 1, 2, 5, 0, NOW);
    }
}
