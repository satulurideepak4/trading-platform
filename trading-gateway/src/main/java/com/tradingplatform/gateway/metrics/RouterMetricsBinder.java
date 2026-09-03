package com.tradingplatform.gateway.metrics;

import com.tradingplatform.matching.routing.OrderRouter;
import com.tradingplatform.matching.routing.WorkerMetricsSnapshot;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

/**
 * Exposes each matching worker's own bookkeeping — queue depth, admitted/processed/failed/
 * saturated command counts — as Micrometer meters tagged by worker. This is what answers "is one
 * symbol/partition overloaded": symbol-to-worker is a fixed hash (see {@code OrderRouter.
 * workerIndex}), so a hot worker in these numbers is a hot symbol, or several sharing it.
 *
 * <p>All pull-based: every meter here reads live from {@link OrderRouter#metrics()} at the moment
 * Micrometer's registry is asked for it (a Prometheus scrape, typically), so nothing polls on a
 * schedule and nothing can drift stale between scrapes. {@code queueDepth} is a {@link Gauge} — it
 * goes up and down. The four command counts are {@link FunctionCounter}s, not gauges, because they
 * are monotonic values this class does not own and never resets; that is exactly what
 * {@code FunctionCounter} means and a plain gauge does not.
 */
public final class RouterMetricsBinder {

    private RouterMetricsBinder() {}

    public static void bind(OrderRouter router, MeterRegistry meters) {
        for (int i = 0; i < router.workerCount(); i++) {
            int workerId = i; // a final capture for the lambdas below; the loop variable itself isn't one
            Tags tags = Tags.of("worker", String.valueOf(workerId));

            Gauge.builder("trading.matching.queue.depth", router, r -> snapshot(r, workerId).queueDepth())
                    .description("Commands currently waiting in this worker's admission queue")
                    .tags(tags)
                    .register(meters);
            Gauge.builder(
                            "trading.matching.queue.capacity",
                            router,
                            r -> snapshot(r, workerId).queueCapacity())
                    .description("This worker's admission queue capacity")
                    .tags(tags)
                    .register(meters);
            FunctionCounter.builder(
                            "trading.matching.admitted",
                            router,
                            r -> snapshot(r, workerId).admittedCommands())
                    .description("Commands this worker has accepted into its queue")
                    .tags(tags)
                    .register(meters);
            FunctionCounter.builder(
                            "trading.matching.processed",
                            router,
                            r -> snapshot(r, workerId).processedCommands())
                    .description("Commands this worker has finished applying")
                    .tags(tags)
                    .register(meters);
            FunctionCounter.builder(
                            "trading.matching.failed",
                            router,
                            r -> snapshot(r, workerId).failedCommands())
                    .description("Commands this worker's engine threw on rather than completed")
                    .tags(tags)
                    .register(meters);
            FunctionCounter.builder(
                            "trading.matching.saturated",
                            router,
                            r -> snapshot(r, workerId).saturatedCommands())
                    .description("Commands refused because this worker's queue was full")
                    .tags(tags)
                    .register(meters);
        }
    }

    private static WorkerMetricsSnapshot snapshot(OrderRouter router, int workerId) {
        return router.metrics().workers().get(workerId);
    }
}
