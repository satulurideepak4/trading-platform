package com.tradingplatform.gateway.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.routing.OrderRouter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RouterMetricsBinderTest {
    private static final Instant NOW = Instant.parse("2025-01-01T09:30:00Z");

    @Test
    void bindsOneSetOfMetersPerWorkerReadingLiveFromTheRouter() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try (OrderRouter router = new OrderRouter(2, 8)) {
            RouterMetricsBinder.bind(router, registry);

            for (int workerId = 0; workerId < 2; workerId++) {
                assertEquals(
                        8.0,
                        registry.find("trading.matching.queue.capacity")
                                .tag("worker", String.valueOf(workerId))
                                .gauge()
                                .value());
                assertEquals(
                        0.0,
                        registry.find("trading.matching.admitted")
                                .tag("worker", String.valueOf(workerId))
                                .functionCounter()
                                .count());
            }

            int workerId = router.workerIndex("AAPL");
            router.submit(SubmitOrder.limit(1, "c-1", "AAPL", Side.BUY, 10, 19_000, NOW))
                    .get(5, TimeUnit.SECONDS);

            assertEquals(
                    1.0,
                    registry.find("trading.matching.admitted")
                            .tag("worker", String.valueOf(workerId))
                            .functionCounter()
                            .count());
            assertEquals(
                    1.0,
                    registry.find("trading.matching.processed")
                            .tag("worker", String.valueOf(workerId))
                            .functionCounter()
                            .count());
            // Drained back to zero once the single command has actually been applied - proves
            // queue.depth reads live state rather than a value captured at bind time.
            assertEquals(
                    0.0,
                    registry.find("trading.matching.queue.depth")
                            .tag("worker", String.valueOf(workerId))
                            .gauge()
                            .value());
        }
    }
}
