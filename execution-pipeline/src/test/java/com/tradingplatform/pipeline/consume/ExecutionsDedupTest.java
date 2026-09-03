package com.tradingplatform.pipeline.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.store.ExecutionStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Fast, unit-level proof of "each execution affects positions exactly once" for the consumer side
 * of the pipeline (see {@link ExecutionStore}'s Javadoc: idempotent by construction, execution id
 * as primary key). This is deliberately narrower and faster than
 * {@code ExecutionPipelineIntegrationTest.redeliveringTheSameExecutionDoesNotMoveThePositionTwice}
 * in {@code trading-gateway}, which proves the same property through a real broker and database —
 * this test isolates the dedup logic itself so it runs in milliseconds.
 */
class ExecutionsDedupTest {

    @Test
    void recordingTheSameExecutionTwiceIsANoOpTheSecondTime() {
        ExecutionStore store = new ExecutionStore();
        ExecutionProcessor processor = new ExecutionProcessor(store, new SimpleMeterRegistry());
        ExecutionCreated execution = execution(1);

        processor.onEvent(execution);
        processor.onEvent(execution);

        assertEquals(1, store.size());
        assertEquals(execution, store.find(1).orElseThrow());
    }

    @Test
    void concurrentRedeliveryOfTheSameExecutionAppliesExactlyOnce() throws InterruptedException {
        ExecutionStore store = new ExecutionStore();
        ExecutionProcessor processor = new ExecutionProcessor(store, new SimpleMeterRegistry());
        ExecutionCreated execution = execution(1);

        int racers = 8;
        CountDownLatch ready = new CountDownLatch(racers);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        try {
            for (int i = 0; i < racers; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    await(go);
                    processor.onEvent(execution);
                });
            }
            ready.await();
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, store.size());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    private static ExecutionCreated execution(long executionId) {
        return new ExecutionCreated(
                "exe-" + executionId,
                "corr-" + executionId,
                executionId,
                "AAPL",
                10_000,
                10,
                1,
                "acc-buy",
                "strat-buy",
                2,
                "acc-sell",
                "strat-sell",
                1,
                2,
                0,
                0,
                Instant.parse("2026-08-22T09:30:00Z"));
    }
}
