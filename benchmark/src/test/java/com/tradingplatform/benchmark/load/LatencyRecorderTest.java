package com.tradingplatform.benchmark.load;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LatencyRecorderTest {

    @Test
    void computesNearestRankPercentilesForOneToOneHundred() {
        // A known distribution with an exact, hand-checkable nearest-rank answer: for n=100,
        // rank(p) = ceil(p*100), so p50 -> value 50, p95 -> value 95, p99 -> value 99.
        LatencyRecorder recorder = new LatencyRecorder(100);
        for (long value = 1; value <= 100; value++) {
            recorder.record(value);
        }

        LatencyRecorder.Percentiles percentiles = recorder.percentiles();

        assertEquals(50, percentiles.p50Nanos());
        assertEquals(95, percentiles.p95Nanos());
        assertEquals(99, percentiles.p99Nanos());
        assertEquals(100, percentiles.p999Nanos());
        assertEquals(100, percentiles.maxNanos());
        assertEquals(100, percentiles.sampleCount());
    }

    @Test
    void isInsensitiveToRecordingOrder() {
        LatencyRecorder recorder = new LatencyRecorder(5);
        recorder.record(50);
        recorder.record(10);
        recorder.record(30);
        recorder.record(20);
        recorder.record(40);

        LatencyRecorder.Percentiles percentiles = recorder.percentiles();

        assertEquals(30, percentiles.p50Nanos());
        assertEquals(50, percentiles.maxNanos());
        assertEquals(5, percentiles.sampleCount());
    }

    @Test
    void singleSampleIsEveryPercentile() {
        LatencyRecorder recorder = new LatencyRecorder(1);
        recorder.record(42);

        LatencyRecorder.Percentiles percentiles = recorder.percentiles();

        assertEquals(42, percentiles.p50Nanos());
        assertEquals(42, percentiles.p95Nanos());
        assertEquals(42, percentiles.p99Nanos());
        assertEquals(42, percentiles.p999Nanos());
        assertEquals(42, percentiles.maxNanos());
    }

    @Test
    void emptyRecorderReportsZeroedPercentilesAndNoSamples() {
        LatencyRecorder recorder = new LatencyRecorder(10);

        LatencyRecorder.Percentiles percentiles = recorder.percentiles();

        assertEquals(0, percentiles.sampleCount());
        assertEquals(0, percentiles.p50Nanos());
        assertEquals(0, percentiles.maxNanos());
    }

    @Test
    void samplesPastCapacityAreSilentlyDroppedNotLost() {
        LatencyRecorder recorder = new LatencyRecorder(3);
        recorder.record(1);
        recorder.record(2);
        recorder.record(3);
        recorder.record(999); // past capacity - must not throw, must not corrupt the first three

        LatencyRecorder.Percentiles percentiles = recorder.percentiles();

        assertEquals(3, percentiles.sampleCount());
        assertEquals(3, percentiles.maxNanos());
    }

    @Test
    void concurrentRecordersNeverLoseOrCollideSlots() throws InterruptedException {
        int threads = 8;
        int perThread = 5_000;
        int total = threads * perThread;
        LatencyRecorder recorder = new LatencyRecorder(total);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    ready.countDown();
                    await(go);
                    for (int i = 0; i < perThread; i++) {
                        recorder.record(1);
                    }
                });
            }
            ready.await();
            go.countDown();
        } finally {
            pool.shutdown();
            assertEquals(true, pool.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertEquals(total, recorder.percentiles().sampleCount());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
        }
    }
}
