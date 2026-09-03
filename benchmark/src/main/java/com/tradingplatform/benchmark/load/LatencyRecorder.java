package com.tradingplatform.benchmark.load;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collects nanosecond latency samples from any number of concurrent threads and reports
 * nearest-rank percentiles at the end of a run.
 *
 * <p>Pre-sized rather than a growable collection: the caller knows the maximum possible sample
 * count up front (at most one sample per submitted command), so this is a single array write per
 * sample with no allocation, no boxing, and no lock — {@link AtomicInteger#getAndIncrement()}
 * reserves a slot, the write to that slot needs no further coordination since no two threads ever
 * reserve the same index.
 *
 * <p>Percentiles are computed by sorting a copy at the end of the run, not streamed — this is a
 * benchmark tool run once per workload, not a production hot path, so simplicity wins over an
 * HdrHistogram-style streaming structure that would matter at a very different scale.
 */
public final class LatencyRecorder {
    private final long[] samples;
    private final AtomicInteger count = new AtomicInteger();

    public LatencyRecorder(int expectedMaxSamples) {
        this.samples = new long[Math.max(1, expectedMaxSamples)];
    }

    /** Silently drops anything past the capacity given at construction; never blocks or throws. */
    public void record(long nanos) {
        int index = count.getAndIncrement();
        if (index < samples.length) {
            samples[index] = nanos;
        }
    }

    public int sampleCount() {
        return Math.min(count.get(), samples.length);
    }

    public Percentiles percentiles() {
        int n = sampleCount();
        if (n == 0) {
            return new Percentiles(0, 0, 0, 0, 0, 0);
        }
        long[] sorted = Arrays.copyOf(samples, n);
        Arrays.sort(sorted);
        return new Percentiles(
                sorted[rank(n, 0.50)],
                sorted[rank(n, 0.95)],
                sorted[rank(n, 0.99)],
                sorted[rank(n, 0.999)],
                sorted[n - 1],
                n);
    }

    /** Nearest-rank: the smallest index i such that (i+1)/n >= p, clamped into range. */
    private static int rank(int n, double p) {
        int index = (int) Math.ceil(p * n) - 1;
        return Math.max(0, Math.min(n - 1, index));
    }

    public record Percentiles(
            long p50Nanos, long p95Nanos, long p99Nanos, long p999Nanos, long maxNanos, int sampleCount) {

        public double p50Micros() {
            return p50Nanos / 1_000.0;
        }

        public double p95Micros() {
            return p95Nanos / 1_000.0;
        }

        public double p99Micros() {
            return p99Nanos / 1_000.0;
        }

        public double p999Micros() {
            return p999Nanos / 1_000.0;
        }

        public double maxMicros() {
            return maxNanos / 1_000.0;
        }
    }
}
