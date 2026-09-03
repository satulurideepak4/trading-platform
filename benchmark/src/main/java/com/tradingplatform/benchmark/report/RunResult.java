package com.tradingplatform.benchmark.report;

import com.tradingplatform.benchmark.load.LatencyRecorder;
import com.tradingplatform.benchmark.load.Workload;
import java.util.Locale;

/**
 * Everything one {@code LoadTestRunner} run measured, plus what it measured against — enough to
 * both print a console summary and write a self-contained report file.
 */
public record RunResult(
        Workload workload,
        String targetName,
        int producerCount,
        long elapsedNanos,
        long admitted,
        long saturated,
        long accepted,
        long rejected,
        long executions,
        long cancelsSent,
        LatencyRecorder.Percentiles latency,
        int maxQueueDepth,
        double avgQueueDepth) {

    public double elapsedSeconds() {
        return elapsedNanos / 1_000_000_000.0;
    }

    /** Completed (accepted + rejected) commands per second — excludes saturated submissions, which
     * the target never actually processed. */
    public double throughputPerSecond() {
        double seconds = elapsedSeconds();
        return seconds == 0 ? 0.0 : (accepted + rejected) / seconds;
    }

    public String toConsoleSummary() {
        return String.format(
                Locale.ROOT,
                """
                event=load_test_complete workload=%s target=%s producers=%d
                  elapsed=%.3fs admitted=%d saturated=%d accepted=%d rejected=%d executions=%d cancels=%d
                  throughput=%.1f/s
                  latency(us) p50=%.2f p95=%.2f p99=%.2f p999=%.2f max=%.2f samples=%d
                  queueDepth max=%d avg=%.1f""",
                workload.name(), targetName, producerCount,
                elapsedSeconds(), admitted, saturated, accepted, rejected, executions, cancelsSent,
                throughputPerSecond(),
                latency.p50Micros(), latency.p95Micros(), latency.p99Micros(), latency.p999Micros(),
                latency.maxMicros(), latency.sampleCount(),
                maxQueueDepth, avgQueueDepth);
    }
}
