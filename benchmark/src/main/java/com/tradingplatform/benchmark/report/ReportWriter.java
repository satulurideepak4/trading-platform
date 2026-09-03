package com.tradingplatform.benchmark.report;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Writes one {@link RunResult} as a self-contained Markdown file into {@code benchmarks/results/}
 * at the repository root, every time — never a hand-edited file, so every number in it traces back
 * to an actual run. Every file carries the same hardware/JDK disclosure header so a reader never has
 * to guess what machine produced it; see docs/benchmark-methodology.md for the full disclosure.
 */
public final class ReportWriter {
    private static final DateTimeFormatter FILENAME_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS").withZone(java.time.ZoneOffset.UTC);

    private ReportWriter() {}

    public static String write(RunResult result) {
        Path resultsDir = resultsDirectory();
        try {
            Files.createDirectories(resultsDir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create " + resultsDir, e);
        }

        String filename = String.format(
                Locale.ROOT,
                "%s-%s-%s.md",
                result.workload().name(),
                result.targetName(),
                FILENAME_TIMESTAMP.format(Instant.now()));
        Path file = resultsDir.resolve(filename);
        try {
            Files.writeString(file, render(result));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write " + file, e);
        }
        return file.toString();
    }

    /**
     * Every JMH/{@code LoadTestRunner} invocation is expected to run from either the repository
     * root or the {@code benchmark} module directory (the two places the module's own README tells
     * a reader to run it from); both resolve to the same {@code benchmarks/results/} directory at
     * the repository root.
     */
    private static Path resultsDirectory() {
        Path cwd = Path.of("").toAbsolutePath();
        Path root = "benchmark".equals(String.valueOf(cwd.getFileName())) ? cwd.getParent() : cwd;
        return root.resolve("benchmarks").resolve("results");
    }

    private static String render(RunResult result) {
        StringBuilder markdown = new StringBuilder();
        markdown.append("# Load test: ").append(result.workload().name())
                .append(" / ").append(result.targetName()).append('\n').append('\n');
        appendDisclosure(markdown);
        appendConfiguration(markdown, result);
        appendResults(markdown, result);
        return markdown.toString();
    }

    private static void appendDisclosure(StringBuilder markdown) {
        markdown.append("## Environment\n\n")
                .append("- Generated: ").append(Instant.now()).append('\n')
                .append("- JDK: ").append(System.getProperty("java.vendor"))
                .append(' ').append(System.getProperty("java.version")).append('\n')
                .append("- OS/arch: ").append(System.getProperty("os.name"))
                .append(' ').append(System.getProperty("os.arch")).append('\n')
                .append("- Available processors: ")
                .append(Runtime.getRuntime().availableProcessors()).append('\n')
                .append("- **This is a developer laptop, not a dedicated benchmark rig.**")
                .append(" See docs/benchmark-methodology.md for the full hardware disclosure and")
                .append(" why these are directional numbers, not vendor-comparable throughput")
                .append(" claims.\n\n");
    }

    private static void appendConfiguration(StringBuilder markdown, RunResult result) {
        var workload = result.workload();
        markdown.append("## Configuration\n\n")
                .append("| Parameter | Value |\n|---|---|\n")
                .append("| target | ").append(result.targetName()).append(" |\n")
                .append("| symbolCount | ").append(workload.symbolCount()).append(" |\n")
                .append("| ordersPerSecond (target) | ")
                .append(workload.ordersPerSecond() == 0 ? "unthrottled" : workload.ordersPerSecond())
                .append(" |\n")
                .append("| cancelRatio | ").append(workload.cancelRatio()).append(" |\n")
                .append("| matchBandTicks | ").append(workload.matchBandTicks()).append(" |\n")
                .append("| orderCount | ").append(workload.orderCount()).append(" |\n")
                .append("| workerCount | ").append(workload.workerCount()).append(" |\n")
                .append("| queueCapacity | ").append(workload.queueCapacity()).append(" |\n")
                .append("| producerCount | ").append(result.producerCount()).append(" |\n")
                .append("| seed | ").append(workload.seed()).append(" |\n\n");
    }

    private static void appendResults(StringBuilder markdown, RunResult result) {
        var latency = result.latency();
        markdown.append("## Results\n\n")
                .append(String.format(Locale.ROOT, "- Elapsed: %.3fs%n", result.elapsedSeconds()))
                .append(String.format(
                        Locale.ROOT, "- Throughput (accepted+rejected/s): %.1f%n",
                        result.throughputPerSecond()))
                .append("- Admitted: ").append(result.admitted()).append('\n')
                .append("- Accepted: ").append(result.accepted()).append('\n')
                .append("- Rejected: ").append(result.rejected()).append('\n')
                .append("- Saturated (queue-full at submit time): ").append(result.saturated())
                .append('\n')
                .append("- Executions: ").append(result.executions()).append('\n')
                .append("- Cancels sent: ").append(result.cancelsSent()).append("\n\n")
                .append("### Producer-observed latency (microseconds)\n\n")
                .append("| p50 | p95 | p99 | p99.9 | max | samples |\n|---|---|---|---|---|---|\n")
                .append(String.format(
                        Locale.ROOT, "| %.2f | %.2f | %.2f | %.2f | %.2f | %d |%n%n",
                        latency.p50Micros(), latency.p95Micros(), latency.p99Micros(),
                        latency.p999Micros(), latency.maxMicros(), latency.sampleCount()))
                .append("### Queue depth\n\n")
                .append("- Max observed: ").append(result.maxQueueDepth()).append('\n')
                .append(String.format(Locale.ROOT, "- Average observed: %.1f%n", result.avgQueueDepth()));
    }
}
