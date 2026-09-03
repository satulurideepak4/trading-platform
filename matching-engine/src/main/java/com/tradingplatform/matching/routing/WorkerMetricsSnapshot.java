package com.tradingplatform.matching.routing;

public record WorkerMetricsSnapshot(
        int workerId,
        int queueDepth,
        int queueCapacity,
        long admittedCommands,
        long saturatedCommands,
        long processedCommands,
        long failedCommands,
        long totalQueueWaitNanos,
        long totalProcessingNanos,
        long maximumProcessingNanos) {

    public double averageProcessingNanos() {
        return processedCommands == 0 ? 0 : (double) totalProcessingNanos / processedCommands;
    }
}
