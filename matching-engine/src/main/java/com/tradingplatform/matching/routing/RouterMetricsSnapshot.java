package com.tradingplatform.matching.routing;

import java.util.List;

public record RouterMetricsSnapshot(List<WorkerMetricsSnapshot> workers) {
    public RouterMetricsSnapshot {
        workers = List.copyOf(workers);
    }

    public int queueDepth() {
        return workers.stream().mapToInt(WorkerMetricsSnapshot::queueDepth).sum();
    }

    public long admittedCommands() {
        return workers.stream().mapToLong(WorkerMetricsSnapshot::admittedCommands).sum();
    }

    public long saturatedCommands() {
        return workers.stream().mapToLong(WorkerMetricsSnapshot::saturatedCommands).sum();
    }

    public long processedCommands() {
        return workers.stream().mapToLong(WorkerMetricsSnapshot::processedCommands).sum();
    }
}
