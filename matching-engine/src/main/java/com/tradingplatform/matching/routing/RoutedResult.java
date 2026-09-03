package com.tradingplatform.matching.routing;

import java.util.Objects;

public record RoutedResult<T>(
        long routingSequence,
        long processingSequence,
        int workerId,
        T value,
        long queueWaitNanos,
        long processingNanos) {

    public RoutedResult {
        if (routingSequence <= 0 || processingSequence <= 0 || workerId < 0) {
            throw new IllegalArgumentException("routing metadata is invalid");
        }
        Objects.requireNonNull(value, "value");
        if (queueWaitNanos < 0 || processingNanos < 0) {
            throw new IllegalArgumentException("timings cannot be negative");
        }
    }
}
