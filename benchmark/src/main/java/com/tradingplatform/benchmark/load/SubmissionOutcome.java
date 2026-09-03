package com.tradingplatform.benchmark.load;

/** What every {@link SubmissionTarget} reports back, independent of which one produced it. */
public record SubmissionOutcome(boolean accepted, int executionCount) {}
