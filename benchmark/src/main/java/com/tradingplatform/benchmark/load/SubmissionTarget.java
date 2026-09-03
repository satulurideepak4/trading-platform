package com.tradingplatform.benchmark.load;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.SubmitOrder;
import java.util.concurrent.CompletableFuture;

/**
 * One thing {@link LoadTestRunner} can drive a {@link Workload} against: the real production
 * {@code OrderRouter} ({@link OrderRouterTarget}), or a benchmark-local variant built specifically
 * to compare against it ({@link CoarseLockedMatchingHarness}, {@link QueueComparisonHarness}).
 *
 * <p>{@code submit}/{@code cancel} return a future rather than a value even for the synchronous
 * targets: {@link LoadTestRunner} measures producer-observed wall-clock latency —
 * {@code System.nanoTime()} bracketing the call and the future's completion — the same way a real
 * caller would experience it, whether that latency comes from queueing, lock contention, or the
 * matching work itself. A synchronous target simply returns an already-completed future.
 *
 * <p>Implementations may throw synchronously, from {@code submit}/{@code cancel} themselves, to
 * signal saturation (a full queue) — the same shape {@code OrderRouter.submit} already has via
 * {@code OrderRoutingRejectedException}. {@link LoadTestRunner} treats any such exception as a
 * saturation event, not a failure of the run.
 */
public interface SubmissionTarget extends AutoCloseable {
    CompletableFuture<SubmissionOutcome> submit(SubmitOrder command);

    CompletableFuture<SubmissionOutcome> cancel(CancelOrder command);

    /** Summed across every shard/worker this target owns; 0 for a target with no internal queue. */
    int queueDepth();

    @Override
    void close();
}
