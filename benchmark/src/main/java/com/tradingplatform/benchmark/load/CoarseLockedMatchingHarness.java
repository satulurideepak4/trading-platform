package com.tradingplatform.benchmark.load;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.MatchingEngine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The thing ADR-002 chose not to build: one shared {@link MatchingEngine} for every symbol, guarded
 * by one lock, no sharding at all. Exists purely so the coarse-lock-vs-single-writer experiment in
 * {@code docs/performance-engineering.md} has a real, measured "what if" to compare {@code
 * OrderRouter} against, rather than the ADR's prose reasoning standing alone.
 *
 * <p>Deliberately not multi-symbol-aware in any way beyond what {@code MatchingEngine} itself
 * already provides — the whole point is the absence of the per-symbol single-writer partitioning
 * {@code OrderRouter} does, not a competing implementation of it.
 */
public final class CoarseLockedMatchingHarness implements SubmissionTarget {
    private final MatchingEngine engine = new MatchingEngine();
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public CompletableFuture<SubmissionOutcome> submit(SubmitOrder command) {
        lock.lock();
        try {
            CommandResult result = engine.submit(command);
            return CompletableFuture.completedFuture(
                    new SubmissionOutcome(result.accepted(), result.executions().size()));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<SubmissionOutcome> cancel(CancelOrder command) {
        lock.lock();
        try {
            CommandResult result = engine.cancel(command);
            return CompletableFuture.completedFuture(
                    new SubmissionOutcome(result.accepted(), result.executions().size()));
        } finally {
            lock.unlock();
        }
    }

    /** No internal queue — a blocked caller is waiting on the lock, not sitting in a buffer. */
    @Override
    public int queueDepth() {
        return 0;
    }

    @Override
    public void close() {
        // Nothing to release: no thread, no queue, no journal.
    }
}
