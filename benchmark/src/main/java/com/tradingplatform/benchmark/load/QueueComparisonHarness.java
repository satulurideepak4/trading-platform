package com.tradingplatform.benchmark.load;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.MatchingEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * A minimal single-writer-per-book harness — one worker thread and one {@link MatchingEngine} per
 * shard, symbol routed by a fixed hash, exactly {@code OrderRouter}'s own architecture — built
 * specifically to swap the one thing this experiment varies: the admission queue implementation.
 * No journal, no metrics, no owner-map, no graceful-drain shutdown: everything {@code OrderRouter}
 * also does that isn't the queue itself is deliberately absent, so a difference in the numbers can
 * only be the queue.
 *
 * <p>Changing {@code OrderRouter}'s own queue to run this comparison was ruled out on purpose — see
 * the benchmark module's design note in {@code docs/performance-engineering.md}: this stage does
 * not modify already-shipped, tested production code for the sake of a comparison.
 *
 * <p>Unlike {@code OrderRouter}, whose order-owner map lives behind its admission lock, this
 * harness's equivalent map is a plain {@code ConcurrentHashMap} written at submit time — deliberate,
 * since this experiment is about the queue implementation, not admission-lock contention.
 */
public final class QueueComparisonHarness implements SubmissionTarget {
    private final List<Worker> workers;
    // Populated synchronously at submit time, the same way OrderRouter.reserveIdentifiers works -
    // needed because CancelOrder carries no symbol, only the id of the order being cancelled.
    private final Map<Long, Integer> orderOwners = new ConcurrentHashMap<>();

    public QueueComparisonHarness(
            int workerCount, int queueCapacity, IntFunction<BlockingQueue<Runnable>> queueFactory) {
        List<Worker> configured = new ArrayList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            configured.add(new Worker(queueFactory.apply(queueCapacity)));
        }
        this.workers = List.copyOf(configured);
        this.workers.forEach(Worker::start);
    }

    @Override
    public CompletableFuture<SubmissionOutcome> submit(SubmitOrder command) {
        int workerIndex = Math.floorMod(command.symbol().hashCode(), workers.size());
        orderOwners.putIfAbsent(command.orderId(), workerIndex);
        return dispatch(workerIndex, engine -> engine.submit(command));
    }

    @Override
    public CompletableFuture<SubmissionOutcome> cancel(CancelOrder command) {
        int workerIndex = orderOwners.getOrDefault(command.orderId(), 0);
        return dispatch(workerIndex, engine -> engine.cancel(command));
    }

    // Throws synchronously rather than completing the future exceptionally, matching
    // SubmissionTarget's documented saturation contract (the same shape OrderRouter.submit already
    // has via OrderRoutingRejectedException) - LoadTestRunner only recognizes a synchronous throw as
    // a saturation event, not merely an exceptionally-completed future, so a caller here needs the
    // same synchronous signal to be counted correctly instead of silently miscounted as "rejected."
    private CompletableFuture<SubmissionOutcome> dispatch(
            int workerIndex, Function<MatchingEngine, CommandResult> operation) {
        Worker worker = workers.get(workerIndex);
        CompletableFuture<SubmissionOutcome> future = new CompletableFuture<>();
        boolean offered = worker.queue.offer(() -> {
            CommandResult result = operation.apply(worker.engine);
            future.complete(new SubmissionOutcome(result.accepted(), result.executions().size()));
        });
        if (!offered) {
            throw new QueueSaturatedException();
        }
        return future;
    }

    @Override
    public int queueDepth() {
        return workers.stream().mapToInt(w -> w.queue.size()).sum();
    }

    @Override
    public void close() {
        workers.forEach(Worker::stop);
    }

    /** Thrown synchronously by {@link #submit}/{@link #cancel} when a shard's queue is full. */
    public static final class QueueSaturatedException extends RuntimeException {}

    private static final class Worker {
        private final MatchingEngine engine = new MatchingEngine();
        private final BlockingQueue<Runnable> queue;
        private final Thread thread;
        private volatile boolean running = true;

        private Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
            this.thread = Thread.ofPlatform().name("queue-comparison-worker").unstarted(() -> {
                while (running || !queue.isEmpty()) {
                    try {
                        Runnable task = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (task != null) {
                            task.run();
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        private void start() {
            thread.start();
        }

        private void stop() {
            running = false;
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
