package com.tradingplatform.matching.routing;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.MatchingEngine;
import com.tradingplatform.matching.OrderBookSnapshot;
import com.tradingplatform.matching.journal.CommandJournal;
import com.tradingplatform.matching.journal.FileCommandJournal;
import com.tradingplatform.matching.journal.FlushPolicy;
import com.tradingplatform.matching.journal.JournalCorruptionException;
import com.tradingplatform.matching.journal.JournalReader;
import com.tradingplatform.matching.journal.JournalRecord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * Routes every symbol to one worker and keeps matching-engine state confined to that worker.
 * Queue admission is serialized only long enough to establish a deterministic admission order;
 * matching for different workers proceeds concurrently.
 */
public final class OrderRouter implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(OrderRouter.class.getName());
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private final Object admissionLock = new Object();
    private final List<MatchingWorker> workers;
    private final Map<Long, Integer> orderOwners = new HashMap<>();
    private final Map<String, Integer> clientOrderOwners = new HashMap<>();

    private boolean accepting = true;
    private long nextRoutingSequence = 1;

    public OrderRouter(int workerCount, int queueCapacity) {
        this(workerCount, queueCapacity, null, FlushPolicy.EVERY_RECORD, true);
    }

    /**
     * Recovers each worker's state from its journal in {@code journalDirectory} before accepting
     * any new traffic, then journals every command it processes from then on. See ADR-015.
     *
     * @param flushPolicy how durably each journaled command is pushed to disk before its caller can
     *     observe it as successful
     */
    public OrderRouter(
            int workerCount, int queueCapacity, Path journalDirectory, FlushPolicy flushPolicy) {
        this(workerCount, queueCapacity, journalDirectory, flushPolicy, true);
    }

    OrderRouter(int workerCount, int queueCapacity, boolean startImmediately) {
        this(workerCount, queueCapacity, null, FlushPolicy.EVERY_RECORD, startImmediately);
    }

    private OrderRouter(
            int workerCount,
            int queueCapacity,
            Path journalDirectory,
            FlushPolicy flushPolicy,
            boolean startImmediately) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }

        List<MatchingWorker> configuredWorkers = new ArrayList<>(workerCount);
        for (int workerId = 0; workerId < workerCount; workerId++) {
            MatchingEngine engine = new MatchingEngine(workerId + 1L, workerCount);
            CommandJournal journal = null;
            long nextJournalSequence = 1;
            if (journalDirectory != null) {
                Path journalFile = FileCommandJournal.fileFor(journalDirectory, workerId);
                List<JournalRecord> recovered = JournalReader.read(journalFile);
                replay(engine, workerId, recovered);
                if (!recovered.isEmpty()) {
                    nextJournalSequence = recovered.get(recovered.size() - 1).journalSequence() + 1;
                }
                truncateTornTail(journalFile);
                journal = new FileCommandJournal(journalFile, flushPolicy);
            }
            configuredWorkers.add(
                    new MatchingWorker(workerId, queueCapacity, engine, journal, nextJournalSequence));
        }
        workers = List.copyOf(configuredWorkers);
        if (startImmediately) {
            startWorkers();
        }
    }

    /**
     * Drops any torn trailing bytes {@link JournalReader} stopped short of — the shape a crash
     * mid-append leaves behind — before this worker resumes writing to the file.
     *
     * <p>Left alone, new records would be appended after that leftover garbage rather than in its
     * place. The next restart's reader would then either silently lose every record written after
     * it (if the garbage happens to decode as a plausible but oversized frame length) or throw
     * {@link JournalCorruptionException} on a file that is otherwise completely intact — in both
     * cases turning one ordinary crash into permanent damage to everything journaled afterward.
     */
    private static void truncateTornTail(Path journalFile) {
        if (!Files.exists(journalFile)) {
            return;
        }
        long validLength = JournalReader.validByteLength(journalFile);
        try (FileChannel channel = FileChannel.open(journalFile, StandardOpenOption.WRITE)) {
            if (channel.size() > validLength) {
                channel.truncate(validLength);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "could not truncate torn tail from journal " + journalFile, e);
        }
    }

    /**
     * Rebuilds one worker's engine state by replaying its journal directly against the engine —
     * never through {@link #enqueue}, so nothing here re-journals what it is reading, and never
     * through anything above this router, so nothing re-publishes an event a downstream consumer
     * already saw before the restart. See ADR-015 and {@code docs/replay-and-reconciliation.md}.
     */
    private void replay(MatchingEngine engine, int workerId, List<JournalRecord> records) {
        for (JournalRecord record : records) {
            switch (record) {
                case JournalRecord.Submit submit -> {
                    engine.submit(submit.command());
                    reserveIdentifiers(submit.command(), workerId);
                }
                case JournalRecord.Cancel cancel -> engine.cancel(cancel.command());
                case JournalRecord.Replace replace -> engine.replace(replace.command());
            }
        }
    }

    public CompletableFuture<RoutedResult<CommandResult>> submit(SubmitOrder command) {
        synchronized (admissionLock) {
            ensureAccepting();
            int workerId = selectSubmitWorker(command);
            CompletableFuture<RoutedResult<CommandResult>> result =
                    enqueue(workerId, command, engine -> engine.submit(command));
            reserveIdentifiers(command, workerId);
            return result;
        }
    }

    public CompletableFuture<RoutedResult<CommandResult>> cancel(CancelOrder command) {
        synchronized (admissionLock) {
            ensureAccepting();
            int workerId = command == null
                    ? 0
                    : orderOwners.getOrDefault(command.orderId(), 0);
            return enqueue(workerId, command, engine -> engine.cancel(command));
        }
    }

    public CompletableFuture<RoutedResult<CommandResult>> replace(ReplaceOrder command) {
        synchronized (admissionLock) {
            ensureAccepting();
            int workerId = command == null
                    ? 0
                    : orderOwners.getOrDefault(command.orderId(), 0);
            return enqueue(workerId, command, engine -> engine.replace(command));
        }
    }

    public CompletableFuture<RoutedResult<Optional<OrderSnapshot>>> findOrder(long orderId) {
        synchronized (admissionLock) {
            ensureAccepting();
            int workerId = orderOwners.getOrDefault(orderId, 0);
            return enqueue(workerId, engine -> engine.findOrder(orderId));
        }
    }

    public CompletableFuture<RoutedResult<OrderBookSnapshot>> book(String symbol) {
        synchronized (admissionLock) {
            ensureAccepting();
            int workerId = workerIndex(symbol);
            return enqueue(workerId, engine -> engine.book(symbol));
        }
    }

    public int workerCount() {
        return workers.size();
    }

    public int workerIndex(String symbol) {
        return symbol == null ? 0 : Math.floorMod(symbol.hashCode(), workers.size());
    }

    public RouterMetricsSnapshot metrics() {
        return new RouterMetricsSnapshot(workers.stream().map(MatchingWorker::metrics).toList());
    }

    @Override
    public void close() {
        synchronized (admissionLock) {
            if (!accepting) {
                return;
            }
            accepting = false;
        }

        workers.forEach(MatchingWorker::requestStopAfterDrain);
        long deadline = System.nanoTime() + SHUTDOWN_TIMEOUT.toNanos();
        for (MatchingWorker worker : workers) {
            long remainingNanos = Math.max(0, deadline - System.nanoTime());
            if (!worker.awaitTermination(remainingNanos)) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Matching worker {0} did not drain before shutdown timeout",
                        worker.workerId());
                worker.forceStop();
            }
        }
        // Only after every worker has stopped touching its engine: a journal write racing this
        // close would either be lost mid-flight or corrupt the file's append ordering.
        //
        // Not workers.forEach(MatchingWorker::closeJournal) - forEach aborts on the first
        // exception, and requestStopAfterDrain's thread.interrupt() can (rarely) land while a
        // worker is blocked inside its own journal's channel.write()/force(), which the JDK's
        // InterruptibleChannel contract responds to by closing that channel out from under it.
        // That worker's own close() then throws on the force() below - real, and not fully
        // avoidable without abandoning interrupt() as the wake signal for a blocked queue.poll()
        // - but it must never cost every worker after it in this list its own clean flush too.
        for (MatchingWorker worker : workers) {
            try {
                worker.closeJournal();
            } catch (RuntimeException failure) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Matching worker " + worker.workerId() + " failed to close its journal "
                                + "cleanly during shutdown",
                        failure);
            }
        }
    }

    void startWorkers() {
        workers.forEach(MatchingWorker::start);
    }

    private int selectSubmitWorker(SubmitOrder command) {
        if (command == null) {
            return 0;
        }
        Integer existingOrderOwner = orderOwners.get(command.orderId());
        if (existingOrderOwner != null) {
            return existingOrderOwner;
        }
        Integer existingClientOwner = clientOrderOwners.get(command.clientOrderId());
        return existingClientOwner != null ? existingClientOwner : workerIndex(command.symbol());
    }

    private void reserveIdentifiers(SubmitOrder command, int workerId) {
        if (command == null
                || command.orderId() <= 0
                || command.clientOrderId() == null
                || command.clientOrderId().isBlank()
                || command.clientOrderId().length() > 64
                || orderOwners.containsKey(command.orderId())
                || clientOrderOwners.containsKey(command.clientOrderId())) {
            return;
        }
        orderOwners.put(command.orderId(), workerId);
        clientOrderOwners.put(command.clientOrderId(), workerId);
    }

    private <T> CompletableFuture<RoutedResult<T>> enqueue(
            int workerId, Function<MatchingEngine, T> operation) {
        return enqueue(workerId, null, operation);
    }

    /**
     * @param journalableCommand the concrete command to durably journal before it is applied, or
     *     {@code null} for a read-only operation ({@code findOrder}, {@code book}) that changes
     *     nothing and so has nothing to recover
     */
    private <T> CompletableFuture<RoutedResult<T>> enqueue(
            int workerId, Object journalableCommand, Function<MatchingEngine, T> operation) {
        long routingSequence = nextRoutingSequence++;
        CompletableFuture<RoutedResult<T>> result =
                workers.get(workerId).enqueue(routingSequence, journalableCommand, operation);
        if (result == null) {
            throw new OrderRoutingRejectedException(
                    RoutingRejectionReason.QUEUE_FULL,
                    workerId,
                    "matching queue is full for worker " + workerId);
        }
        return result;
    }

    private void ensureAccepting() {
        if (!accepting) {
            throw new OrderRoutingRejectedException(
                    RoutingRejectionReason.ROUTER_CLOSED, -1, "order router is closed");
        }
    }

    private static final class MatchingWorker implements Runnable {
        private final int workerId;
        private final int queueCapacity;
        private final MatchingEngine engine;
        private final CommandJournal journal;
        private final ArrayBlockingQueue<WorkItem<?>> queue;
        private final Thread thread;
        private final LongAdder admittedCommands = new LongAdder();
        private final LongAdder saturatedCommands = new LongAdder();
        private final LongAdder processedCommands = new LongAdder();
        private final LongAdder failedCommands = new LongAdder();
        private final LongAdder totalQueueWaitNanos = new LongAdder();
        private final LongAdder totalProcessingNanos = new LongAdder();
        private final AtomicLong maximumProcessingNanos = new AtomicLong();

        private volatile boolean stopRequested;
        private volatile boolean started;
        private long nextProcessingSequence = 1;
        private long nextJournalSequence;

        private MatchingWorker(
                int workerId,
                int queueCapacity,
                MatchingEngine engine,
                CommandJournal journal,
                long nextJournalSequence) {
            this.workerId = workerId;
            this.queueCapacity = queueCapacity;
            this.engine = engine;
            this.journal = journal;
            this.nextJournalSequence = nextJournalSequence;
            this.queue = new ArrayBlockingQueue<>(queueCapacity);
            this.thread = Thread.ofPlatform()
                    .name("matching-worker-" + workerId)
                    .unstarted(this);
        }

        private synchronized void start() {
            if (!started) {
                started = true;
                thread.start();
            }
        }

        private <T> CompletableFuture<RoutedResult<T>> enqueue(
                long routingSequence, Object journalableCommand, Function<MatchingEngine, T> operation) {
            CompletableFuture<RoutedResult<T>> completion = new CompletableFuture<>();
            WorkItem<T> item = new WorkItem<>(
                    routingSequence, System.nanoTime(), journalableCommand, operation, completion);
            if (!queue.offer(item)) {
                saturatedCommands.increment();
                return null;
            }
            admittedCommands.increment();
            return completion;
        }

        @Override
        public void run() {
            while (!stopRequested || !queue.isEmpty()) {
                try {
                    WorkItem<?> item = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (item != null) {
                        processCaptured(item);
                    }
                } catch (InterruptedException interrupted) {
                    if (!stopRequested) {
                        LOGGER.log(
                                System.Logger.Level.WARNING,
                                "Matching worker {0} was interrupted while active",
                                workerId);
                    }
                }
            }
        }

        private <T> void processCaptured(WorkItem<T> item) {
            long processingSequence = nextProcessingSequence++;
            long startedAt = System.nanoTime();
            long queueWaitNanos = Math.max(0, startedAt - item.enqueuedAtNanos());
            try {
                // Written before the command is applied, and durable (for FlushPolicy.EVERY_RECORD)
                // before this call returns: nothing observes this command as successful — the
                // completion below has not fired yet — until it can be recovered after a crash.
                if (journal != null && item.journalableCommand() != null) {
                    journal.append(toJournalRecord(item.journalableCommand()));
                }
                T value = item.operation().apply(engine);
                long processingNanos = Math.max(0, System.nanoTime() - startedAt);
                recordSuccess(queueWaitNanos, processingNanos);
                item.completion().complete(new RoutedResult<>(
                        item.routingSequence(),
                        processingSequence,
                        workerId,
                        value,
                        queueWaitNanos,
                        processingNanos));
            } catch (Throwable failure) {
                failedCommands.increment();
                item.completion().completeExceptionally(failure);
                LOGGER.log(
                        System.Logger.Level.ERROR,
                        "Matching worker " + workerId + " failed command "
                                + item.routingSequence(),
                        failure);
            }
        }

        private JournalRecord toJournalRecord(Object command) {
            long journalSequence = nextJournalSequence++;
            return switch (command) {
                case SubmitOrder submit -> new JournalRecord.Submit(journalSequence, workerId, submit);
                case CancelOrder cancel -> new JournalRecord.Cancel(journalSequence, workerId, cancel);
                case ReplaceOrder replace -> new JournalRecord.Replace(journalSequence, workerId, replace);
                default -> throw new IllegalStateException("not a journalable command: " + command);
            };
        }

        private void closeJournal() {
            if (journal != null) {
                journal.close();
            }
        }

        private void recordSuccess(long queueWaitNanos, long processingNanos) {
            processedCommands.increment();
            totalQueueWaitNanos.add(queueWaitNanos);
            totalProcessingNanos.add(processingNanos);
            maximumProcessingNanos.accumulateAndGet(processingNanos, Math::max);
        }

        private void requestStopAfterDrain() {
            stopRequested = true;
            if (!started) {
                failQueuedCommands();
            } else {
                thread.interrupt();
            }
        }

        private boolean awaitTermination(long timeoutNanos) {
            if (!started) {
                return true;
            }
            try {
                thread.join(Duration.ofNanos(timeoutNanos));
                return !thread.isAlive();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void forceStop() {
            thread.interrupt();
            failQueuedCommands();
        }

        private void failQueuedCommands() {
            OrderRoutingRejectedException failure = new OrderRoutingRejectedException(
                    RoutingRejectionReason.ROUTER_CLOSED,
                    workerId,
                    "router closed before queued command was processed");
            WorkItem<?> item;
            while ((item = queue.poll()) != null) {
                item.completion().completeExceptionally(failure);
            }
        }

        private WorkerMetricsSnapshot metrics() {
            return new WorkerMetricsSnapshot(
                    workerId,
                    queue.size(),
                    queueCapacity,
                    admittedCommands.sum(),
                    saturatedCommands.sum(),
                    processedCommands.sum(),
                    failedCommands.sum(),
                    totalQueueWaitNanos.sum(),
                    totalProcessingNanos.sum(),
                    maximumProcessingNanos.get());
        }

        private int workerId() {
            return workerId;
        }
    }

    private record WorkItem<T>(
            long routingSequence,
            long enqueuedAtNanos,
            Object journalableCommand,
            Function<MatchingEngine, T> operation,
            CompletableFuture<RoutedResult<T>> completion) {}
}
