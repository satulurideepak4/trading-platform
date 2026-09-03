package com.tradingplatform.portfolio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.pipeline.events.EventIds;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.portfolio.PositionReconciliation.Difference;
import com.tradingplatform.portfolio.PositionReconciliation.ReconciliationReport;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Reconciliation exists for the case where the fast path is trusted but wrong: a hand-edited row,
 * a restore from an older backup, a bug in the fold. These tests simulate that by moving a position
 * directly, bypassing {@link PortfolioUpdater} entirely, which is the only way to create the drift
 * reconciliation is meant to catch.
 */
class PositionReconciliationTest extends PortfolioTestBase {
    private static final AtomicLong EXECUTION_IDS = new AtomicLong(2_000_000);

    @Autowired private PortfolioUpdater updater;
    @Autowired private PortfolioRepository repository;
    @Autowired private PositionReconciliation reconciliation;

    @Test
    void matchesWhenNothingHasDrifted() {
        updater.apply(execution("AAPL", "ACC-R1", "ACC-R2", 10, 19_000));
        updater.apply(execution("AAPL", "ACC-R2", "ACC-R1", 3, 19_500));

        ReconciliationReport report = reconciliation.check();

        assertTrue(report.matches(), () -> "unexpected differences: " + report.differences());
        assertTrue(report.executionsReplayed() >= 2);
    }

    @Test
    void detectsAPositionThatWasChangedOutsideTheNormalPath() {
        ExecutionCreated execution = execution("MSFT", "ACC-R3", "ACC-R4", 5, 42_000);
        updater.apply(execution);

        // Simulates drift: a hand-edited row, a bad migration, a restore from an older backup.
        PositionKey key = new PositionKey("ACC-R3", "DEFAULT", "MSFT");
        repository.upsert(
                key,
                new PositionState(999, 999 * 42_000, 0, 999, 0, 1),
                execution.executionId(),
                Instant.now());

        ReconciliationReport report = reconciliation.check();

        Difference difference = report.differences().stream()
                .filter(candidate -> candidate.key().equals(key))
                .findFirst()
                .orElseThrow(() -> new AssertionError("drifted position was not reported"));
        assertEquals(5, difference.expected().openQuantity());
        assertEquals(999, difference.actual().openQuantity());
    }

    @Test
    void repairOverwritesDriftedPositionsWithWhatTheHistoryImplies() {
        ExecutionCreated execution = execution("TSLA", "ACC-R5", "ACC-R6", 7, 24_000);
        updater.apply(execution);

        PositionKey key = new PositionKey("ACC-R5", "DEFAULT", "TSLA");
        repository.upsert(key, PositionState.FLAT, execution.executionId(), Instant.now());
        assertEquals(0, repository.findPosition(key).orElseThrow().state().openQuantity());

        ReconciliationReport repairReport = reconciliation.repair();

        assertTrue(
                repairReport.differences().stream().anyMatch(diff -> diff.key().equals(key)),
                "repair should report what it found wrong before fixing it");
        assertEquals(7, repository.findPosition(key).orElseThrow().state().openQuantity());
        assertTrue(reconciliation.check().matches(), "repaired state should now match history");
    }

    /**
     * A missing position — one whose row was deleted or never written — must also be rebuilt, not
     * only positions whose values are wrong.
     */
    @Test
    void repairRecreatesAPositionThatWasMissingEntirely() {
        ExecutionCreated execution = execution("AAPL", "ACC-R7", "ACC-R8", 3, 19_100);
        updater.apply(execution);
        repository.deleteAllPositions();

        ReconciliationReport report = reconciliation.repair();

        assertTrue(report.differences().size() >= 2, "both legs of the trade should be reported");
        PositionKey key = new PositionKey("ACC-R7", "DEFAULT", "AAPL");
        assertEquals(3, repository.findPosition(key).orElseThrow().state().openQuantity());
    }

    /**
     * The race {@code repair()}'s exclusive table lock exists to close: without it, a trade that
     * commits after {@code recomputeFromHistory()} reads but before the delete+reinsert would be
     * deleted and then silently overwritten with the stale value {@code repair()} computed before
     * that trade ever happened. Rather than hand-orchestrate that exact interleaving (fragile,
     * timing-dependent, and would need a test-only hook into production code to force), this races
     * real {@code repair()} and real {@code updater.apply()} calls against each other under real
     * contention and asserts the one invariant a lost update would violate: after everything
     * settles, the stored positions still exactly equal what the full trade history implies.
     */
    @Test
    void repairRunningConcurrentlyWithNewTradesNeverLosesOne() throws InterruptedException {
        String symbol = "NVDA";
        String buyer = "ACC-R9";
        String seller = "ACC-R10";
        PositionKey buyerKey = new PositionKey(buyer, "DEFAULT", symbol);
        updater.apply(execution(symbol, buyer, seller, 100, 12_000)); // seeds a position to repair

        int repairers = 4;
        int tradersPerRepairer = 4;
        List<ExecutionCreated> concurrentTrades = java.util.stream.IntStream
                .range(0, repairers * tradersPerRepairer)
                .mapToObj(index -> execution(symbol, buyer, seller, 1, 12_000))
                .toList();

        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService threads = Executors.newFixedThreadPool(repairers + concurrentTrades.size())) {
            for (int index = 0; index < repairers; index++) {
                threads.submit(() -> {
                    start.await();
                    reconciliation.repair();
                    return null;
                });
            }
            for (ExecutionCreated trade : concurrentTrades) {
                threads.submit(() -> {
                    start.await();
                    updater.apply(trade);
                    return null;
                });
            }
            start.countDown();
            threads.shutdown();
            assertTrue(threads.awaitTermination(30, TimeUnit.SECONDS));
        }

        ReconciliationReport finalCheck = reconciliation.check();
        assertTrue(
                finalCheck.matches(),
                () -> "a concurrent repair() lost a trade instead of blocking on it: "
                        + finalCheck.differences());
        assertEquals(
                100 + concurrentTrades.size(),
                repository.findPosition(buyerKey).orElseThrow().state().openQuantity());
    }

    private static ExecutionCreated execution(
            String symbol, String buyAccount, String sellAccount, long quantity, long price) {
        long executionId = EXECUTION_IDS.incrementAndGet();
        return new ExecutionCreated(
                EventIds.execution(executionId),
                "test",
                executionId,
                symbol,
                price,
                quantity,
                executionId * 10,
                buyAccount,
                "DEFAULT",
                executionId * 10 + 1,
                sellAccount,
                "DEFAULT",
                executionId * 10 + 1,
                executionId * 10,
                0,
                0,
                Instant.parse("2026-08-19T09:00:00Z"));
    }
}
