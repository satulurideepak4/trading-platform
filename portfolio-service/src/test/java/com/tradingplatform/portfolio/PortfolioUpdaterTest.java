package com.tradingplatform.portfolio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.pipeline.events.EventIds;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Exercises {@link PortfolioUpdater} against a real Postgres, which is the only way to prove the
 * transactional idempotency claim: it depends on a real primary-key conflict and a real commit
 * boundary, neither of which an in-memory fake can stand in for honestly.
 */
class PortfolioUpdaterTest extends PortfolioTestBase {
    private static final AtomicLong EXECUTION_IDS = new AtomicLong(1_000_000);

    @Autowired private PortfolioUpdater updater;
    @Autowired private PortfolioRepository repository;

    @Test
    void applyingTheSameExecutionTwiceMovesThePositionOnce() {
        ExecutionCreated execution = execution("AAPL", "ACC-A1", "ACC-A2", 10, 19_000);

        assertTrue(updater.apply(execution));
        assertFalse(updater.apply(execution));

        assertEquals(10, positionOf("ACC-A1", "AAPL").openQuantity());
        assertEquals(-10, positionOf("ACC-A2", "AAPL").openQuantity());
        assertEquals(1, positionOf("ACC-A1", "AAPL").executionCount());
    }

    @Test
    void aSelfTradeBooksBothLegsInsteadOfNettingToNothing() {
        ExecutionCreated selfTrade = execution("MSFT", "ACC-B1", "ACC-B1", 6, 42_000);

        updater.apply(selfTrade);

        PositionState position = positionOf("ACC-B1", "MSFT");
        assertEquals(6, position.boughtQuantity());
        assertEquals(6, position.soldQuantity());
        assertEquals(0, position.openQuantity());
    }

    @Test
    void averagingAndRealizingFollowTheStandardWeightedAverageCostRules() {
        String buyer = "ACC-C1";
        String seller = "ACC-C2";
        updater.apply(execution("NVDA", buyer, seller, 10, 100));
        updater.apply(execution("NVDA", seller, buyer, 4, 130)); // buyer sells 4 back to seller

        PositionState buyerPosition = positionOf(buyer, "NVDA");
        assertEquals(6, buyerPosition.openQuantity());
        assertEquals(600, buyerPosition.openCost());
        assertEquals(120, buyerPosition.realizedPnl());
    }

    /**
     * The transaction is what makes this true. If the execution insert and the two position
     * updates were three separate statements, a crash between them would leave a position moved
     * without its trade recorded, or a trade recorded whose positions never moved.
     */
    @Test
    void concurrentApplicationsOfTheSameExecutionResultInExactlyOneAppliedCall() throws Exception {
        ExecutionCreated execution = execution("TSLA", "ACC-D1", "ACC-D2", 5, 24_000);
        int callers = 8;
        AtomicInteger applied = new AtomicInteger();

        try (ExecutorService threads = Executors.newFixedThreadPool(callers)) {
            CountDownLatch start = new CountDownLatch(1);
            for (int index = 0; index < callers; index++) {
                threads.submit(() -> {
                    start.await();
                    if (updater.apply(execution)) {
                        applied.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
            threads.shutdown();
            assertTrue(threads.awaitTermination(20, TimeUnit.SECONDS));
        }

        assertEquals(1, applied.get());
        assertEquals(5, positionOf("ACC-D1", "TSLA").openQuantity());
        assertEquals(1, positionOf("ACC-D1", "TSLA").executionCount());
    }

    private PositionState positionOf(String accountId, String symbol) {
        return repository
                .findPosition(new PositionKey(accountId, "DEFAULT", symbol))
                .orElseThrow(() -> new AssertionError("no position for " + accountId + " " + symbol))
                .state();
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
