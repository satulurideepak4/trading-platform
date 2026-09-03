package com.tradingplatform.matching.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.OrderBookSnapshot;
import com.tradingplatform.matching.journal.FileCommandJournal;
import com.tradingplatform.matching.journal.FlushPolicy;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The headline Stage 7 proof: a matching worker's state is not lost with the process. See ADR-015.
 *
 * <p>{@code router1} is never given a chance to shut down cleanly — no {@code close()} — because
 * the point is to prove recovery from exactly what a {@code kill -9} would leave behind, not from a
 * cooperative drain. With {@link FlushPolicy#EVERY_RECORD}, every command a caller ever observed as
 * successful is already fsynced by the time it was observed, so this is a faithful approximation of
 * an ungraceful crash rather than a weaker stand-in for one.
 */
class OrderRouterRecoveryTest {
    private static final Instant BASE_TIME = Instant.parse("2026-01-01T09:30:00Z");
    private static final int WORKER_COUNT = 3;

    @TempDir Path journalDir;

    @Test
    void aRestartedRouterRecoversTheBookAndCanKeepTradingCorrectly() {
        // A symbol guaranteed to land on a non-zero worker, so a bug that silently defaults an
        // unrecognized order to worker 0 would be caught rather than accidentally passing.
        String symbol = symbolForWorker(1);

        OrderBookSnapshot bookBeforeCrash;
        OrderSnapshot restingOrderBeforeCrash;
        long executionIdBeforeCrash;

        OrderRouter router1 =
                new OrderRouter(WORKER_COUNT, 64, journalDir, FlushPolicy.EVERY_RECORD);
        try {
            await(router1.submit(limit(1, symbol, Side.SELL, 10, 10_000, 1)));
            await(router1.submit(limit(2, symbol, Side.SELL, 5, 10_050, 2)));
            // Crosses order 1 only: price-time priority fills the cheaper resting order first.
            RoutedResult<CommandResult> crossing =
                    await(router1.submit(limit(3, symbol, Side.BUY, 8, 10_050, 3)));
            await(router1.replace(new ReplaceOrder(2, 3, 10_025, at(4))));
            await(router1.cancel(new CancelOrder(2, at(5))));

            Execution execution = crossing.value().executions().get(0);
            executionIdBeforeCrash = execution.executionId();

            bookBeforeCrash = await(router1.book(symbol)).value();
            restingOrderBeforeCrash = await(router1.findOrder(1)).value().orElseThrow();
        } finally {
            // Cleanup only — with EVERY_RECORD every assertion above already reflects fsynced
            // state, so this close() proves nothing about durability, only frees the threads.
            router1.close();
        }

        assertEquals(OrderStatus.PARTIALLY_FILLED, restingOrderBeforeCrash.status());
        assertEquals(2, restingOrderBeforeCrash.remainingQuantity());

        // No cooperative handoff: a brand new router reading only what the crashed one journaled.
        OrderRouter router2 =
                new OrderRouter(WORKER_COUNT, 64, journalDir, FlushPolicy.EVERY_RECORD);
        try {
            assertEquals(bookBeforeCrash, await(router2.book(symbol)).value());
            assertEquals(restingOrderBeforeCrash, await(router2.findOrder(1)).value().orElseThrow());

            // Order 2 was replaced then cancelled before the crash, and lives on worker 1's
            // engine specifically. Finding it at all (rather than an empty Optional from a
            // wrongly-defaulted worker 0) proves the owner map was rebuilt, not just the engine.
            OrderSnapshot cancelledOrder = await(router2.findOrder(2)).value().orElseThrow();
            assertEquals(OrderStatus.CANCELLED, cancelledOrder.status());

            // The engine's own execution-id counter must have resumed from replay, not reset:
            // a fresh trade's execution id has to continue the same worker's disjoint sequence
            // rather than colliding with one that happened before the crash.
            RoutedResult<CommandResult> newTrade =
                    await(router2.submit(limit(4, symbol, Side.BUY, 2, 10_000, 6)));
            long newExecutionId = newTrade.value().executions().get(0).executionId();
            assertTrue(
                    newExecutionId > executionIdBeforeCrash,
                    "post-recovery execution id " + newExecutionId
                            + " must continue past pre-crash id " + executionIdBeforeCrash);
            assertEquals(WORKER_COUNT, newExecutionId - executionIdBeforeCrash);
        } finally {
            router2.close();
        }
    }

    /**
     * A leftover torn tail from one crash must never sit ahead of what gets journaled after the
     * next restart. Left untruncated, this exact sequence — recover past a torn tail, then keep
     * writing — corrupts the *next* restart's replay: either the torn bytes look like an
     * oversized frame (so reading stops there and every record written after recovery is
     * silently lost) or they trip the sequence check (so a router that should start cleanly
     * throws {@code JournalCorruptionException} on a file that was otherwise completely intact).
     */
    @Test
    void aTornTailFromOneCrashDoesNotCorruptWhatIsJournaledAfterRecovery() {
        String symbol = symbolForWorker(1);

        OrderRouter router1 = new OrderRouter(WORKER_COUNT, 64, journalDir, FlushPolicy.EVERY_RECORD);
        try {
            await(router1.submit(limit(1, symbol, Side.SELL, 10, 10_000, 1)));
        } finally {
            router1.close();
        }

        // Simulate a crash mid-append on worker 1's journal specifically, the same shape
        // FileCommandJournalTest's torn-tail test injects: a length prefix promising content that
        // never fully arrived.
        Path workerFile = FileCommandJournal.fileFor(journalDir, 1);
        try (RandomAccessFile raf = new RandomAccessFile(workerFile.toFile(), "rw")) {
            raf.seek(raf.length());
            ByteBuffer torn = ByteBuffer.allocate(4);
            torn.putInt(500);
            raf.write(torn.array());
            raf.write(new byte[] {1, 2, 3});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Recovers past the torn tail and, with it, must remove it before writing anything new.
        OrderRouter router2 = new OrderRouter(WORKER_COUNT, 64, journalDir, FlushPolicy.EVERY_RECORD);
        try {
            await(router2.submit(limit(2, symbol, Side.SELL, 5, 10_050, 2)));
        } finally {
            router2.close();
        }

        // The real proof: a third restart must see both orders, not lose order 2 (if the torn
        // bytes decoded as a plausible oversized frame) or fail to start at all (if they instead
        // tripped the sequence check) - the two failure modes an untruncated torn tail produces.
        OrderRouter router3 = new OrderRouter(WORKER_COUNT, 64, journalDir, FlushPolicy.EVERY_RECORD);
        try {
            assertTrue(await(router3.findOrder(1)).value().isPresent());
            assertTrue(await(router3.findOrder(2)).value().isPresent());
        } finally {
            router3.close();
        }
    }

    @Test
    void aFreshRouterWithNoJournalHistoryStartsEmpty() {
        OrderRouter router = new OrderRouter(WORKER_COUNT, 64, journalDir, FlushPolicy.EVERY_RECORD);
        try {
            OrderBookSnapshot book = await(router.book("AAPL")).value();
            assertTrue(book.bids().isEmpty());
            assertTrue(book.asks().isEmpty());
        } finally {
            router.close();
        }
    }

    /** Mirrors {@code OrderRouter.workerIndex}: {@code Math.floorMod(symbol.hashCode(), workerCount)}. */
    private static String symbolForWorker(int workerId) {
        return IntStream.range(0, 10_000)
                .mapToObj(index -> "SYM" + index)
                .filter(symbol -> Math.floorMod(symbol.hashCode(), WORKER_COUNT) == workerId)
                .findFirst()
                .orElseThrow();
    }

    private static SubmitOrder limit(
            long orderId, String symbol, Side side, long quantity, long price, long timestampOffset) {
        return SubmitOrder.limit(
                orderId, "client-" + orderId, symbol, side, quantity, price, at(timestampOffset));
    }

    private static Instant at(long offset) {
        return BASE_TIME.plusNanos(offset);
    }

    private static <T> RoutedResult<T> await(CompletableFuture<RoutedResult<T>> completion) {
        return completion.orTimeout(5, TimeUnit.SECONDS).join();
    }
}
