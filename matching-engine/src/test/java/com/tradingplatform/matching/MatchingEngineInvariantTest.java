package com.tradingplatform.matching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * General, randomized invariant checks for {@link MatchingEngine}, replacing "every invariant only
 * has a fixed hand-picked example" with assertions that must hold across many generated command
 * sequences. There is no property-based testing library in this codebase (see
 * {@code portfolio-service}'s {@code PositionStateTest.aRandomRoundTripRealizesExactlyProceedsMinusCost()}
 * for the established precedent); this class follows the same hand-rolled, fixed-seed style so a
 * failure is reproducible just by rerunning this file.
 *
 * <p>Covers the Master-Prompt's required invariants:
 *
 * <ul>
 *   <li>executed quantity can never exceed order quantity
 *   <li>order remaining quantity cannot become negative
 *   <li>cancelled orders cannot execute afterward
 *   <li>filled orders cannot return to active state
 *   <li>bid/ask ordering remains valid
 *   <li>replaying identical journal/events recreates identical state
 * </ul>
 *
 * "Each execution affects positions exactly once" is deliberately out of scope here: the matching
 * engine has no position concept. It is covered by {@code PortfolioUpdaterTest} (portfolio-service),
 * {@code ExecutionPipelineIntegrationTest} (trading-gateway) and {@code ExecutionsDedupTest}
 * (execution-pipeline).
 */
class MatchingEngineInvariantTest {
    private static final String SYMBOL = "AAPL";
    private static final Instant BASE_TIME = Instant.parse("2026-08-22T09:30:00Z");
    private static final int TRIALS = 200;
    private static final int COMMANDS_PER_TRIAL = 80;
    private static final int REPLAY_TRIALS = 50;

    @Test
    void quantityStatusAndBookOrderingInvariantsHoldAcrossRandomizedCommandSequences() {
        for (int trial = 0; trial < TRIALS; trial++) {
            long seed = 20260822_000_000L + trial;
            List<GeneratedCommand> commands = generateTrial(seed, COMMANDS_PER_TRIAL);
            checkInvariantsAcrossTrial(trial, commands);
        }
    }

    @Test
    void replayingRandomizedCommandSequencesProducesIdenticalResultsAndBookState() {
        for (int trial = 0; trial < REPLAY_TRIALS; trial++) {
            long seed = 20260822_500_000L + trial;
            List<GeneratedCommand> commands = generateTrial(seed, COMMANDS_PER_TRIAL);

            MatchingEngine first = new MatchingEngine();
            List<CommandResult> firstResults = applyAll(first, commands);
            MatchingEngine second = new MatchingEngine();
            List<CommandResult> secondResults = applyAll(second, commands);

            assertEquals(
                    firstResults,
                    secondResults,
                    "trial " + trial + " (seed " + seed + ") replay produced different command results");
            assertEquals(
                    first.book(SYMBOL),
                    second.book(SYMBOL),
                    "trial " + trial + " (seed " + seed + ") replay produced a different final book");
            for (long orderId : orderIdsIn(commands)) {
                assertEquals(
                        first.findOrder(orderId),
                        second.findOrder(orderId),
                        "trial " + trial + " (seed " + seed + ") replay diverged for order " + orderId);
            }
        }
    }

    // --- invariant checking -------------------------------------------------------------------

    private void checkInvariantsAcrossTrial(int trialIndex, List<GeneratedCommand> commands) {
        MatchingEngine engine = new MatchingEngine();
        Map<Long, OrderStatus> terminalStatusSeen = new HashMap<>();
        List<Long> knownOrderIds = new ArrayList<>();

        for (int commandIndex = 0; commandIndex < commands.size(); commandIndex++) {
            GeneratedCommand command = commands.get(commandIndex);
            apply(engine, command);
            if (command instanceof GeneratedCommand.Submit submit) {
                knownOrderIds.add(submit.order().orderId());
            }

            for (long orderId : knownOrderIds) {
                OrderSnapshot snapshot = engine.findOrder(orderId).orElseThrow();
                String where = "trial " + trialIndex + " command " + commandIndex + " order " + orderId;

                assertTrue(
                        snapshot.remainingQuantity() >= 0,
                        where + ": remaining quantity went negative (" + snapshot.remainingQuantity() + ")");
                assertTrue(
                        snapshot.remainingQuantity() <= snapshot.quantity(),
                        where + ": executed quantity exceeded order quantity (remaining="
                                + snapshot.remainingQuantity() + " quantity=" + snapshot.quantity() + ")");

                if (snapshot.status() == OrderStatus.CANCELLED || snapshot.status() == OrderStatus.FILLED) {
                    OrderStatus previousTerminal = terminalStatusSeen.putIfAbsent(orderId, snapshot.status());
                    if (previousTerminal != null) {
                        assertEquals(
                                previousTerminal,
                                snapshot.status(),
                                where + ": terminal status changed from " + previousTerminal
                                        + " to " + snapshot.status());
                    }
                } else {
                    OrderStatus previousTerminal = terminalStatusSeen.get(orderId);
                    assertNull(
                            previousTerminal,
                            where + ": order reverted from terminal status " + previousTerminal
                                    + " back to " + snapshot.status());
                }
            }

            OrderBookSnapshot book = engine.book(SYMBOL);
            String where = "trial " + trialIndex + " command " + commandIndex;
            assertNonIncreasingByPrice(book.bids(), where + " bids");
            assertNonDecreasingByPrice(book.asks(), where + " asks");
            if (!book.bids().isEmpty() && !book.asks().isEmpty()) {
                assertTrue(
                        book.bids().getFirst().price() < book.asks().getFirst().price(),
                        where + ": book is crossed, best bid " + book.bids().getFirst().price()
                                + " >= best ask " + book.asks().getFirst().price());
            }
        }
    }

    private static void assertNonIncreasingByPrice(List<OrderSnapshot> levels, String where) {
        for (int i = 1; i < levels.size(); i++) {
            assertTrue(
                    levels.get(i - 1).price() >= levels.get(i).price(),
                    where + ": not non-increasing at index " + i + " (" + levels.get(i - 1).price()
                            + " then " + levels.get(i).price() + ")");
        }
    }

    private static void assertNonDecreasingByPrice(List<OrderSnapshot> levels, String where) {
        for (int i = 1; i < levels.size(); i++) {
            assertTrue(
                    levels.get(i - 1).price() <= levels.get(i).price(),
                    where + ": not non-decreasing at index " + i + " (" + levels.get(i - 1).price()
                            + " then " + levels.get(i).price() + ")");
        }
    }

    // --- generation -----------------------------------------------------------------------------

    /** One command in a generated trial, carrying enough type information to replay it verbatim. */
    private sealed interface GeneratedCommand {
        record Submit(SubmitOrder order) implements GeneratedCommand {}

        record Cancel(CancelOrder cancel) implements GeneratedCommand {}

        record Replace(ReplaceOrder replace) implements GeneratedCommand {}
    }

    /**
     * Generates a bounded, seeded sequence of commands against a local model of which order ids
     * exist so far (chosen uniformly among all ids ever submitted, active or not, so
     * cancel-after-fill and cancel-after-cancel rejection paths get exercised too, not just the
     * happy path).
     */
    private static List<GeneratedCommand> generateTrial(long seed, int count) {
        SplittableRandom random = new SplittableRandom(seed);
        List<GeneratedCommand> commands = new ArrayList<>(count);
        List<Long> knownOrderIds = new ArrayList<>();
        long nextOrderId = 1;

        for (int i = 0; i < count; i++) {
            Instant timestamp = BASE_TIME.plusNanos(i);
            double roll = random.nextDouble();

            if (knownOrderIds.isEmpty() || roll < 0.6) {
                long orderId = nextOrderId++;
                Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
                long quantity = 1 + random.nextInt(20);
                boolean market = random.nextInt(10) == 0;
                SubmitOrder order = market
                        ? SubmitOrder.market(orderId, "c-" + orderId, SYMBOL, side, quantity, timestamp)
                        : SubmitOrder.limit(
                                orderId, "c-" + orderId, SYMBOL, side, quantity, randomPrice(random), timestamp);
                commands.add(new GeneratedCommand.Submit(order));
                knownOrderIds.add(orderId);
            } else if (roll < 0.8) {
                long targetId = knownOrderIds.get(random.nextInt(knownOrderIds.size()));
                commands.add(new GeneratedCommand.Cancel(new CancelOrder(targetId, timestamp)));
            } else {
                long targetId = knownOrderIds.get(random.nextInt(knownOrderIds.size()));
                long newQuantity = 1 + random.nextInt(20);
                commands.add(new GeneratedCommand.Replace(
                        new ReplaceOrder(targetId, newQuantity, randomPrice(random), timestamp)));
            }
        }
        return commands;
    }

    /** A tight band around one mid so generated limit orders actually cross often. */
    private static long randomPrice(SplittableRandom random) {
        return 9_800 + random.nextInt(41) * 10L;
    }

    private static List<Long> orderIdsIn(List<GeneratedCommand> commands) {
        List<Long> ids = new ArrayList<>();
        for (GeneratedCommand command : commands) {
            if (command instanceof GeneratedCommand.Submit submit) {
                ids.add(submit.order().orderId());
            }
        }
        return ids;
    }

    private static List<CommandResult> applyAll(MatchingEngine engine, List<GeneratedCommand> commands) {
        List<CommandResult> results = new ArrayList<>(commands.size());
        for (GeneratedCommand command : commands) {
            results.add(apply(engine, command));
        }
        return results;
    }

    private static CommandResult apply(MatchingEngine engine, GeneratedCommand command) {
        return switch (command) {
            case GeneratedCommand.Submit submit -> engine.submit(submit.order());
            case GeneratedCommand.Cancel cancel -> engine.cancel(cancel.cancel());
            case GeneratedCommand.Replace replace -> engine.replace(replace.replace());
        };
    }
}
