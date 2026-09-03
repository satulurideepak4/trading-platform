package com.tradingplatform.matching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.RejectionReason;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MatchingEngineTest {
    private static final Instant BASE_TIME = Instant.parse("2025-01-01T09:30:00Z");

    @Test
    void fullyFillsCrossingLimitOrdersAtRestingPrice() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 10, 10_000, 1));

        CommandResult result = engine.submit(limit(2, Side.BUY, 10, 10_100, 2));

        assertEquals(OrderStatus.FILLED, result.resultingStatus());
        assertEquals(OrderStatus.FILLED, engine.findOrder(1).orElseThrow().status());
        assertEquals(0, result.order().orElseThrow().remainingQuantity());
        assertEquals(1, result.executions().size());
        Execution execution = result.executions().getFirst();
        assertEquals(10_000, execution.price());
        assertEquals(10, execution.quantity());
        assertEquals(1, execution.makerOrderId());
        assertEquals(2, execution.takerOrderId());
        assertTrue(engine.book("AAPL").bids().isEmpty());
        assertTrue(engine.book("AAPL").asks().isEmpty());
    }

    @Test
    void partiallyFillsRestingOrderAndPreservesRemainder() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 10, 10_000, 1));

        CommandResult result = engine.submit(limit(2, Side.BUY, 4, 10_000, 2));

        assertEquals(OrderStatus.FILLED, result.resultingStatus());
        OrderSnapshot resting = engine.findOrder(1).orElseThrow();
        assertEquals(OrderStatus.PARTIALLY_FILLED, resting.status());
        assertEquals(6, resting.remainingQuantity());
        assertEquals(4, resting.executedQuantity());
        assertEquals(List.of(1L), orderIds(engine.book("AAPL").asks()));
    }

    @Test
    void matchesBestSellPricesBeforeWorsePrices() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 5, 10_100, 1));
        engine.submit(limit(2, Side.SELL, 5, 10_000, 2));
        engine.submit(limit(3, Side.SELL, 5, 10_200, 3));

        CommandResult result = engine.submit(market(4, Side.BUY, 12, 4));

        assertEquals(
                List.of(10_000L, 10_100L, 10_200L),
                result.executions().stream().map(Execution::price).toList());
        assertEquals(List.of(5L, 5L, 2L), quantities(result.executions()));
        assertEquals(OrderStatus.FILLED, result.resultingStatus());
        assertEquals(3, engine.book("AAPL").asks().getFirst().orderId());
        assertEquals(3, engine.book("AAPL").asks().getFirst().remainingQuantity());
    }

    @Test
    void matchesHighestBuyPricesBeforeLowerPrices() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.BUY, 5, 9_900, 1));
        engine.submit(limit(2, Side.BUY, 5, 10_100, 2));

        CommandResult result = engine.submit(market(3, Side.SELL, 7, 3));

        assertEquals(List.of(10_100L, 9_900L), result.executions().stream().map(Execution::price).toList());
        assertEquals(List.of(2L, 1L), result.executions().stream().map(Execution::makerOrderId).toList());
    }

    @Test
    void preservesArrivalPriorityWithinPriceLevel() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 10, 10_000, 1));
        engine.submit(limit(2, Side.SELL, 10, 10_000, 2));

        CommandResult result = engine.submit(market(3, Side.BUY, 15, 3));

        assertEquals(List.of(1L, 2L), result.executions().stream().map(Execution::makerOrderId).toList());
        assertEquals(OrderStatus.FILLED, engine.findOrder(1).orElseThrow().status());
        assertEquals(5, engine.findOrder(2).orElseThrow().remainingQuantity());
    }

    @Test
    void marketOrderConsumesAvailableLiquidityAndCancelsRemainder() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 4, 10_000, 1));

        CommandResult result = engine.submit(market(2, Side.BUY, 10, 2));

        OrderSnapshot marketOrder = result.order().orElseThrow();
        assertEquals(OrderStatus.CANCELLED, marketOrder.status());
        assertEquals(6, marketOrder.remainingQuantity());
        assertEquals(4, marketOrder.executedQuantity());
        assertTrue(engine.book("AAPL").bids().isEmpty());
    }

    @Test
    void marketOrderWithNoLiquidityIsCancelledWithoutExecution() {
        MatchingEngine engine = new MatchingEngine();

        CommandResult result = engine.submit(market(1, Side.BUY, 10, 1));

        assertEquals(OrderStatus.CANCELLED, result.resultingStatus());
        assertTrue(result.executions().isEmpty());
        assertEquals(10, result.order().orElseThrow().remainingQuantity());
        assertTrue(engine.book("AAPL").bids().isEmpty());
    }

    @Test
    void cancellationRemovesLiquidityAndPreventsExecution() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 10, 10_000, 1));

        CommandResult cancellation = engine.cancel(new CancelOrder(1, at(2)));
        CommandResult marketOrder = engine.submit(market(2, Side.BUY, 10, 3));

        assertEquals(OrderStatus.CANCELLED, cancellation.resultingStatus());
        assertTrue(engine.book("AAPL").asks().isEmpty());
        assertTrue(marketOrder.executions().isEmpty());
        assertEquals(OrderStatus.CANCELLED, engine.findOrder(1).orElseThrow().status());
    }

    @Test
    void replacementCanCrossTheSpread() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.BUY, 10, 9_900, 1));
        engine.submit(limit(2, Side.SELL, 10, 10_000, 2));

        CommandResult result = engine.replace(new ReplaceOrder(1, 10, 10_000, at(3)));

        assertEquals(OrderStatus.FILLED, result.resultingStatus());
        assertEquals(1, result.executions().size());
        assertEquals(2, result.executions().getFirst().makerOrderId());
        assertEquals(1, result.executions().getFirst().takerOrderId());
    }

    @Test
    void everyAcceptedReplacementLosesTimePriority() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.BUY, 10, 10_000, 1));
        engine.submit(limit(2, Side.BUY, 10, 10_000, 2));

        engine.replace(new ReplaceOrder(1, 10, 10_000, at(3)));
        CommandResult result = engine.submit(market(3, Side.SELL, 10, 4));

        assertEquals(2, result.executions().getFirst().makerOrderId());
        assertEquals(List.of(1L), orderIds(engine.book("AAPL").bids()));
    }

    @Test
    void replacementPreservesCumulativeExecutedQuantity() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 10, 10_000, 1));
        engine.submit(limit(2, Side.BUY, 4, 10_000, 2));

        CommandResult result = engine.replace(new ReplaceOrder(1, 12, 10_100, at(3)));

        OrderSnapshot replacement = result.order().orElseThrow();
        assertEquals(OrderStatus.PARTIALLY_FILLED, replacement.status());
        assertEquals(4, replacement.executedQuantity());
        assertEquals(8, replacement.remainingQuantity());
        assertEquals(12, replacement.quantity());
    }

    @Test
    void nonCrossingLimitOrderRestsWhenLiquidityIsInsufficient() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 10, 10_100, 1));

        CommandResult result = engine.submit(limit(2, Side.BUY, 10, 10_000, 2));

        assertEquals(OrderStatus.NEW, result.resultingStatus());
        assertTrue(result.executions().isEmpty());
        assertEquals(List.of(2L), orderIds(engine.book("AAPL").bids()));
        assertEquals(List.of(1L), orderIds(engine.book("AAPL").asks()));
    }

    @Test
    void rejectsZeroAndNegativeQuantities() {
        MatchingEngine engine = new MatchingEngine();

        CommandResult zero = engine.submit(limit(1, Side.BUY, 0, 10_000, 1));
        CommandResult negative = engine.submit(limit(2, Side.BUY, -1, 10_000, 2));

        assertRejected(zero, RejectionReason.INVALID_QUANTITY);
        assertRejected(negative, RejectionReason.INVALID_QUANTITY);
    }

    @Test
    void rejectsInvalidLimitAndMarketPrices() {
        MatchingEngine engine = new MatchingEngine();

        CommandResult limit = engine.submit(limit(1, Side.BUY, 1, 0, 1));
        CommandResult market = engine.submit(new SubmitOrder(
                2, "client-2", "AAPL", Side.BUY, OrderType.MARKET, 1, 10_000, at(2)));

        assertRejected(limit, RejectionReason.INVALID_PRICE);
        assertRejected(market, RejectionReason.INVALID_PRICE);
    }

    @Test
    void rejectsDuplicateOrderAndClientOrderIdsAcrossOrderLifetime() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.BUY, 10, 10_000, 1));
        engine.cancel(new CancelOrder(1, at(2)));

        CommandResult duplicateOrderId = engine.submit(
                SubmitOrder.limit(1, "different-client", "AAPL", Side.BUY, 1, 10_000, at(3)));
        CommandResult duplicateClientId = engine.submit(
                SubmitOrder.limit(2, "client-1", "AAPL", Side.BUY, 1, 10_000, at(4)));

        assertRejected(duplicateOrderId, RejectionReason.DUPLICATE_ORDER_ID);
        assertRejected(duplicateClientId, RejectionReason.DUPLICATE_CLIENT_ORDER_ID);
    }

    @Test
    void filledAndCancelledOrdersCannotBeActedOnAgain() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 5, 10_000, 1));
        engine.submit(limit(2, Side.BUY, 5, 10_000, 2));
        engine.submit(limit(3, Side.BUY, 5, 9_900, 3));
        engine.cancel(new CancelOrder(3, at(4)));

        assertRejected(
                engine.cancel(new CancelOrder(1, at(5))), RejectionReason.ORDER_NOT_ACTIVE);
        assertRejected(
                engine.replace(new ReplaceOrder(3, 5, 10_000, at(6))),
                RejectionReason.ORDER_NOT_ACTIVE);
    }

    @Test
    void rejectsReplacementBelowAlreadyExecutedQuantityWithoutChangingOrder() {
        MatchingEngine engine = new MatchingEngine();
        engine.submit(limit(1, Side.SELL, 10, 10_000, 1));
        engine.submit(limit(2, Side.BUY, 4, 10_000, 2));

        CommandResult result = engine.replace(new ReplaceOrder(1, 3, 10_100, at(3)));

        assertRejected(result, RejectionReason.REPLACEMENT_QUANTITY_BELOW_FILLED_QUANTITY);
        OrderSnapshot unchanged = engine.findOrder(1).orElseThrow();
        assertEquals(10, unchanged.quantity());
        assertEquals(6, unchanged.remainingQuantity());
        assertEquals(10_000, unchanged.price());
    }

    @Test
    void replayingTheSameCommandsProducesIdenticalResultsAndBook() {
        MatchingEngine first = new MatchingEngine();
        MatchingEngine replay = new MatchingEngine();

        List<CommandResult> firstResults = executeScenario(first);
        List<CommandResult> replayResults = executeScenario(replay);

        assertEquals(firstResults, replayResults);
        assertEquals(first.book("AAPL"), replay.book("AAPL"));
        for (long orderId = 1; orderId <= 5; orderId++) {
            assertEquals(first.findOrder(orderId), replay.findOrder(orderId));
        }
    }

    private static List<CommandResult> executeScenario(MatchingEngine engine) {
        List<CommandResult> results = new ArrayList<>();
        results.add(engine.submit(limit(1, Side.SELL, 7, 10_100, 1)));
        results.add(engine.submit(limit(2, Side.SELL, 8, 10_000, 2)));
        results.add(engine.submit(limit(3, Side.BUY, 10, 10_100, 3)));
        results.add(engine.replace(new ReplaceOrder(1, 9, 10_050, at(4))));
        results.add(engine.submit(market(4, Side.BUY, 2, 5)));
        results.add(engine.submit(limit(5, Side.BUY, 3, 9_900, 6)));
        results.add(engine.cancel(new CancelOrder(5, at(7))));
        return results;
    }

    private static SubmitOrder limit(
            long orderId, Side side, long quantity, long price, long timestampOffset) {
        return SubmitOrder.limit(
                orderId,
                "client-" + orderId,
                "AAPL",
                side,
                quantity,
                price,
                at(timestampOffset));
    }

    private static SubmitOrder market(
            long orderId, Side side, long quantity, long timestampOffset) {
        return SubmitOrder.market(
                orderId,
                "client-" + orderId,
                "AAPL",
                side,
                quantity,
                at(timestampOffset));
    }

    private static Instant at(long offset) {
        return BASE_TIME.plusNanos(offset);
    }

    private static List<Long> orderIds(List<OrderSnapshot> orders) {
        return orders.stream().map(OrderSnapshot::orderId).toList();
    }

    private static List<Long> quantities(List<Execution> executions) {
        return executions.stream().map(Execution::quantity).toList();
    }

    private static void assertRejected(CommandResult result, RejectionReason reason) {
        assertFalse(result.accepted());
        assertEquals(OrderStatus.REJECTED, result.resultingStatus());
        assertEquals(reason, result.rejectionReason().orElseThrow());
        assertTrue(result.executions().isEmpty());
    }
}
