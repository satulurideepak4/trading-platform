package com.tradingplatform.pipeline.consume;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.OrderAccepted;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Every branch a redelivered-but-corrupted or hand-crafted record can hit before it is applied.
 * See {@link TradingEventValidator}'s own Javadoc: this is what turns a bad record into one
 * dead-lettered record instead of a poisoned partition.
 */
class TradingEventValidatorTest {
    private static final Instant OCCURRED_AT = Instant.parse("2026-08-22T09:30:00Z");

    @Test
    void nullEventIsRejected() {
        assertThrows(InvalidEventException.class, () -> TradingEventValidator.requireUsable(null));
    }

    @Test
    void blankEventIdIsRejected() {
        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(execution(" ", "AAPL", 1, 10, 100, 0, 0)));
    }

    @Test
    void blankSymbolIsRejected() {
        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(execution("exe-1", "", 1, 10, 100, 0, 0)));
    }

    @Test
    void missingOccurredAtIsRejected() {
        ExecutionCreated withoutTimestamp = new ExecutionCreated(
                "exe-1", "corr-1", 1, "AAPL", 10_000, 10, 1, "acc-buy", "strat-buy", 2, "acc-sell",
                "strat-sell", 1, 2, 0, 0, null);

        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(withoutTimestamp));
    }

    @Test
    void nonExecutionEventOnlySkipsExecutionSpecificChecks() {
        OrderAccepted accepted = new OrderAccepted(
                "acc-1", "corr-1", 1, "client-1", "acc-buy", "strat-buy", "AAPL", Side.BUY,
                OrderType.LIMIT, OrderStatus.NEW, 10, 10, 10_000, OCCURRED_AT);

        assertDoesNotThrow(() -> TradingEventValidator.requireUsable(accepted));
    }

    @Test
    void nonPositiveExecutionIdIsRejected() {
        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(execution("exe-1", "AAPL", 0, 10, 100, 0, 0)));
    }

    @Test
    void nonPositiveQuantityIsRejected() {
        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(execution("exe-1", "AAPL", 1, 0, 100, 0, 0)));
    }

    @Test
    void nonPositivePriceIsRejected() {
        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(execution("exe-1", "AAPL", 1, 10, 0, 0, 0)));
    }

    @Test
    void negativeBuyRemainingQuantityIsRejected() {
        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(execution("exe-1", "AAPL", 1, 10, 100, -1, 0)));
    }

    @Test
    void negativeSellRemainingQuantityIsRejected() {
        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(execution("exe-1", "AAPL", 1, 10, 100, 0, -1)));
    }

    @Test
    void blankBuyAccountIdIsRejected() {
        ExecutionCreated blankBuyAccount = new ExecutionCreated(
                "exe-1", "corr-1", 1, "AAPL", 10_000, 10, 1, " ", "strat-buy", 2, "acc-sell",
                "strat-sell", 1, 2, 0, 0, OCCURRED_AT);

        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(blankBuyAccount));
    }

    @Test
    void blankSellAccountIdIsRejected() {
        ExecutionCreated blankSellAccount = new ExecutionCreated(
                "exe-1", "corr-1", 1, "AAPL", 10_000, 10, 1, "acc-buy", "strat-buy", 2, "",
                "strat-sell", 1, 2, 0, 0, OCCURRED_AT);

        assertThrows(
                InvalidEventException.class,
                () -> TradingEventValidator.requireUsable(blankSellAccount));
    }

    @Test
    void aWellFormedExecutionCreatedPassesValidation() {
        assertDoesNotThrow(
                () -> TradingEventValidator.requireUsable(execution("exe-1", "AAPL", 1, 10, 100, 0, 0)));
    }

    private static ExecutionCreated execution(
            String eventId,
            String symbol,
            long executionId,
            long quantity,
            long price,
            long buyRemainingQuantity,
            long sellRemainingQuantity) {
        return new ExecutionCreated(
                eventId,
                "corr-1",
                executionId,
                symbol,
                price,
                quantity,
                1,
                "acc-buy",
                "strat-buy",
                2,
                "acc-sell",
                "strat-sell",
                1,
                2,
                buyRemainingQuantity,
                sellRemainingQuantity,
                OCCURRED_AT);
    }
}
