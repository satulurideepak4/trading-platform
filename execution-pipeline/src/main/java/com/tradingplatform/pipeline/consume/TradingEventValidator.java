package com.tradingplatform.pipeline.consume;

import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;

/**
 * Rejects records that deserialized but cannot be true.
 *
 * <p>A consumer cannot assume the producer was this application. Someone can write to the topic by
 * hand, an older producer can still be running during a deploy, and a partially rolled back schema
 * change can put a field in a state no current code produces. Validating at the consumer is what
 * turns those into one dead-lettered record instead of a poisoned partition.
 */
public final class TradingEventValidator {

    private TradingEventValidator() {}

    public static void requireUsable(TradingEvent event) {
        if (event == null) {
            throw new InvalidEventException("record deserialized to no event at all");
        }
        requireText(event.eventId(), "eventId");
        requireText(event.symbol(), "symbol");
        if (event.occurredAt() == null) {
            throw new InvalidEventException(event.eventId() + " has no occurredAt");
        }
        if (event instanceof ExecutionCreated execution) {
            requireExecutionUsable(execution);
        }
    }

    private static void requireExecutionUsable(ExecutionCreated execution) {
        if (execution.executionId() <= 0) {
            throw new InvalidEventException(
                    "execution id must be positive but was " + execution.executionId());
        }
        if (execution.quantity() <= 0 || execution.price() <= 0) {
            throw new InvalidEventException(
                    "execution " + execution.executionId() + " has a non-positive price or quantity");
        }
        if (execution.buyRemainingQuantity() < 0 || execution.sellRemainingQuantity() < 0) {
            throw new InvalidEventException(
                    "execution " + execution.executionId() + " has a negative remaining quantity");
        }
        requireText(execution.buyAccountId(), "buyAccountId");
        requireText(execution.sellAccountId(), "sellAccountId");
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new InvalidEventException(field + " is required");
        }
    }
}
