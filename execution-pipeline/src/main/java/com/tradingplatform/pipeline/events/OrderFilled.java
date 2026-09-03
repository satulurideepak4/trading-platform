package com.tradingplatform.pipeline.events;

import com.tradingplatform.domain.Side;
import java.time.Instant;

/**
 * An order reached its terminal filled state.
 *
 * <p>A lifecycle milestone, not a trade: the money moved in the {@link ExecutionCreated} events that
 * led here. It exists so that a consumer tracking order state does not have to accumulate fills to
 * notice an order is done.
 *
 * <p>It deliberately carries no quantity. The event is derived from the execution that closed the
 * order out, which knows its own size but not the order's total, and a field that is only sometimes
 * the whole story is worse than no field. {@code closingExecutionId} points at the trade that
 * finished it; the executions stream holds the full history.
 */
public record OrderFilled(
        String eventId,
        String correlationId,
        long orderId,
        String clientOrderId,
        String accountId,
        String strategyId,
        String symbol,
        Side side,
        long closingExecutionId,
        Instant occurredAt)
        implements TradingEvent {}
