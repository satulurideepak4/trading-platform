package com.tradingplatform.pipeline.events;

import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import java.time.Instant;

/**
 * The matching engine took the order. It may already be partially or fully filled, which is why the
 * resulting status travels with the event rather than being assumed to be NEW.
 */
public record OrderAccepted(
        String eventId,
        String correlationId,
        long orderId,
        String clientOrderId,
        String accountId,
        String strategyId,
        String symbol,
        Side side,
        OrderType orderType,
        OrderStatus status,
        long quantity,
        long remainingQuantity,
        long price,
        Instant occurredAt)
        implements TradingEvent {}
