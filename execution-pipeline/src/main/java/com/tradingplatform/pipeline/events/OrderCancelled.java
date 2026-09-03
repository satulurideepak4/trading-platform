package com.tradingplatform.pipeline.events;

import java.time.Instant;

/**
 * @param remainingQuantity the quantity that was still working and has now been withdrawn
 */
public record OrderCancelled(
        String eventId,
        String correlationId,
        long orderId,
        String clientOrderId,
        String accountId,
        String strategyId,
        String symbol,
        long remainingQuantity,
        Instant occurredAt)
        implements TradingEvent {}
