package com.tradingplatform.pipeline.events;

import java.time.Instant;

/**
 * @param prioritySequence the engine's new time-priority position; a replacement always loses its
 *     old place in the queue, and this is the evidence of it
 */
public record OrderReplaced(
        String eventId,
        String correlationId,
        long orderId,
        String clientOrderId,
        String accountId,
        String strategyId,
        String symbol,
        long newQuantity,
        long newPrice,
        long remainingQuantity,
        long prioritySequence,
        Instant occurredAt)
        implements TradingEvent {}
