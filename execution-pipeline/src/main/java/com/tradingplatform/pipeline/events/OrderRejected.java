package com.tradingplatform.pipeline.events;

import java.time.Instant;

/**
 * The order never became live. Rejections are published because an audit trail that only records
 * what succeeded cannot answer why a client's order did not trade.
 *
 * @param rejectionSource which layer refused it, mirroring the gateway's HTTP response
 */
public record OrderRejected(
        String eventId,
        String correlationId,
        long orderId,
        String clientOrderId,
        String accountId,
        String strategyId,
        String symbol,
        String rejectionSource,
        String rejectionReason,
        String detail,
        Instant occurredAt)
        implements TradingEvent {}
