package com.tradingplatform.pipeline.events;

import java.time.Instant;

/**
 * A trade. This is the only event that moves money, so it carries both sides in full: a consumer
 * must never have to join two records to know who traded what.
 *
 * <p>It is also the reason the partition key is the symbol rather than the account. An execution
 * belongs to two accounts and exactly one instrument, so the symbol is the only key that is
 * well defined for it. See ADR-007.
 *
 * <p>Each side carries its own strategy. Positions are reported per account, strategy and
 * instrument, and the two sides of a trade are unrelated books, so a single strategy field on the
 * trade would be meaningless.
 */
public record ExecutionCreated(
        String eventId,
        String correlationId,
        long executionId,
        String symbol,
        long price,
        long quantity,
        long buyOrderId,
        String buyAccountId,
        String buyStrategyId,
        long sellOrderId,
        String sellAccountId,
        String sellStrategyId,
        long makerOrderId,
        long takerOrderId,
        long buyRemainingQuantity,
        long sellRemainingQuantity,
        Instant occurredAt)
        implements TradingEvent {}
