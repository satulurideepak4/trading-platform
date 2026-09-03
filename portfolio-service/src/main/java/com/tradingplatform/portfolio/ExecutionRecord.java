package com.tradingplatform.portfolio;

import java.time.Instant;

/** One stored trade, as both sides see it. */
public record ExecutionRecord(
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
        Instant occurredAt) {}
