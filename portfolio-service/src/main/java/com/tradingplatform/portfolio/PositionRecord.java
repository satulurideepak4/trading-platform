package com.tradingplatform.portfolio;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

/** A stored position plus the values derived from a mark price. */
public record PositionRecord(
        PositionKey key,
        PositionState state,
        Long lastExecutionId,
        Instant updatedAt) {

    public Optional<BigDecimal> averageEntryPrice() {
        return state.averageEntryPrice();
    }

    public long unrealizedPnl(long markPrice) {
        return state.unrealizedPnl(markPrice);
    }
}
