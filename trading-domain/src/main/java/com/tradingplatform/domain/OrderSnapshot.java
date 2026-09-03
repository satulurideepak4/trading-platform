package com.tradingplatform.domain;

import java.time.Instant;

public record OrderSnapshot(
        long orderId,
        String clientOrderId,
        String symbol,
        Side side,
        OrderType type,
        OrderStatus status,
        long quantity,
        long remainingQuantity,
        long price,
        Instant createdAt,
        Instant updatedAt,
        long prioritySequence) {

    public long executedQuantity() {
        return quantity - remainingQuantity;
    }
}
