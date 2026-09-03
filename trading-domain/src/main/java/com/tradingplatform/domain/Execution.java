package com.tradingplatform.domain;

import java.time.Instant;
import java.util.Objects;

public record Execution(
        long executionId,
        String symbol,
        long price,
        long quantity,
        long buyOrderId,
        String buyClientOrderId,
        long sellOrderId,
        String sellClientOrderId,
        long makerOrderId,
        long takerOrderId,
        long buyRemainingQuantity,
        long sellRemainingQuantity,
        Instant timestamp) {

    public Execution {
        if (executionId <= 0) {
            throw new IllegalArgumentException("executionId must be positive");
        }
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(buyClientOrderId, "buyClientOrderId");
        Objects.requireNonNull(sellClientOrderId, "sellClientOrderId");
        Objects.requireNonNull(timestamp, "timestamp");
        if (price <= 0 || quantity <= 0) {
            throw new IllegalArgumentException("execution price and quantity must be positive");
        }
        if (buyRemainingQuantity < 0 || sellRemainingQuantity < 0) {
            throw new IllegalArgumentException("remaining quantities cannot be negative");
        }
    }
}
