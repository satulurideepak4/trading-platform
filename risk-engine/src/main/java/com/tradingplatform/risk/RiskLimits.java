package com.tradingplatform.risk;

/**
 * Limits applied to a single account. Quantities are whole units and prices are integer ticks, so
 * notional is also an integer and needs no rounding rules.
 */
public record RiskLimits(
        long maxOrderQuantity,
        long maxOrderNotional,
        int maxOpenOrders,
        long maxPositionQuantity) {

    public RiskLimits {
        if (maxOrderQuantity <= 0
                || maxOrderNotional <= 0
                || maxOpenOrders <= 0
                || maxPositionQuantity <= 0) {
            throw new IllegalArgumentException("all risk limits must be positive");
        }
    }
}
