package com.tradingplatform.domain;

import java.time.Instant;

/**
 * Untrusted new-order input. Validation belongs to the matching-engine boundary so invalid
 * commands produce deterministic rejection results instead of constructor exceptions.
 */
public record SubmitOrder(
        long orderId,
        String clientOrderId,
        String symbol,
        Side side,
        OrderType type,
        long quantity,
        long price,
        Instant timestamp) {

    public static SubmitOrder limit(
            long orderId,
            String clientOrderId,
            String symbol,
            Side side,
            long quantity,
            long price,
            Instant timestamp) {
        return new SubmitOrder(
                orderId, clientOrderId, symbol, side, OrderType.LIMIT, quantity, price, timestamp);
    }

    public static SubmitOrder market(
            long orderId,
            String clientOrderId,
            String symbol,
            Side side,
            long quantity,
            Instant timestamp) {
        return new SubmitOrder(
                orderId, clientOrderId, symbol, side, OrderType.MARKET, quantity, 0, timestamp);
    }
}
