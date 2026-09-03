package com.tradingplatform.risk;

import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import java.util.Objects;

/**
 * One order to be risk-checked. Shape validation has already happened upstream, so anything
 * structurally impossible here is a programming error rather than a rejection.
 *
 * <p>{@code price} is ignored for market orders; the engine substitutes the instrument reference
 * price.
 */
public record RiskCheckRequest(
        long orderId, String symbol, Side side, OrderType type, long quantity, long price) {

    public RiskCheckRequest {
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(type, "type");
        if (orderId <= 0) {
            throw new IllegalArgumentException("orderId must be positive");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        if (type == OrderType.LIMIT && price <= 0) {
            throw new IllegalArgumentException("limit orders need a positive price");
        }
    }
}
