package com.tradingplatform.risk;

import com.tradingplatform.domain.Side;
import java.util.Objects;

/**
 * An order the risk engine still considers capable of trading. {@code effectivePrice} is the limit
 * price for limit orders and the instrument reference price for market orders, because a market
 * order has no price of its own to size a notional check against.
 */
public record WorkingOrder(
        long orderId, String symbol, Side side, long remainingQuantity, long effectivePrice) {

    public WorkingOrder {
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(side, "side");
        if (orderId <= 0) {
            throw new IllegalArgumentException("orderId must be positive");
        }
        if (remainingQuantity <= 0 || effectivePrice <= 0) {
            throw new IllegalArgumentException(
                    "working orders need a positive remaining quantity and price");
        }
    }

    WorkingOrder withRemainingQuantity(long newRemainingQuantity) {
        return new WorkingOrder(orderId, symbol, side, newRemainingQuantity, effectivePrice);
    }
}
