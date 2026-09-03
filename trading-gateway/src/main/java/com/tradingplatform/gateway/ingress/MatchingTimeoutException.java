package com.tradingplatform.gateway.ingress;

/**
 * The command was admitted to a matching worker but no outcome arrived within the gateway timeout.
 *
 * <p>This is explicitly not a statement that the command failed. The worker still owns it and will
 * still process it, and the idempotency registration is deliberately left in place so a retry with
 * the same clientOrderId observes the real outcome instead of creating a second order.
 */
public final class MatchingTimeoutException extends RuntimeException {
    private final long orderId;

    public MatchingTimeoutException(long orderId) {
        super("no matching outcome for order " + orderId + " within the gateway timeout");
        this.orderId = orderId;
    }

    public long orderId() {
        return orderId;
    }
}
