package com.tradingplatform.gateway.ingress;

/**
 * Also thrown when the order exists but belongs to another account. Answering "not found" rather
 * than "forbidden" keeps the gateway from confirming that a given order id exists to a caller who
 * is not entitled to know.
 */
public final class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(long orderId) {
        super("no order " + orderId + " for this account");
    }
}
