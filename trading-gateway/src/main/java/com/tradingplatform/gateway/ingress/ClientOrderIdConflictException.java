package com.tradingplatform.gateway.ingress;

/**
 * Raised when a clientOrderId that already identifies an order is reused with different order
 * content. Treating that as a conflict rather than as a new order is what makes retries safe: the
 * gateway cannot tell a genuine retry from a bug, so it refuses the ambiguous case instead of
 * guessing.
 */
public final class ClientOrderIdConflictException extends RuntimeException {
    private final long existingOrderId;

    public ClientOrderIdConflictException(String clientOrderId, long existingOrderId) {
        super("clientOrderId " + clientOrderId + " already identifies order " + existingOrderId
                + " with different content");
        this.existingOrderId = existingOrderId;
    }

    public long existingOrderId() {
        return existingOrderId;
    }
}
