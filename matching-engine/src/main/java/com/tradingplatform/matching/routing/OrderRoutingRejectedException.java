package com.tradingplatform.matching.routing;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;

public final class OrderRoutingRejectedException extends RejectedExecutionException {
    private final RoutingRejectionReason reason;
    private final int workerId;

    public OrderRoutingRejectedException(
            RoutingRejectionReason reason, int workerId, String message) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
        this.workerId = workerId;
    }

    public RoutingRejectionReason reason() {
        return reason;
    }

    public int workerId() {
        return workerId;
    }
}
