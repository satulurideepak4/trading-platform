package com.tradingplatform.gateway.ingress;

import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import java.util.List;
import java.util.Optional;

/**
 * What the gateway decided about one command, in a form that can be stored under an idempotency key
 * and replayed verbatim to a retry.
 *
 * @param duplicate true when this outcome is being replayed to a repeated request rather than
 *     produced by it
 */
public record OrderOutcome(
        long orderId,
        String clientOrderId,
        Optional<OrderSnapshot> order,
        List<Execution> executions,
        Optional<Rejection> rejection,
        boolean duplicate) {

    public OrderOutcome {
        executions = List.copyOf(executions);
    }

    public boolean accepted() {
        return rejection.isEmpty();
    }

    public static OrderOutcome accepted(
            String clientOrderId, OrderSnapshot order, List<Execution> executions) {
        return new OrderOutcome(
                order.orderId(),
                clientOrderId,
                Optional.of(order),
                executions,
                Optional.empty(),
                false);
    }

    public static OrderOutcome rejected(
            long orderId, String clientOrderId, RejectionSource source, String reason, String detail) {
        return new OrderOutcome(
                orderId,
                clientOrderId,
                Optional.empty(),
                List.of(),
                Optional.of(new Rejection(source, reason, detail)),
                false);
    }

    OrderOutcome asDuplicate() {
        return new OrderOutcome(orderId, clientOrderId, order, executions, rejection, true);
    }

    public record Rejection(RejectionSource source, String reason, String detail) {}
}
