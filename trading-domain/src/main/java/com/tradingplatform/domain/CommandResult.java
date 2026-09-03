package com.tradingplatform.domain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record CommandResult(
        long orderId,
        boolean accepted,
        OrderStatus resultingStatus,
        Optional<OrderSnapshot> order,
        List<Execution> executions,
        Optional<RejectionReason> rejectionReason) {

    public CommandResult {
        Objects.requireNonNull(resultingStatus, "resultingStatus");
        order = Objects.requireNonNull(order, "order");
        executions = List.copyOf(executions);
        rejectionReason = Objects.requireNonNull(rejectionReason, "rejectionReason");
        if (accepted == rejectionReason.isPresent()) {
            throw new IllegalArgumentException("accepted results and rejection reasons are inconsistent");
        }
        if (!accepted && resultingStatus != OrderStatus.REJECTED) {
            throw new IllegalArgumentException("rejected commands must have REJECTED result status");
        }
    }

    public static CommandResult accepted(OrderSnapshot order, List<Execution> executions) {
        return new CommandResult(
                order.orderId(),
                true,
                order.status(),
                Optional.of(order),
                executions,
                Optional.empty());
    }

    public static CommandResult rejected(long orderId, RejectionReason reason) {
        return rejected(orderId, reason, Optional.empty());
    }

    public static CommandResult rejected(
            long orderId, RejectionReason reason, Optional<OrderSnapshot> existingOrder) {
        return new CommandResult(
                orderId,
                false,
                OrderStatus.REJECTED,
                existingOrder,
                List.of(),
                Optional.of(reason));
    }
}
