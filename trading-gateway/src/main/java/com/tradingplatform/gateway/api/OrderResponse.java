package com.tradingplatform.gateway.api;

import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import java.time.Instant;
import java.util.List;

/**
 * @param clientOrderId the identifier the client supplied, not the account-qualified form the
 *     matching engine stores
 * @param duplicate true when this response is a replay of an earlier identical request
 */
public record OrderResponse(
        long orderId,
        String clientOrderId,
        String symbol,
        Side side,
        OrderType type,
        OrderStatus status,
        long quantity,
        long remainingQuantity,
        long executedQuantity,
        long price,
        Instant createdAt,
        Instant updatedAt,
        List<ExecutionResponse> executions,
        boolean duplicate) {

    public static OrderResponse from(OrderOutcome outcome) {
        OrderSnapshot order = outcome.order().orElseThrow(
                () -> new IllegalStateException("accepted outcomes always carry an order"));
        return new OrderResponse(
                order.orderId(),
                outcome.clientOrderId(),
                order.symbol(),
                order.side(),
                order.type(),
                order.status(),
                order.quantity(),
                order.remainingQuantity(),
                order.executedQuantity(),
                order.price(),
                order.createdAt(),
                order.updatedAt(),
                outcome.executions().stream()
                        .map(execution -> ExecutionResponse.from(execution, order.orderId()))
                        .toList(),
                outcome.duplicate());
    }

    /**
     * @param liquidity MAKER when this order was already resting, TAKER when it was the aggressor.
     *     Venues price the two differently, so clients need it per fill.
     */
    public record ExecutionResponse(
            long executionId, long price, long quantity, String liquidity, Instant timestamp) {

        static ExecutionResponse from(Execution execution, long orderId) {
            return new ExecutionResponse(
                    execution.executionId(),
                    execution.price(),
                    execution.quantity(),
                    execution.makerOrderId() == orderId ? "MAKER" : "TAKER",
                    execution.timestamp());
        }
    }
}
