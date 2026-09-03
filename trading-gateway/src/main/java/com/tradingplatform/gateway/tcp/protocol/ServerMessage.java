package com.tradingplatform.gateway.tcp.protocol;

import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import com.tradingplatform.gateway.ingress.RejectionSource;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * One decoded (or about-to-be-encoded) server-to-client frame.
 *
 * <p>Deliberately its own types rather than reusing {@code OrderOutcome}/domain {@code Execution}
 * directly on the wire, for two reasons: those are server-internal ingress types a decoding client
 * has no reason to depend on, and the raw domain {@code Execution} carries both sides of a trade —
 * the counterparty's order id and client order id — which would leak one account's order identity
 * to the other side of their own fill. {@code OrderResponse.ExecutionResponse} (the REST path)
 * already makes this same choice for the same reason; {@link Ack#from} keeps the two consistent.
 */
public sealed interface ServerMessage {

    record AuthAck(String accountId) implements ServerMessage {}

    /** Sent, then the server closes the connection — there is no recovering a rejected auth. */
    record AuthReject(String reason) implements ServerMessage {}

    /** The response to one {@link ClientMessage.Submit}/{@code Cancel}/{@code Replace}. */
    record Ack(
            long requestId,
            long orderId,
            String clientOrderId,
            boolean duplicate,
            boolean accepted,
            Optional<AcceptedOrder> order,
            List<AckExecution> executions,
            Optional<Rejection> rejection)
            implements ServerMessage {

        /** Converts the real {@code OrderOutcome} the ingress pipeline produced into this wire
         * shape — the one place that conversion happens, so REST and TCP can never quietly drift
         * in what they consider safe to expose about a fill. */
        public static Ack from(long requestId, OrderOutcome outcome) {
            Optional<AcceptedOrder> order = outcome.order().map(AcceptedOrder::from);
            List<AckExecution> executions = outcome.executions().stream()
                    .map(execution -> AckExecution.from(execution, outcome.orderId()))
                    .toList();
            Optional<Rejection> rejection = outcome.rejection().map(Rejection::from);
            return new Ack(
                    requestId, outcome.orderId(), outcome.clientOrderId(), outcome.duplicate(),
                    outcome.accepted(), order, executions, rejection);
        }

        public record AcceptedOrder(
                String symbol,
                Side side,
                OrderType type,
                OrderStatus status,
                long quantity,
                long remainingQuantity,
                long price,
                Instant createdAt,
                Instant updatedAt) {

            static AcceptedOrder from(OrderSnapshot order) {
                return new AcceptedOrder(
                        order.symbol(), order.side(), order.type(), order.status(), order.quantity(),
                        order.remainingQuantity(), order.price(), order.createdAt(), order.updatedAt());
            }
        }

        /** @param maker true when the order this ack is about was already resting */
        public record AckExecution(
                long executionId, long price, long quantity, boolean maker, Instant timestamp) {

            static AckExecution from(Execution execution, long orderId) {
                return new AckExecution(
                        execution.executionId(), execution.price(), execution.quantity(),
                        execution.makerOrderId() == orderId, execution.timestamp());
            }
        }

        public record Rejection(RejectionSource source, String reason, String detail) {
            static Rejection from(OrderOutcome.Rejection rejection) {
                return new Rejection(rejection.source(), rejection.reason(), rejection.detail());
            }
        }
    }

    /**
     * Unsolicited: this connection's order was just filled by a command that arrived on a
     * different connection (or over REST). Scoped to this account's own side only — see {@link
     * Ack}'s Javadoc for why the counterparty's identity is never sent. Best-effort; see {@code
     * ExecutionPushingTradingEventPublisher}'s Javadoc for why that is an acceptable trade-off here.
     *
     * @param maker true when this connection's order was already resting
     */
    record ExecutionPush(
            long orderId,
            long executionId,
            String symbol,
            long price,
            long quantity,
            long remainingQuantity,
            boolean maker,
            Instant timestamp)
            implements ServerMessage {}

    /** A protocol-level problem not tied to any one request id. */
    record Error(String message) implements ServerMessage {}
}
