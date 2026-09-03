package com.tradingplatform.gateway.protobuf;

import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.CorrelationId;
import com.tradingplatform.gateway.api.SubmitOrderRequest;
import com.tradingplatform.gateway.ingress.OrderIngressService;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderContent;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.ExecutionProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderAckProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderStatusProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderTypeProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.RejectionProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SideProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SubmitOrderProto;

/**
 * Converts between the generated Protobuf messages and this gateway's own domain/ingress types —
 * the same job {@code SubmitOrderRequest.toOrderContent()}/{@code OrderResponse.from()} do for the
 * JSON path, kept in one place for the same reason: two independent, hand-written conversions for
 * the same shape are exactly how a REST/Protobuf response drift apart un-noticed.
 */
public final class ProtobufOrderMapper {
    private ProtobufOrderMapper() {}

    public static OrderContent toOrderContent(SubmitOrderProto request) {
        return new OrderContent(
                request.getSymbol(),
                toSide(request.getSide()),
                toOrderType(request.getType()),
                request.getQuantity(),
                request.getPrice(),
                resolvedStrategyId(request.getStrategyId()));
    }

    /** Matches {@code SubmitOrderRequest.resolvedStrategyId()} exactly - proto3 has no null, only
     * an empty-string default for an unset field, but "no strategy id was given" has to resolve to
     * {@code DEFAULT} the same way on both paths, or the same logical request produces two
     * different position/P&L attributions depending only on which encoding a client happened to
     * use, an inconsistency this mapper exists specifically to prevent. Found by a real end-to-end
     * run: a Protobuf submission left strategyId as protobuf's own empty-string default, which a
     * REST/JSON client's null instead resolves to DEFAULT - the same clientOrderId submitted once
     * on each path then legitimately triggered ClientOrderIdConflictException, since the two
     * OrderContent values genuinely differed. */
    private static String resolvedStrategyId(String strategyId) {
        return strategyId == null || strategyId.isBlank() ? SubmitOrderRequest.DEFAULT_STRATEGY : strategyId;
    }

    /** Mirrors {@link OrderIngressService}'s callers: an accepted or rejected outcome both convert
     * to one self-contained {@code OrderAckProto}, never two different message types. */
    public static OrderAckProto toAck(OrderOutcome outcome) {
        OrderAckProto.Builder builder = OrderAckProto.newBuilder()
                .setOrderId(outcome.orderId())
                .setClientOrderId(outcome.clientOrderId())
                .setDuplicate(outcome.duplicate())
                .setAccepted(outcome.accepted());
        if (outcome.accepted()) {
            OrderSnapshot order = outcome.order().orElseThrow(
                    () -> new IllegalStateException("accepted outcomes always carry an order"));
            builder.setSymbol(order.symbol())
                    .setSide(fromSide(order.side()))
                    .setType(fromOrderType(order.type()))
                    .setStatus(fromOrderStatus(order.status()))
                    .setQuantity(order.quantity())
                    .setRemainingQuantity(order.remainingQuantity())
                    .setExecutedQuantity(order.executedQuantity())
                    .setPrice(order.price())
                    .setCreatedAtEpochMillis(order.createdAt().toEpochMilli())
                    .setUpdatedAtEpochMillis(order.updatedAt().toEpochMilli());
            for (Execution execution : outcome.executions()) {
                builder.addExecutions(toExecutionProto(execution, order.orderId()));
            }
        } else {
            OrderOutcome.Rejection rejection = outcome.rejection().orElseThrow(
                    () -> new IllegalStateException("rejected outcomes always carry a rejection"));
            builder.setRejection(RejectionProto.newBuilder()
                    .setSource(rejection.source().name())
                    .setReason(rejection.reason())
                    .setMessage(rejection.detail())
                    .setCorrelationId(CorrelationId.current())
                    .build());
        }
        return builder.build();
    }

    /** @param orderId whose side's perspective this execution is reported from — the same scoping
     * {@code OrderResponse.ExecutionResponse.from} already applies, so the counterparty's own order
     * id is never part of either encoding. */
    private static ExecutionProto toExecutionProto(Execution execution, long orderId) {
        return ExecutionProto.newBuilder()
                .setExecutionId(execution.executionId())
                .setPrice(execution.price())
                .setQuantity(execution.quantity())
                .setLiquidity(execution.makerOrderId() == orderId ? "MAKER" : "TAKER")
                .setTimestampEpochMillis(execution.timestamp().toEpochMilli())
                .build();
    }

    private static Side toSide(SideProto side) {
        return switch (side) {
            case BUY -> Side.BUY;
            case SELL -> Side.SELL;
            case SIDE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("side is required");
        };
    }

    private static SideProto fromSide(Side side) {
        return switch (side) {
            case BUY -> SideProto.BUY;
            case SELL -> SideProto.SELL;
        };
    }

    private static OrderType toOrderType(OrderTypeProto type) {
        return switch (type) {
            case LIMIT -> OrderType.LIMIT;
            case MARKET -> OrderType.MARKET;
            case ORDER_TYPE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("order type is required");
        };
    }

    private static OrderTypeProto fromOrderType(OrderType type) {
        return switch (type) {
            case LIMIT -> OrderTypeProto.LIMIT;
            case MARKET -> OrderTypeProto.MARKET;
        };
    }

    private static OrderStatusProto fromOrderStatus(OrderStatus status) {
        return switch (status) {
            case NEW -> OrderStatusProto.NEW;
            case PARTIALLY_FILLED -> OrderStatusProto.PARTIALLY_FILLED;
            case FILLED -> OrderStatusProto.FILLED;
            case CANCELLED -> OrderStatusProto.CANCELLED;
            case REJECTED -> OrderStatusProto.REJECTED;
        };
    }
}
