package com.tradingplatform.matching;

import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import java.time.Instant;

final class OrderState {
    private final long orderId;
    private final String clientOrderId;
    private final String symbol;
    private final Side side;
    private final OrderType type;
    private final Instant createdAt;

    private long quantity;
    private long remainingQuantity;
    private long price;
    private Instant updatedAt;
    private long prioritySequence;
    private OrderStatus status;

    OrderState(SubmitOrder command, long prioritySequence) {
        this.orderId = command.orderId();
        this.clientOrderId = command.clientOrderId();
        this.symbol = command.symbol();
        this.side = command.side();
        this.type = command.type();
        this.quantity = command.quantity();
        this.remainingQuantity = command.quantity();
        this.price = command.price();
        this.createdAt = command.timestamp();
        this.updatedAt = command.timestamp();
        this.prioritySequence = prioritySequence;
        this.status = OrderStatus.NEW;
    }

    void fill(long fillQuantity, Instant timestamp) {
        if (!isActive() || fillQuantity <= 0 || fillQuantity > remainingQuantity) {
            throw new IllegalStateException("invalid fill for order " + orderId);
        }
        remainingQuantity -= fillQuantity;
        updatedAt = timestamp;
        status = remainingQuantity == 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
    }

    void cancel(Instant timestamp) {
        if (!isActive()) {
            throw new IllegalStateException("only active orders can be cancelled");
        }
        updatedAt = timestamp;
        status = OrderStatus.CANCELLED;
    }

    void replace(long newQuantity, long newPrice, Instant timestamp, long newPrioritySequence) {
        long alreadyExecuted = executedQuantity();
        quantity = newQuantity;
        remainingQuantity = newQuantity - alreadyExecuted;
        price = newPrice;
        updatedAt = timestamp;
        prioritySequence = newPrioritySequence;
        if (remainingQuantity == 0) {
            status = OrderStatus.FILLED;
        } else if (alreadyExecuted == 0) {
            status = OrderStatus.NEW;
        } else {
            status = OrderStatus.PARTIALLY_FILLED;
        }
    }

    boolean isActive() {
        return status == OrderStatus.NEW || status == OrderStatus.PARTIALLY_FILLED;
    }

    long executedQuantity() {
        return quantity - remainingQuantity;
    }

    long orderId() {
        return orderId;
    }

    String clientOrderId() {
        return clientOrderId;
    }

    String symbol() {
        return symbol;
    }

    Side side() {
        return side;
    }

    OrderType type() {
        return type;
    }

    long remainingQuantity() {
        return remainingQuantity;
    }

    long price() {
        return price;
    }

    OrderSnapshot snapshot() {
        return new OrderSnapshot(
                orderId,
                clientOrderId,
                symbol,
                side,
                type,
                status,
                quantity,
                remainingQuantity,
                price,
                createdAt,
                updatedAt,
                prioritySequence);
    }
}
