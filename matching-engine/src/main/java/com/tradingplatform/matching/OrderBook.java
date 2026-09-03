package com.tradingplatform.matching;

import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.Side;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Mutable state for one instrument. Calls must be serialized by the owner; this class is
 * deliberately not thread-safe in Stage 1.
 */
final class OrderBook {
    private final String symbol;
    private final NavigableMap<Long, PriceLevel> bids =
            new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<Long, PriceLevel> asks = new TreeMap<>();

    OrderBook(String symbol) {
        this.symbol = symbol;
    }

    void add(OrderState order) {
        if (!order.isActive() || order.type() != com.tradingplatform.domain.OrderType.LIMIT) {
            throw new IllegalStateException("only active limit orders can rest");
        }
        levels(order.side())
                .computeIfAbsent(order.price(), ignored -> new PriceLevel())
                .add(order);
    }

    void remove(OrderState order) {
        NavigableMap<Long, PriceLevel> levels = levels(order.side());
        PriceLevel level = levels.get(order.price());
        if (level == null || !level.remove(order.orderId())) {
            throw new IllegalStateException("order is not resting in its expected price level");
        }
        if (level.isEmpty()) {
            levels.remove(order.price());
        }
    }

    OrderState bestOpposite(Side incomingSide) {
        NavigableMap<Long, PriceLevel> opposite = levels(incomingSide.opposite());
        Map.Entry<Long, PriceLevel> bestLevel = opposite.firstEntry();
        return bestLevel == null ? null : bestLevel.getValue().first();
    }

    boolean crosses(OrderState incoming, OrderState resting) {
        if (incoming.type() == com.tradingplatform.domain.OrderType.MARKET) {
            return true;
        }
        return incoming.side() == Side.BUY
                ? incoming.price() >= resting.price()
                : incoming.price() <= resting.price();
    }

    OrderBookSnapshot snapshot() {
        return new OrderBookSnapshot(symbol, flatten(bids), flatten(asks));
    }

    private NavigableMap<Long, PriceLevel> levels(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    private static List<OrderSnapshot> flatten(NavigableMap<Long, PriceLevel> levels) {
        List<OrderSnapshot> orders = new ArrayList<>();
        for (PriceLevel level : levels.values()) {
            level.appendSnapshotsTo(orders);
        }
        return orders;
    }

    private static final class PriceLevel {
        private final LinkedHashMap<Long, OrderState> orders = new LinkedHashMap<>();

        void add(OrderState order) {
            if (orders.putIfAbsent(order.orderId(), order) != null) {
                throw new IllegalStateException("order already exists at price level");
            }
        }

        boolean remove(long orderId) {
            return orders.remove(orderId) != null;
        }

        OrderState first() {
            return orders.sequencedValues().getFirst();
        }

        boolean isEmpty() {
            return orders.isEmpty();
        }

        void appendSnapshotsTo(List<OrderSnapshot> destination) {
            for (OrderState order : orders.values()) {
                destination.add(order.snapshot());
            }
        }
    }
}
