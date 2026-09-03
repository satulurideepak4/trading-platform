package com.tradingplatform.gateway.api;

import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.matching.OrderBookSnapshot;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregated view of a book. Individual resting orders are not exposed: they belong to other
 * accounts, and a venue publishes depth by price level rather than by order.
 */
public record OrderBookResponse(String symbol, List<PriceLevel> bids, List<PriceLevel> asks) {

    public static OrderBookResponse from(OrderBookSnapshot snapshot, int depth) {
        return new OrderBookResponse(
                snapshot.symbol(),
                aggregate(snapshot.bids(), depth),
                aggregate(snapshot.asks(), depth));
    }

    /**
     * The snapshot already lists orders in price-then-time priority, so equal prices are adjacent
     * and one pass is enough.
     */
    private static List<PriceLevel> aggregate(List<OrderSnapshot> orders, int depth) {
        List<PriceLevel> levels = new ArrayList<>();
        long currentPrice = 0;
        long currentQuantity = 0;
        int currentOrderCount = 0;

        for (OrderSnapshot order : orders) {
            if (currentOrderCount > 0 && order.price() != currentPrice) {
                levels.add(new PriceLevel(currentPrice, currentQuantity, currentOrderCount));
                if (levels.size() == depth) {
                    return levels;
                }
                currentQuantity = 0;
                currentOrderCount = 0;
            }
            currentPrice = order.price();
            currentQuantity += order.remainingQuantity();
            currentOrderCount++;
        }
        if (currentOrderCount > 0 && levels.size() < depth) {
            levels.add(new PriceLevel(currentPrice, currentQuantity, currentOrderCount));
        }
        return levels;
    }

    public record PriceLevel(long price, long quantity, int orderCount) {}
}
