package com.tradingplatform.matching;

import com.tradingplatform.domain.OrderSnapshot;
import java.util.List;

public record OrderBookSnapshot(
        String symbol, List<OrderSnapshot> bids, List<OrderSnapshot> asks) {

    public OrderBookSnapshot {
        bids = List.copyOf(bids);
        asks = List.copyOf(asks);
    }
}
