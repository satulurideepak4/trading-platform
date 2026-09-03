package com.tradingplatform.gateway.api;

import com.tradingplatform.marketdata.process.SymbolSnapshot;
import java.time.Instant;

public record MarketSnapshotResponse(
        String symbol,
        Long bestBidPrice,
        Long bestBidQuantity,
        Long bestAskPrice,
        Long bestAskQuantity,
        Long lastTradePrice,
        Long lastTradeQuantity,
        Long referencePrice,
        long lastSequence,
        Instant asOf) {

    public static MarketSnapshotResponse from(SymbolSnapshot snapshot) {
        return new MarketSnapshotResponse(
                snapshot.symbol(),
                snapshot.bestBidPrice(),
                snapshot.bestBidQuantity(),
                snapshot.bestAskPrice(),
                snapshot.bestAskQuantity(),
                snapshot.lastTradePrice(),
                snapshot.lastTradeQuantity(),
                snapshot.referencePrice(),
                snapshot.lastSequence(),
                snapshot.asOf());
    }
}
