package com.tradingplatform.marketdata.process;

import java.time.Instant;

/**
 * The latest known state of one symbol's top of book and last trade.
 *
 * <p>Immutable and replaced wholesale on every update rather than mutated in place. A reader — the
 * REST snapshot endpoint, a newly subscribing WebSocket client, {@code MarkPriceUpdater} — always
 * sees a value that was true at some single instant, never a partially applied one.
 *
 * <p>Any field may be {@code null}/zero-valued until the first update of that kind arrives for the
 * symbol: a symbol can have traded without ever showing a two-sided quote, or vice versa.
 */
public record SymbolSnapshot(
        String symbol,
        Long bestBidPrice,
        Long bestBidQuantity,
        Long bestAskPrice,
        Long bestAskQuantity,
        Long lastTradePrice,
        Long lastTradeQuantity,
        long lastSequence,
        Instant asOf) {

    public static SymbolSnapshot empty(String symbol) {
        return new SymbolSnapshot(symbol, null, null, null, null, null, null, 0, Instant.EPOCH);
    }

    /**
     * The reference price this symbol should be marked at right now: mid of a two-sided quote, or
     * the last trade when the book is one-sided or empty. {@code null} if neither exists yet.
     */
    public Long referencePrice() {
        if (bestBidPrice != null && bestAskPrice != null) {
            return Math.addExact(bestBidPrice, bestAskPrice) / 2;
        }
        return lastTradePrice;
    }
}
