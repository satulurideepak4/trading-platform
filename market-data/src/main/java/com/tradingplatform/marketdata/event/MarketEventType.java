package com.tradingplatform.marketdata.event;

/** Discriminator carried on the normalized side, once a {@link RawMarketEvent} has a known shape. */
public enum MarketEventType {
    BID,
    ASK,
    TRADE;

    public static MarketEventType of(RawMarketEvent event) {
        return switch (event) {
            case RawMarketEvent.BidUpdate ignored -> BID;
            case RawMarketEvent.AskUpdate ignored -> ASK;
            case RawMarketEvent.Trade ignored -> TRADE;
        };
    }
}
