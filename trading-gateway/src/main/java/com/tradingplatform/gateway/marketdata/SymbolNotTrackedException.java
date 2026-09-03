package com.tradingplatform.gateway.marketdata;

/** No market-data update has been seen for this symbol, so there is no snapshot to return. */
public class SymbolNotTrackedException extends RuntimeException {
    public SymbolNotTrackedException(String symbol) {
        super("no market data has been seen for symbol " + symbol);
    }
}
