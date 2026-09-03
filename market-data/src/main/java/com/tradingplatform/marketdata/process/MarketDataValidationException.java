package com.tradingplatform.marketdata.process;

/** A raw event failed a basic sanity check — nonpositive price or quantity. Dropped, never applied. */
public class MarketDataValidationException extends RuntimeException {
    public MarketDataValidationException(String message) {
        super(message);
    }
}
