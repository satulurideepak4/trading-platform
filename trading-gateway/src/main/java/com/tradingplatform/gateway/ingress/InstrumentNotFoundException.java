package com.tradingplatform.gateway.ingress;

public final class InstrumentNotFoundException extends RuntimeException {

    public InstrumentNotFoundException(String symbol) {
        super(symbol + " is not an enabled instrument");
    }
}
