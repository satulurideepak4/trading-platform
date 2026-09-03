package com.tradingplatform.marketdata.event;

import java.time.Instant;

/**
 * What an exchange feed actually puts on the wire, before anything downstream has looked at it.
 *
 * <p>Deliberately as untrusted as a real feed: {@code exchangeSequence} may repeat, arrive out of
 * order, or skip values, because the simulator that produces these injects exactly those faults on
 * purpose. See {@code MarketSimulator}. Nothing here is validated or normalized — that is
 * {@code MarketDataProcessor}'s job, kept separate so the ingestion boundary and the cleanup logic
 * can be reasoned about independently.
 */
public sealed interface RawMarketEvent {
    String symbol();

    /** Assigned by the exchange, per symbol. Not a global sequence. */
    long exchangeSequence();

    Instant exchangeTimestamp();

    record BidUpdate(
            String symbol, long exchangeSequence, long price, long quantity, Instant exchangeTimestamp)
            implements RawMarketEvent {}

    record AskUpdate(
            String symbol, long exchangeSequence, long price, long quantity, Instant exchangeTimestamp)
            implements RawMarketEvent {}

    record Trade(
            String symbol, long exchangeSequence, long price, long quantity, Instant exchangeTimestamp)
            implements RawMarketEvent {}
}
