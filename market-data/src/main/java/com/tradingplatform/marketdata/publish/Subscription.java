package com.tradingplatform.marketdata.publish;

import java.util.Set;

/** A live registration with {@link MarketDataHub}. */
public interface Subscription extends AutoCloseable {
    /** Replaces the symbol interest set, e.g. in response to a client "subscribe" message. */
    void updateSymbols(Set<String> symbols);

    /** Updates dropped because this subscriber could not keep up. */
    long droppedCount();

    /** Unregisters and stops the drain thread. Idempotent. */
    @Override
    void close();
}
