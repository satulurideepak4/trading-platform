package com.tradingplatform.marketdata.publish;

import com.tradingplatform.marketdata.process.NormalizedUpdate;

/**
 * One subscriber's delivery callback. Implementations may block — a WebSocket send is exactly
 * that — because each is driven from its own drain thread; see {@link QueuedMarketDataSubscription}.
 * A slow implementation only ever slows its own subscription, never another one and never
 * ingestion.
 */
public interface MarketDataListener {
    void onUpdate(NormalizedUpdate update);

    /** Called once, from the drain thread, after the subscription has been torn down. */
    default void onDisconnected(String reason) {}
}
