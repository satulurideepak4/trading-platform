package com.tradingplatform.marketdata.process;

import com.tradingplatform.marketdata.event.MarketEventType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link SymbolSnapshot} into the {@link NormalizedUpdate}s that represent it, so a
 * newly subscribing client can be shown current state in the same shape as a live tick rather than
 * a bespoke DTO. Every one of these carries {@link SequenceOutcome#SNAPSHOT}, which is how a client
 * tells "this is where things stood when I subscribed" apart from "this just happened".
 */
public final class SnapshotUpdates {

    private SnapshotUpdates() {}

    /** One update per side that actually has a value; an empty list for a symbol with no state. */
    public static List<NormalizedUpdate> of(SymbolSnapshot snapshot) {
        List<NormalizedUpdate> updates = new ArrayList<>(3);
        Instant now = Instant.now();
        if (snapshot.bestBidPrice() != null) {
            updates.add(update(
                    snapshot, MarketEventType.BID, snapshot.bestBidPrice(), snapshot.bestBidQuantity(), now));
        }
        if (snapshot.bestAskPrice() != null) {
            updates.add(update(
                    snapshot, MarketEventType.ASK, snapshot.bestAskPrice(), snapshot.bestAskQuantity(), now));
        }
        if (snapshot.lastTradePrice() != null) {
            updates.add(update(
                    snapshot,
                    MarketEventType.TRADE,
                    snapshot.lastTradePrice(),
                    snapshot.lastTradeQuantity(),
                    now));
        }
        return updates;
    }

    private static NormalizedUpdate update(
            SymbolSnapshot snapshot, MarketEventType type, long price, long quantity, Instant now) {
        return new NormalizedUpdate(
                snapshot.symbol(),
                type,
                price,
                quantity,
                snapshot.lastSequence(),
                snapshot.asOf(),
                0,
                now,
                SequenceOutcome.SNAPSHOT,
                0);
    }
}
