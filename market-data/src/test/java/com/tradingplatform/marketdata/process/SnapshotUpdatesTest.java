package com.tradingplatform.marketdata.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.marketdata.event.MarketEventType;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotUpdatesTest {
    private static final Instant NOW = Instant.parse("2025-01-01T09:30:00Z");

    @Test
    void aFullyPopulatedSnapshotProducesOneUpdatePerSide() {
        SymbolSnapshot snapshot =
                new SymbolSnapshot("AAPL", 19_000L, 100L, 19_010L, 50L, 18_995L, 10L, 7, NOW);

        List<NormalizedUpdate> updates = SnapshotUpdates.of(snapshot);

        assertEquals(3, updates.size());
        assertTrue(updates.stream().allMatch(u -> u.outcome() == SequenceOutcome.SNAPSHOT));
        assertTrue(updates.stream().anyMatch(u -> u.type() == MarketEventType.BID && u.price() == 19_000));
        assertTrue(updates.stream().anyMatch(u -> u.type() == MarketEventType.ASK && u.price() == 19_010));
        assertTrue(updates.stream().anyMatch(u -> u.type() == MarketEventType.TRADE && u.price() == 18_995));
    }

    @Test
    void aOneSidedSnapshotProducesOnlyTheSidesItHas() {
        SymbolSnapshot tradeOnly = new SymbolSnapshot("AAPL", null, null, null, null, 18_995L, 10L, 1, NOW);

        List<NormalizedUpdate> updates = SnapshotUpdates.of(tradeOnly);

        assertEquals(1, updates.size());
        assertEquals(MarketEventType.TRADE, updates.get(0).type());
    }

    @Test
    void anEmptySnapshotProducesNoUpdates() {
        assertTrue(SnapshotUpdates.of(SymbolSnapshot.empty("AAPL")).isEmpty());
    }
}
