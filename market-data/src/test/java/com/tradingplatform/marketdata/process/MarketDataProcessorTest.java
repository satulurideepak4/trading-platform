package com.tradingplatform.marketdata.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.marketdata.event.MarketEventType;
import com.tradingplatform.marketdata.event.RawMarketEvent;
import com.tradingplatform.marketdata.publish.MarketDataSink;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MarketDataProcessorTest {
    private static final Instant NOW = Instant.parse("2025-01-01T09:30:00Z");

    private final List<NormalizedUpdate> published = new ArrayList<>();
    private final MarketDataSink sink = published::add;
    private MarketDataProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new MarketDataProcessor(sink, new MarketDataMetrics(new SimpleMeterRegistry()));
    }

    @Test
    void inOrderBidUpdatesLatestSnapshotAndPublishes() {
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 1, 19_000, 100, NOW));

        assertEquals(1, published.size());
        NormalizedUpdate update = published.get(0);
        assertEquals(MarketEventType.BID, update.type());
        assertEquals(SequenceOutcome.IN_ORDER, update.outcome());
        assertEquals(1, update.processorSequence());

        SymbolSnapshot snapshot = processor.snapshot("AAPL").orElseThrow();
        assertEquals(19_000, snapshot.bestBidPrice());
        assertEquals(100, snapshot.bestBidQuantity());
    }

    @Test
    void bidAndAskAreMergedIntoOneSnapshot() {
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 1, 19_000, 100, NOW));
        processor.ingest(new RawMarketEvent.AskUpdate("AAPL", 2, 19_010, 50, NOW));

        SymbolSnapshot snapshot = processor.snapshot("AAPL").orElseThrow();
        assertEquals(19_000, snapshot.bestBidPrice());
        assertEquals(19_010, snapshot.bestAskPrice());
        assertEquals(19_005, snapshot.referencePrice());
    }

    @Test
    void tradeAloneIsTheReferencePriceWhenTheBookIsOneSided() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 19_000, 10, NOW));

        assertEquals(19_000, processor.snapshot("AAPL").orElseThrow().referencePrice());
    }

    @Test
    void nonPositivePriceIsRejectedAndDoesNotAdvanceState() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 0, 10, NOW));

        assertTrue(published.isEmpty());
        assertTrue(processor.snapshot("AAPL").isEmpty());
    }

    @Test
    void nonPositiveQuantityIsRejected() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 19_000, 0, NOW));

        assertTrue(published.isEmpty());
    }

    @Test
    void duplicateSequenceIsPublishedButDoesNotChangeSnapshot() {
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 5, 19_000, 100, NOW));
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 5, 19_500, 100, NOW));

        assertEquals(2, published.size());
        assertEquals(SequenceOutcome.DUPLICATE, published.get(1).outcome());
        assertEquals(19_000, processor.snapshot("AAPL").orElseThrow().bestBidPrice());
    }

    @Test
    void outOfOrderSequenceDoesNotRegressSnapshot() {
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 10, 19_000, 100, NOW));
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 7, 18_000, 100, NOW));

        assertEquals(SequenceOutcome.OUT_OF_ORDER, published.get(1).outcome());
        assertEquals(19_000, processor.snapshot("AAPL").orElseThrow().bestBidPrice());
    }

    @Test
    void gapIsStillAppliedAndFlagged() {
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 1, 19_000, 100, NOW));
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 4, 19_100, 100, NOW));

        assertEquals(SequenceOutcome.GAP, published.get(1).outcome());
        assertEquals(2, published.get(1).gapSize());
        assertEquals(19_100, processor.snapshot("AAPL").orElseThrow().bestBidPrice());
    }

    @Test
    void unknownSymbolHasNoSnapshot() {
        assertTrue(processor.snapshot("NOPE").isEmpty());
    }

    @Test
    void symbolsReturnsEveryTrackedSymbol() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 19_000, 10, NOW));
        processor.ingest(new RawMarketEvent.Trade("MSFT", 1, 42_000, 10, NOW));

        assertEquals(java.util.Set.of("AAPL", "MSFT"), processor.symbols());
    }

    @Test
    void processorSequenceIsGloballyMonotonicAcrossSymbols() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 19_000, 10, NOW));
        processor.ingest(new RawMarketEvent.Trade("MSFT", 1, 42_000, 10, NOW));

        assertEquals(1, published.get(0).processorSequence());
        assertEquals(2, published.get(1).processorSequence());
    }
}
