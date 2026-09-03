package com.tradingplatform.marketdata.process;

import com.tradingplatform.marketdata.event.MarketEventType;
import java.time.Instant;

/**
 * What {@link MarketDataProcessor} publishes: one raw event, validated and classified, with a
 * processor-assigned sequence stitched on. Self-describing enough that a subscriber which is not
 * this application — a strategy process, a dashboard — can consume it directly.
 *
 * @param processorSequence global, monotonically increasing across all symbols. Exchange
 *     sequences are per symbol and untrusted (see {@link SequenceOutcome}); this is the one number
 *     a subscriber can use to detect a gap in what *this processor* published, independent of
 *     upstream feed quality.
 * @param outcome what {@link SymbolSequenceTracker} decided about {@code exchangeSequence}
 * @param gapSize number of exchange sequence numbers skipped; nonzero only when outcome is
 *     {@code GAP}
 */
public record NormalizedUpdate(
        String symbol,
        MarketEventType type,
        long price,
        long quantity,
        long exchangeSequence,
        Instant exchangeTimestamp,
        long processorSequence,
        Instant receivedAt,
        SequenceOutcome outcome,
        int gapSize) {}
