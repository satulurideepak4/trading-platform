package com.tradingplatform.marketdata.process;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-symbol classification of exchange sequence numbers. See {@link SequenceOutcome} for what
 * each result means and why {@code OUT_OF_ORDER} and {@code DUPLICATE} are told apart the way they
 * are.
 *
 * <p>One {@code lastApplied} value per symbol is all that is kept — no window of recently seen
 * sequences, no per-symbol history. That is a deliberate memory bound: a feed running for days
 * across many symbols must not grow an ever-larger dedup set, and the one number kept is exactly
 * what "did this move the symbol forward" needs.
 *
 * <p>Not designed for concurrent callers on the same symbol. In practice there is exactly one
 * writer — {@link MarketDataProcessor} is fed by a single ingestion thread, the same single-writer
 * discipline the matching engine uses per order book — so no locking is needed here either.
 */
final class SymbolSequenceTracker {
    private final Map<String, Long> lastApplied = new ConcurrentHashMap<>();

    /**
     * Classifies {@code sequence} for {@code symbol} and, if it advances the symbol, records it as
     * the new last-applied value.
     *
     * @return the outcome, and for {@code GAP} the number of sequence numbers skipped
     */
    Classification classify(String symbol, long sequence) {
        Long previous = lastApplied.get(symbol);
        if (previous == null) {
            lastApplied.put(symbol, sequence);
            return new Classification(SequenceOutcome.IN_ORDER, 0);
        }
        if (sequence == previous) {
            return new Classification(SequenceOutcome.DUPLICATE, 0);
        }
        if (sequence < previous) {
            return new Classification(SequenceOutcome.OUT_OF_ORDER, 0);
        }
        long skipped = sequence - previous - 1;
        lastApplied.put(symbol, sequence);
        return skipped == 0
                ? new Classification(SequenceOutcome.IN_ORDER, 0)
                : new Classification(SequenceOutcome.GAP, (int) Math.min(skipped, Integer.MAX_VALUE));
    }

    record Classification(SequenceOutcome outcome, int gapSize) {
        boolean advancesState() {
            return outcome == SequenceOutcome.IN_ORDER || outcome == SequenceOutcome.GAP;
        }
    }
}
