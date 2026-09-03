package com.tradingplatform.marketdata.process;

/**
 * What {@link SymbolSequenceTracker} decided about one incoming exchange sequence number, relative
 * to the last one this symbol actually applied.
 */
public enum SequenceOutcome {
    /** Exactly one greater than the last applied sequence. The common case. */
    IN_ORDER,

    /** Exactly equal to the last applied sequence. An exact repeat; state is not advanced. */
    DUPLICATE,

    /**
     * Strictly less than the last applied sequence. Whether this exact record was seen before or
     * simply left the exchange after an already-applied one and arrived later, the tracker only
     * keeps the last applied sequence rather than unbounded history, so both look the same from
     * here: too late to matter. State is not advanced either way — applying it would move a
     * symbol's book backwards in time.
     */
    OUT_OF_ORDER,

    /**
     * More than one greater than the last applied sequence. One or more updates were lost in
     * between. Still applied — there is nothing to reconstruct the missing updates from — but
     * flagged so a subscriber knows this symbol's state may have skipped intermediate prices.
     */
    GAP,

    /** Not from the feed at all: the state pushed to a subscriber the moment it subscribes. */
    SNAPSHOT
}
