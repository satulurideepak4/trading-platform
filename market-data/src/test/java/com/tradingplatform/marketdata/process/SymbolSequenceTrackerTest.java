package com.tradingplatform.marketdata.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SymbolSequenceTrackerTest {
    private final SymbolSequenceTracker tracker = new SymbolSequenceTracker();

    @Test
    void firstSequenceSeenForASymbolIsInOrder() {
        var classification = tracker.classify("AAPL", 1);
        assertEquals(SequenceOutcome.IN_ORDER, classification.outcome());
        assertTrue(classification.advancesState());
    }

    @Test
    void consecutiveSequencesAreInOrder() {
        tracker.classify("AAPL", 1);
        var classification = tracker.classify("AAPL", 2);
        assertEquals(SequenceOutcome.IN_ORDER, classification.outcome());
    }

    @Test
    void repeatingTheLastAppliedSequenceIsADuplicate() {
        tracker.classify("AAPL", 5);
        var classification = tracker.classify("AAPL", 5);
        assertEquals(SequenceOutcome.DUPLICATE, classification.outcome());
        assertFalse(classification.advancesState());
    }

    @Test
    void aSequenceBelowLastAppliedIsOutOfOrder() {
        tracker.classify("AAPL", 10);
        var classification = tracker.classify("AAPL", 7);
        assertEquals(SequenceOutcome.OUT_OF_ORDER, classification.outcome());
    }

    @Test
    void aJumpAheadIsAGapWithTheSkippedCount() {
        tracker.classify("AAPL", 10);
        var classification = tracker.classify("AAPL", 14);
        assertEquals(SequenceOutcome.GAP, classification.outcome());
        assertEquals(3, classification.gapSize());
        assertTrue(classification.advancesState());
    }

    @Test
    void afterAGapTheNewSequenceBecomesLastApplied() {
        tracker.classify("AAPL", 10);
        tracker.classify("AAPL", 14);
        var classification = tracker.classify("AAPL", 15);
        assertEquals(SequenceOutcome.IN_ORDER, classification.outcome());
    }

    @Test
    void symbolsAreTrackedIndependently() {
        tracker.classify("AAPL", 100);
        var classification = tracker.classify("MSFT", 1);
        assertEquals(SequenceOutcome.IN_ORDER, classification.outcome());
    }
}
