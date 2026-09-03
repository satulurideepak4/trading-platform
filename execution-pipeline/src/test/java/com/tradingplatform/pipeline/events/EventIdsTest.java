package com.tradingplatform.pipeline.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the whole basis of consumer idempotency (see {@link EventIds}'s own
 * Javadoc and ADR-008): every id is a pure, stable function of the fact it describes, and every
 * event kind occupies its own namespace so two different facts about the same order id can never
 * collide.
 */
class EventIdsTest {

    @Test
    void acceptedIdIsPrefixedAndStableForTheSameOrderId() {
        assertEquals("acc-42", EventIds.accepted(42));
        assertEquals(EventIds.accepted(42), EventIds.accepted(42));
    }

    @Test
    void rejectedAndCancelledIdsUseDistinctPrefixesFromAccepted() {
        assertEquals("rej-42", EventIds.rejected(42));
        assertEquals("can-42", EventIds.cancelled(42));
        assertNotEquals(EventIds.accepted(42), EventIds.rejected(42));
        assertNotEquals(EventIds.accepted(42), EventIds.cancelled(42));
    }

    @Test
    void replacedIdIncludesPrioritySequenceSoRepeatedReplacementsAreDistinguishable() {
        String firstReplacement = EventIds.replaced(42, 1);
        String secondReplacement = EventIds.replaced(42, 2);

        assertEquals("rep-42-1", firstReplacement);
        assertNotEquals(firstReplacement, secondReplacement);
    }

    @Test
    void executionIdIsPrefixedByExecutionId() {
        assertEquals("exe-777", EventIds.execution(777));
    }

    @Test
    void filledIdIsPrefixedAndDistinctFromAccepted() {
        assertEquals("fil-42", EventIds.filled(42));
        assertNotEquals(EventIds.accepted(42), EventIds.filled(42));
    }

    @Test
    void everyEventKindProducesADisjointIdNamespaceForTheSameOrderId() {
        long orderId = 42;
        List<String> ids = List.of(
                EventIds.accepted(orderId),
                EventIds.rejected(orderId),
                EventIds.cancelled(orderId),
                EventIds.replaced(orderId, 1),
                EventIds.filled(orderId));

        assertEquals(ids.size(), Set.copyOf(ids).size(), "every event-kind id must be unique for one order id");
    }
}
