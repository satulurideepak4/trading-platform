package com.tradingplatform.pipeline.store;

import com.tradingplatform.pipeline.events.TradingEvent;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Every lifecycle event and every trade, in arrival order.
 *
 * <p>Bounded on purpose. An unbounded audit buffer in a process that also matches orders is a slow
 * memory leak that eventually takes trading down with it, and losing the oldest entries is a much
 * smaller problem than that. The durable audit trail is Kafka itself, which retains the same
 * records; this is a window over it, not the record of truth.
 */
public final class AuditLog {
    private final int maxEntries;
    private final Deque<TradingEvent> entries = new ArrayDeque<>();
    private final Set<String> seenEventIds = new HashSet<>();
    private long droppedEntries;

    public AuditLog(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("audit log must retain at least one entry");
        }
        this.maxEntries = maxEntries;
    }

    /**
     * Duplicate suppression only holds while an event is still inside the window; a redelivery
     * arriving after its entry was evicted is appended again. That is a property of the bound, and
     * acceptable because this is a debugging window rather than the audit record of truth.
     *
     * @return true if this call appended the event, false if it had already been recorded
     */
    public synchronized boolean append(TradingEvent event) {
        if (!seenEventIds.add(event.eventId())) {
            return false;
        }
        entries.addLast(event);
        if (entries.size() > maxEntries) {
            TradingEvent evicted = entries.removeFirst();
            seenEventIds.remove(evicted.eventId());
            droppedEntries++;
        }
        return true;
    }

    public synchronized List<TradingEvent> recent(int limit) {
        return entries.stream()
                .skip(Math.max(0, entries.size() - limit))
                .toList();
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized long droppedEntries() {
        return droppedEntries;
    }
}
