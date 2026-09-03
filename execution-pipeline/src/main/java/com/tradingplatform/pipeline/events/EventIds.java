package com.tradingplatform.pipeline.events;

/**
 * Event ids derived from the fact they describe.
 *
 * <p>Every id here is a pure function of domain identifiers that the matching engine already
 * guarantees to be unique. Nothing is generated, so publishing the same fact twice — after a
 * producer retry, a gateway restart or a replay — yields the same id, and a consumer that has
 * already applied it can skip it. This is the whole basis of consumer idempotency; see ADR-008.
 */
public final class EventIds {

    private EventIds() {}

    /** An order id is accepted at most once. */
    public static String accepted(long orderId) {
        return "acc-" + orderId;
    }

    /** The gateway allocates a fresh order id per rejection, so one id is rejected at most once. */
    public static String rejected(long orderId) {
        return "rej-" + orderId;
    }

    /** An order reaches CANCELLED at most once. */
    public static String cancelled(long orderId) {
        return "can-" + orderId;
    }

    /**
     * An order can be replaced repeatedly, so the id includes the priority sequence the engine
     * assigns on each replacement. That sequence is strictly increasing within an engine, which
     * makes each replacement of an order distinguishable from the next.
     */
    public static String replaced(long orderId, long prioritySequence) {
        return "rep-" + orderId + "-" + prioritySequence;
    }

    /** Execution ids come from per-worker disjoint sequences and are unique platform-wide. */
    public static String execution(long executionId) {
        return "exe-" + executionId;
    }

    /** An order reaches FILLED at most once. */
    public static String filled(long orderId) {
        return "fil-" + orderId;
    }
}
