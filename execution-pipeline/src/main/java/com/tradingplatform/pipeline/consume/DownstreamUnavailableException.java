package com.tradingplatform.pipeline.consume;

/**
 * A dependency the consumer needs is unavailable. The record itself is fine.
 *
 * <p>This is the opposite of {@link InvalidEventException} and it exists to make that difference
 * actionable. An invalid record can never succeed, so it is dead-lettered immediately. A record
 * that failed because a database was unreachable will succeed as soon as the database returns, and
 * dead-lettering it would turn a temporary outage into permanent missing state.
 *
 * <p>Records failing this way are retried indefinitely, which blocks the partition until the
 * dependency recovers. That is the intended behaviour: a stalled consumer is visible in lag and
 * recoverable by fixing the dependency, whereas a trade that has been dropped into a dead-letter
 * topic is a silent hole in someone's position.
 */
public class DownstreamUnavailableException extends RuntimeException {

    public DownstreamUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
