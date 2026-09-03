package com.tradingplatform.portfolio;

import com.tradingplatform.pipeline.consume.DownstreamUnavailableException;

/**
 * The database could not be reached or the statement failed for a reason that may not recur.
 *
 * <p>Separated from every other failure because it decides what happens to the record. A trade that
 * cannot be written because the database is down must not be dead-lettered: the outage will end,
 * and a dead-lettered execution is a permanent hole in a position. Records failing this way are
 * retried indefinitely, blocking the partition until the database returns. Blocking is recoverable;
 * a lost trade is not.
 */
public class TransientPersistenceException extends DownstreamUnavailableException {

    public TransientPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
