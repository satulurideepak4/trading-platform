package com.tradingplatform.pipeline.consume;

/**
 * The record deserialized but does not describe a possible trading fact.
 *
 * <p>This is deliberately a separate type from any transient failure, because it decides retry
 * policy: no amount of retrying will make a negative quantity valid, so records that fail this way
 * go straight to the dead-letter topic instead of consuming the retry budget. See ADR-009.
 */
public final class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }
}
