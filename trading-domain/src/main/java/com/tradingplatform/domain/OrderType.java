package com.tradingplatform.domain;

/**
 * {@code ordinal()} is a load-bearing on-disk contract: {@code matching-engine}'s
 * {@code JournalCodec} stores this ordinal directly (one byte, via {@code OrderType.values()[...]}
 * on decode) as part of the matching-engine's crash-recovery journal — see ADR-015. Reordering
 * these constants, or inserting a new one anywhere but the end, silently changes what an existing
 * journal file decodes to on the next replay, with no runtime error to catch it. Add new
 * constants only at the end.
 */
public enum OrderType {
    LIMIT,
    MARKET
}
