package com.tradingplatform.matching.journal;

/**
 * How aggressively {@link FileCommandJournal} pushes a record to disk before {@code append}
 * returns.
 *
 * <p>This is a recovery-point-versus-latency trade-off, not a style preference. See ADR-015.
 */
public enum FlushPolicy {
    /**
     * {@code force(false)} — fsync the file's data, not its metadata — after every single record.
     * The default. A command is not considered durable, and no caller ever observes it as
     * successful, until it is fsynced: a crash immediately afterward loses nothing that was ever
     * acknowledged. The cost is one fsync per command on the worker thread, and no throughput number
     * is claimed for it here — that measurement is Stage 9's job, not this one's.
     */
    EVERY_RECORD,

    /**
     * Writes go to the OS page cache and are flushed only when the journal is closed. Survives a
     * process crash (the OS still holds the page cache), not a power loss or OS crash. Exists for
     * tests and for a future throughput comparison against {@link #EVERY_RECORD}; not used by
     * {@code trading-gateway} today.
     */
    BUFFERED
}
