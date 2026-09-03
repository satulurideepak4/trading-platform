package com.tradingplatform.matching.journal;

/**
 * Where a matching worker durably records the commands it processes, so its state can be rebuilt
 * after a restart. See {@link FileCommandJournal} and ADR-015.
 *
 * <p>{@link FileCommandJournal} is the only implementation today. The interface still earns its
 * place: a worker's journal reference is {@code null} whenever journaling is disabled
 * ({@code MATCHING_JOURNAL_ENABLED=false}), so {@code OrderRouter}'s worker loop is written
 * against this narrow contract rather than against file/fsync details it has no reason to know
 * about, and a future in-memory or test-only implementation has a seam to plug into without
 * touching worker logic.
 */
public interface CommandJournal extends AutoCloseable {
    /**
     * Durably appends {@code record}. Does not return until the configured {@link FlushPolicy} is
     * satisfied — for {@link FlushPolicy#EVERY_RECORD}, that means the record is on disk before this
     * call returns.
     */
    void append(JournalRecord record);

    @Override
    void close();
}
