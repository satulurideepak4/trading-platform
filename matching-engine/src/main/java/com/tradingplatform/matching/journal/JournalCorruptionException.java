package com.tradingplatform.matching.journal;

/**
 * The journal cannot be trusted to replay correctly: a mid-file sequence gap, a record whose
 * declared type is not one of the three known kinds, or a structurally-intact record (correct
 * length prefix, so not a torn trailing write) whose field bytes decode to something invalid — a
 * bad enum ordinal, a length prefix that overruns the record. Deliberately fatal rather than
 * best-effort — see {@link JournalReader} for the one kind of damage that is <em>not</em> fatal, a
 * torn trailing write.
 */
public class JournalCorruptionException extends RuntimeException {
    public JournalCorruptionException(String message) {
        super(message);
    }

    public JournalCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
