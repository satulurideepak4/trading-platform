package com.tradingplatform.matching.journal;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.SubmitOrder;

/**
 * One durably journaled command. Read-only operations ({@code findOrder}, {@code book}) are never
 * journaled — only the three commands that change engine state need to be replayed to reconstruct
 * it.
 *
 * <p>{@code journalSequence} is per-worker and strictly increasing by one; see {@link
 * JournalReader} for how a gap or a torn trailing record are told apart during replay.
 */
public sealed interface JournalRecord {
    long journalSequence();

    int workerId();

    record Submit(long journalSequence, int workerId, SubmitOrder command) implements JournalRecord {}

    record Cancel(long journalSequence, int workerId, CancelOrder command) implements JournalRecord {}

    record Replace(long journalSequence, int workerId, ReplaceOrder command) implements JournalRecord {}
}
