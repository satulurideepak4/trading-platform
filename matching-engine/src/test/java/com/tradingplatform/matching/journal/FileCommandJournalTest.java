package com.tradingplatform.matching.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FileCommandJournalTest {
    private static final Instant NOW = Instant.parse("2025-01-01T09:30:00Z");

    @TempDir Path tempDir;

    @ParameterizedTest
    @EnumSource(FlushPolicy.class)
    void appendedRecordsReadBackInOrderWithIdenticalContent(FlushPolicy policy) {
        Path file = FileCommandJournal.fileFor(tempDir, 0);
        JournalRecord.Submit submit = new JournalRecord.Submit(
                1, 0, new SubmitOrder(1, "c-1", "AAPL", Side.BUY, OrderType.LIMIT, 100, 19_000, NOW));
        JournalRecord.Cancel cancel = new JournalRecord.Cancel(2, 0, new CancelOrder(1, NOW));
        JournalRecord.Replace replace =
                new JournalRecord.Replace(3, 0, new ReplaceOrder(1, 50, 19_500, NOW));

        try (FileCommandJournal journal = new FileCommandJournal(file, policy)) {
            journal.append(submit);
            journal.append(cancel);
            journal.append(replace);
        }

        List<JournalRecord> replayed = JournalReader.read(file);
        assertEquals(List.of(submit, cancel, replace), replayed);
    }

    @Test
    void aMissingJournalFileReadsAsEmpty() {
        Path file = FileCommandJournal.fileFor(tempDir, 7);
        assertEquals(List.of(), JournalReader.read(file));
    }

    @Test
    void aMarketOrderAndFullFieldRangeSurviveTheRoundTrip() {
        Path file = FileCommandJournal.fileFor(tempDir, 1);
        JournalRecord.Submit marketOrder = new JournalRecord.Submit(
                1, 1, SubmitOrder.market(42, "client-order-42", "EURUSD", Side.SELL, 999_999, NOW));

        try (FileCommandJournal journal = new FileCommandJournal(file, FlushPolicy.EVERY_RECORD)) {
            journal.append(marketOrder);
        }

        assertEquals(List.of(marketOrder), JournalReader.read(file));
    }

    @Test
    void aTornTrailingRecordIsDroppedButEverythingBeforeItSurvives() throws IOException {
        Path file = FileCommandJournal.fileFor(tempDir, 0);
        JournalRecord.Cancel first = new JournalRecord.Cancel(1, 0, new CancelOrder(1, NOW));
        JournalRecord.Cancel second = new JournalRecord.Cancel(2, 0, new CancelOrder(2, NOW));

        try (FileCommandJournal journal = new FileCommandJournal(file, FlushPolicy.EVERY_RECORD)) {
            journal.append(first);
            journal.append(second);
        }

        // Simulate a crash mid-append: a length prefix promising more content than ever arrived.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            ByteBuffer torn = ByteBuffer.allocate(4);
            torn.putInt(500);
            raf.write(torn.array());
            raf.write(new byte[]{1, 2, 3});
        }

        List<JournalRecord> replayed = JournalReader.read(file);
        assertEquals(List.of(first, second), replayed);
    }

    @Test
    void anEmptyTrailingLengthPrefixIsAlsoTolerated() throws IOException {
        Path file = FileCommandJournal.fileFor(tempDir, 0);
        JournalRecord.Cancel only = new JournalRecord.Cancel(1, 0, new CancelOrder(1, NOW));
        try (FileCommandJournal journal = new FileCommandJournal(file, FlushPolicy.EVERY_RECORD)) {
            journal.append(only);
        }
        // Two stray bytes: not even enough for a length prefix.
        Files.write(file, new byte[]{9, 9}, java.nio.file.StandardOpenOption.APPEND);

        assertEquals(List.of(only), JournalReader.read(file));
    }

    @Test
    void aSequenceGapAnywhereButTheTailIsFatal() throws IOException {
        Path file = FileCommandJournal.fileFor(tempDir, 0);
        JournalRecord.Cancel one = new JournalRecord.Cancel(1, 0, new CancelOrder(1, NOW));
        JournalRecord.Cancel three = new JournalRecord.Cancel(3, 0, new CancelOrder(2, NOW));

        try (FileCommandJournal journal = new FileCommandJournal(file, FlushPolicy.EVERY_RECORD)) {
            journal.append(one);
            // Skips sequence 2 entirely - not something this writer would ever do on its own, so
            // the reader must treat it as corruption rather than a torn write.
            journal.append(three);
        }

        JournalCorruptionException failure =
                assertThrows(JournalCorruptionException.class, () -> JournalReader.read(file));
        assertTrue(failure.getMessage().contains("expected sequence 2"));
    }

    @Test
    void aStructurallyIntactButFieldCorruptedRecordIsJournalCorruptionNotARawException()
            throws IOException {
        Path file = FileCommandJournal.fileFor(tempDir, 0);
        JournalRecord.Submit submit = new JournalRecord.Submit(
                1, 0, SubmitOrder.limit(1, "client-1", "AAPL", Side.BUY, 10, 19_000, NOW));
        byte[] content = JournalCodec.encode(submit);

        // The record's length prefix (so this is not the torn-tail case) and journal sequence (so
        // this is not the sequence-gap case) are both left correct - only the side ordinal, the
        // third-from-last byte before orderType/quantity/price, is corrupted to a value neither
        // Side constant has. Every caller of JournalReader.read is documented to expect
        // JournalCorruptionException from a corrupt journal, not whatever raw JDK exception
        // Side.values()[...] happens to throw.
        content[content.length - 18] = (byte) 99;

        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            ByteBuffer frame = ByteBuffer.allocate(4 + content.length);
            frame.putInt(content.length);
            frame.put(content);
            raf.write(frame.array());
        }

        JournalCorruptionException failure =
                assertThrows(JournalCorruptionException.class, () -> JournalReader.read(file));
        assertTrue(failure.getCause() instanceof ArrayIndexOutOfBoundsException);
    }
}
