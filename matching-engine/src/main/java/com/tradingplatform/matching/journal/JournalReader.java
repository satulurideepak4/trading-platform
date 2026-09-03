package com.tradingplatform.matching.journal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads one worker's journal file back into the ordered command sequence that produced it.
 *
 * <p>Two kinds of damage are told apart, on purpose:
 *
 * <ul>
 *   <li><b>A torn trailing record</b> — the length prefix or the bytes it promises are cut short —
 *       is exactly the shape a crash mid-{@code append} leaves behind. Everything before it is
 *       intact, so reading stops there and replay proceeds with what was fully written.
 *   <li><b>A sequence gap anywhere else</b> is not something a crash mid-write can produce — this
 *       writer only ever appends the next number — so it means something is actually wrong with the
 *       file, and {@link JournalCorruptionException} refuses to guess which record is missing.
 * </ul>
 */
public final class JournalReader {

    private JournalReader() {}

    /** Empty if the file does not exist yet — a worker that has never journaled anything. */
    public static List<JournalRecord> read(Path file) {
        return scan(file).records();
    }

    /**
     * The byte offset {@link #read} stopped at — the end of the last complete, sequence-valid
     * record, not including any torn trailing bytes after it. A recovering worker truncates its
     * journal to this offset before appending anything new; see {@code OrderRouter}'s recovery
     * path and its Javadoc on why a leftover torn tail can never be allowed to sit ahead of newly
     * appended records.
     */
    public static long validByteLength(Path file) {
        return scan(file).validBytes();
    }

    private static ScanResult scan(Path file) {
        if (!Files.exists(file)) {
            return new ScanResult(List.of(), 0);
        }
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read journal file " + file, e);
        }

        List<JournalRecord> records = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long expectedSequence = -1;

        while (true) {
            Optional<byte[]> frame = readFrame(buffer);
            if (frame.isEmpty()) {
                break;
            }
            JournalRecord record = JournalCodec.decode(frame.get());
            if (expectedSequence != -1 && record.journalSequence() != expectedSequence) {
                throw new JournalCorruptionException(
                        "journal " + file + " expected sequence " + expectedSequence
                                + " but found " + record.journalSequence());
            }
            records.add(record);
            expectedSequence = record.journalSequence() + 1;
        }
        // readFrame rewinds to the frame's start before returning empty, so the buffer's position
        // here is exactly the end of the last complete record - the torn tail, if any, is whatever
        // comes after it.
        return new ScanResult(List.copyOf(records), buffer.position());
    }

    private record ScanResult(List<JournalRecord> records, long validBytes) {}

    /** Empty means "nothing more to read" — a clean end of file or an unusable torn tail. */
    private static Optional<byte[]> readFrame(ByteBuffer buffer) {
        if (buffer.remaining() < Integer.BYTES) {
            return Optional.empty();
        }
        int mark = buffer.position();
        int length = buffer.getInt();
        if (length < 0 || buffer.remaining() < length) {
            // Torn: either the length prefix itself was cut short at write time and now decodes
            // to nonsense, or it is intact but the promised bytes never made it to disk. Rewind
            // and stop; there is nothing salvageable from a partially written record.
            buffer.position(mark);
            return Optional.empty();
        }
        byte[] content = new byte[length];
        buffer.get(content);
        return Optional.of(content);
    }
}
