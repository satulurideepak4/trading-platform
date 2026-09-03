package com.tradingplatform.matching.journal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * One worker's durable command log: an append-only file, one length-prefixed record per command.
 * Single-writer by contract — exactly one {@code MatchingWorker} thread calls {@link #append}, the
 * same single-writer-per-book discipline the matching engine already uses (ADR-002), so no locking
 * is needed here either.
 */
public final class FileCommandJournal implements CommandJournal {
    private final FileChannel channel;
    private final FlushPolicy flushPolicy;

    public FileCommandJournal(Path file, FlushPolicy flushPolicy) {
        this.flushPolicy = flushPolicy;
        try {
            if (file.toAbsolutePath().getParent() != null) {
                Files.createDirectories(file.toAbsolutePath().getParent());
            }
            this.channel = FileChannel.open(
                    file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("could not open journal file " + file, e);
        }
    }

    /** Where {@code workerId}'s journal lives under {@code directory} — the one place this name is spelled. */
    public static Path fileFor(Path directory, int workerId) {
        return directory.resolve("worker-" + workerId + ".journal");
    }

    @Override
    public void append(JournalRecord record) {
        byte[] content = JournalCodec.encode(record);
        ByteBuffer frame = ByteBuffer.allocate(4 + content.length);
        frame.putInt(content.length);
        frame.put(content);
        frame.flip();
        try {
            // APPEND mode makes each write() an atomic seek-to-end-and-write, so concurrent
            // readers never observe a write at any position but the true end of the file.
            while (frame.hasRemaining()) {
                channel.write(frame);
            }
            if (flushPolicy == FlushPolicy.EVERY_RECORD) {
                // false: sync file data, not the metadata (mtime etc.) a full fsync also pushes.
                channel.force(false);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not append to journal", e);
        }
    }

    @Override
    public void close() {
        try {
            // Unconditional: BUFFERED must still not lose whatever is sitting in the OS write
            // buffer at a clean shutdown, only at a crash.
            channel.force(true);
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("could not close journal", e);
        }
    }
}
