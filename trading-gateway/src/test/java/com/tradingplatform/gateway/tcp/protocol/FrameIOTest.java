package com.tradingplatform.gateway.tcp.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FrameIOTest {

    @Test
    void writeThenReadReturnsTheSamePayload() throws IOException {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        FrameIO.writeFrame(new DataOutputStream(buffer), payload);

        Optional<byte[]> read =
                FrameIO.readFrame(reader(buffer.toByteArray()), 1024);

        assertTrue(read.isPresent());
        assertArrayEquals(payload, read.get());
    }

    @Test
    void anEmptyStreamBetweenFramesReadsAsEmpty() throws IOException {
        Optional<byte[]> read = FrameIO.readFrame(reader(new byte[0]), 1024);

        assertTrue(read.isEmpty());
    }

    @Test
    void aDeclaredLengthOverTheBoundIsRejectedBeforeAllocatingAnything() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        new DataOutputStream(buffer).writeInt(10_000);

        FrameTooLargeException failure = assertThrows(
                FrameTooLargeException.class,
                () -> FrameIO.readFrame(reader(buffer.toByteArray()), 1024));
        assertTrue(failure.getMessage().contains("10000"));
    }

    /**
     * The whole point of using {@link DataInputStream#readFully} rather than a single {@code
     * read()} call: a socket is a byte stream, not a message stream, and a length prefix or a
     * payload can legitimately arrive split across several reads. This proves {@link
     * FrameIO#readFrame} is correct under exactly that condition, not just under the
     * single-read-returns-everything shape every other test here happens to exercise.
     */
    @Test
    void aFrameSplitAcrossManySmallReadsStillDecodesCorrectly() throws IOException {
        byte[] payload = "a longer payload than one fragment".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        FrameIO.writeFrame(new DataOutputStream(buffer), payload);

        DataInputStream fragmenting =
                new DataInputStream(new OneToThreeBytesAtATimeInputStream(buffer.toByteArray()));
        Optional<byte[]> read = FrameIO.readFrame(fragmenting, 1024);

        assertTrue(read.isPresent());
        assertArrayEquals(payload, read.get());
    }

    @Test
    void twoFramesBackToBackOnAFragmentingStreamBothDecodeCorrectly() throws IOException {
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second-message".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        FrameIO.writeFrame(out, first);
        FrameIO.writeFrame(out, second);

        DataInputStream fragmenting =
                new DataInputStream(new OneToThreeBytesAtATimeInputStream(buffer.toByteArray()));

        assertArrayEquals(first, FrameIO.readFrame(fragmenting, 1024).orElseThrow());
        assertArrayEquals(second, FrameIO.readFrame(fragmenting, 1024).orElseThrow());
        assertEquals(Optional.empty(), FrameIO.readFrame(fragmenting, 1024));
    }

    private static DataInputStream reader(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    /** Returns at most 3 bytes (often just 1) per {@code read()} call, regardless of how many the
     * caller asked for - deliberately hostile to any code that assumes one read fills a buffer. */
    private static final class OneToThreeBytesAtATimeInputStream extends FilterInputStream {
        private final java.util.SplittableRandom random = new java.util.SplittableRandom(7);

        OneToThreeBytesAtATimeInputStream(byte[] bytes) {
            super(new ByteArrayInputStream(bytes));
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int fragment = Math.min(len, 1 + random.nextInt(3));
            return super.read(b, off, fragment);
        }
    }
}
