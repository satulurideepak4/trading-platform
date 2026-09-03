package com.tradingplatform.gateway.tcp.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Optional;

/**
 * Length-prefixed message framing: a 4-byte big-endian length, then exactly that many payload
 * bytes. The same shape {@code matching-engine}'s {@code JournalCodec}/{@code FileCommandJournal}
 * already use for the same reason — a fixed-size prefix is the simplest way to know where one
 * record ends and the next begins without a delimiter that payload bytes could themselves contain.
 *
 * <p>Unlike the journal's file-based framing, a socket is not guaranteed to hand back a whole frame
 * — or even a whole length prefix — in one {@code read()} call: TCP is a byte stream, not a message
 * stream, and the kernel is free to split a write across as many reads as it likes on the receiving
 * end. {@link DataInputStream#readFully} is what makes this correct regardless: it loops internally
 * until the requested number of bytes has actually arrived, or the stream ends first.
 */
public final class FrameIO {
    private FrameIO() {}

    public static void writeFrame(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    /**
     * Empty means the stream ended cleanly between frames — the ordinary way a connection closes,
     * not a protocol error.
     *
     * @throws FrameTooLargeException if the declared length exceeds {@code maxFrameLength}, checked
     *     before any payload bytes are read or a buffer for them is allocated
     */
    public static Optional<byte[]> readFrame(DataInputStream in, int maxFrameLength) throws IOException {
        int length;
        try {
            length = in.readInt();
        } catch (EOFException endOfStream) {
            return Optional.empty();
        }
        if (length < 0 || length > maxFrameLength) {
            throw new FrameTooLargeException(length, maxFrameLength);
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return Optional.of(payload);
    }
}
