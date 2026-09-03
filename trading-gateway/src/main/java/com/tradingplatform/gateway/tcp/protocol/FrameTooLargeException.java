package com.tradingplatform.gateway.tcp.protocol;

/**
 * A frame's declared length exceeds the configured bound. Checked before a single byte of payload
 * is read or allocated — unlike the matching-engine journal's own framing (see {@code
 * JournalReader}), which trusts a file it wrote itself, a socket carries untrusted input, and a
 * length prefix is exactly the kind of field a hostile or broken client can set to something
 * absurd to force a large allocation.
 */
public final class FrameTooLargeException extends TcpProtocolException {
    public FrameTooLargeException(int declaredLength, int maxFrameLength) {
        super("frame length " + declaredLength + " exceeds maximum " + maxFrameLength);
    }
}
