package com.tradingplatform.gateway.tcp.protocol;

/**
 * The one byte at the start of every frame's payload. Deliberately not an enum whose {@code
 * ordinal()} gets written to the wire — see {@link WireCodes}'s Javadoc for why: a reordered or
 * inserted constant would silently change what an already-connected client's bytes mean.
 */
final class MessageType {
    // Client -> server.
    static final byte AUTH = 0;
    static final byte SUBMIT = 10;
    static final byte CANCEL = 11;
    static final byte REPLACE = 12;

    // Server -> client.
    static final byte AUTH_ACK = 1;
    static final byte AUTH_REJECT = 2;
    static final byte ACK = 13;
    static final byte EXECUTION_PUSH = 20;
    static final byte ERROR = 99;

    private MessageType() {}
}
