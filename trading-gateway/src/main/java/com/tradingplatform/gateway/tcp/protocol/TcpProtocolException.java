package com.tradingplatform.gateway.tcp.protocol;

/**
 * A frame arrived that could not be decoded as this protocol: an unrecognized message type, an
 * unknown wire code, a truncated payload. Always a reason to close the connection — unlike an
 * application-level rejection (a bad order), which gets an {@code ACK}, this means the two ends no
 * longer agree on what the bytes mean, and there is nothing safe to keep reading.
 */
public class TcpProtocolException extends RuntimeException {
    public TcpProtocolException(String message) {
        super(message);
    }

    public TcpProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
