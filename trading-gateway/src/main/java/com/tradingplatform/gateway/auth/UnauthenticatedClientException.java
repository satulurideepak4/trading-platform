package com.tradingplatform.gateway.auth;

/** Carries no credential material, so it is safe to log and to convert into a response. */
public final class UnauthenticatedClientException extends RuntimeException {

    public UnauthenticatedClientException(String message) {
        super(message);
    }
}
