package com.tradingplatform.gateway.auth;

/**
 * Turns a transport credential into a trading identity.
 *
 * <p>This exists as an interface because the credential mechanism is expected to change (mutual
 * TLS, a signed session token, an upstream identity provider) while everything downstream only ever
 * needs an account. Swapping the mechanism must not touch risk, idempotency or routing.
 */
public interface ClientAuthenticator {

    /**
     * @throws UnauthenticatedClientException if the credential is missing or not recognised
     */
    AuthenticatedClient authenticate(String credential);
}
