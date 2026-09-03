package com.tradingplatform.gateway.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * Static API-key authentication for local and test environments.
 *
 * <p>Keys are loaded once at startup and never logged. Lookup is a plain hash lookup on an
 * immutable map, which keeps authentication off the list of things that can add latency or
 * contention to order ingress.
 *
 * <p>This is deliberately the weakest realistic mechanism: it has no rotation, no expiry and no
 * constant-time comparison. It is enough to prove that identity is established before risk and
 * routing run, and it is the piece a real deployment would replace first.
 */
public final class ApiKeyAuthenticator implements ClientAuthenticator {
    /** Bounded so that {@code accountId + ":" + clientOrderId} fits the engine's 64-char field. */
    public static final int MAX_ACCOUNT_ID_LENGTH = 31;

    private final Map<String, AuthenticatedClient> clientsByApiKey;

    private ApiKeyAuthenticator(Map<String, AuthenticatedClient> clientsByApiKey) {
        this.clientsByApiKey = clientsByApiKey;
    }

    /**
     * Parses {@code apiKey:accountId} pairs separated by commas.
     *
     * @throws IllegalArgumentException if the configuration is empty or malformed, so that a
     *     misconfigured gateway fails at startup instead of rejecting every order at runtime
     */
    public static ApiKeyAuthenticator fromConfiguration(String configuredPairs) {
        if (configuredPairs == null || configuredPairs.isBlank()) {
            throw new IllegalArgumentException(
                    "no API keys configured; set GATEWAY_API_KEYS to apiKey:accountId pairs");
        }

        Map<String, AuthenticatedClient> clients = new HashMap<>();
        for (String entry : configuredPairs.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int separator = trimmed.indexOf(':');
            if (separator <= 0 || separator == trimmed.length() - 1) {
                throw new IllegalArgumentException(
                        "API key entry must be apiKey:accountId but had " + trimmed.length()
                                + " characters in an unexpected shape");
            }
            String apiKey = trimmed.substring(0, separator);
            String accountId = trimmed.substring(separator + 1);
            if (accountId.length() > MAX_ACCOUNT_ID_LENGTH) {
                throw new IllegalArgumentException(
                        "account id " + accountId + " exceeds " + MAX_ACCOUNT_ID_LENGTH
                                + " characters");
            }
            if (clients.putIfAbsent(apiKey, new AuthenticatedClient(accountId)) != null) {
                // The offending key is not echoed: it is a credential.
                throw new IllegalArgumentException("the same API key is configured twice");
            }
        }
        if (clients.isEmpty()) {
            throw new IllegalArgumentException("no usable API keys were configured");
        }
        return new ApiKeyAuthenticator(Map.copyOf(clients));
    }

    @Override
    public AuthenticatedClient authenticate(String credential) {
        if (credential == null || credential.isBlank()) {
            throw new UnauthenticatedClientException("missing API key");
        }
        AuthenticatedClient client = clientsByApiKey.get(credential);
        if (client == null) {
            throw new UnauthenticatedClientException("unrecognised API key");
        }
        return client;
    }
}
