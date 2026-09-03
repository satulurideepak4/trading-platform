package com.tradingplatform.gateway.auth;

import java.util.Objects;

/**
 * The identity every request is processed under. Risk limits, idempotency keys, rate limits and
 * order ownership are all scoped to {@code accountId}, so this is the only piece of the credential
 * that the rest of the gateway ever sees.
 */
public record AuthenticatedClient(String accountId) {

    public AuthenticatedClient {
        Objects.requireNonNull(accountId, "accountId");
    }
}
