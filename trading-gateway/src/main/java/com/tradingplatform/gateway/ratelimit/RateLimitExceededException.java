package com.tradingplatform.gateway.ratelimit;

public final class RateLimitExceededException extends RuntimeException {
    private final long retryAfterMillis;

    public RateLimitExceededException(String accountId, long retryAfterMillis) {
        super("request rate limit exceeded for account " + accountId);
        this.retryAfterMillis = retryAfterMillis;
    }

    public long retryAfterMillis() {
        return retryAfterMillis;
    }
}
