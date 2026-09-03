package com.tradingplatform.gateway.ratelimit;

/**
 * @param retryAfterMillis how long the caller must wait for one permit; 0 when allowed
 */
public record RateLimitDecision(boolean allowed, long retryAfterMillis) {

    private static final RateLimitDecision ALLOWED = new RateLimitDecision(true, 0);

    public static RateLimitDecision allow() {
        return ALLOWED;
    }

    public static RateLimitDecision throttle(long retryAfterMillis) {
        return new RateLimitDecision(false, Math.max(1, retryAfterMillis));
    }
}
