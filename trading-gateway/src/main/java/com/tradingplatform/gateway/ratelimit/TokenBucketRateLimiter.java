package com.tradingplatform.gateway.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Per-account token bucket.
 *
 * <p>A token bucket is used instead of a fixed window because trading clients legitimately send
 * bursts: a strategy repricing a book sends many orders in a few milliseconds and then goes quiet.
 * A fixed window either rejects that burst or, if sized for it, allows twice the intended rate
 * across a window boundary. The bucket separates the two concerns: {@code burst} is how much
 * short-term bunching is tolerated, {@code permitsPerSecond} is the sustained rate.
 *
 * <p>Buckets refill lazily on access rather than from a timer thread, so an idle account costs
 * nothing. Each bucket is independently synchronized; accounts never contend with each other.
 */
public final class TokenBucketRateLimiter {
    private final double permitsPerNanosecond;
    private final double burst;
    private final LongSupplier nanoClock;
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(double permitsPerSecond, long burst) {
        this(permitsPerSecond, burst, System::nanoTime);
    }

    TokenBucketRateLimiter(double permitsPerSecond, long burst, LongSupplier nanoClock) {
        if (permitsPerSecond <= 0 || burst <= 0) {
            throw new IllegalArgumentException("permitsPerSecond and burst must be positive");
        }
        this.permitsPerNanosecond = permitsPerSecond / 1_000_000_000.0;
        this.burst = burst;
        this.nanoClock = nanoClock;
    }

    public RateLimitDecision tryAcquire(String accountId) {
        return buckets
                .computeIfAbsent(accountId, ignored -> new Bucket(burst, nanoClock.getAsLong()))
                .tryAcquire(nanoClock.getAsLong(), permitsPerNanosecond, burst);
    }

    private static final class Bucket {
        private double tokens;
        private long lastRefillNanos;

        private Bucket(double initialTokens, long nowNanos) {
            this.tokens = initialTokens;
            this.lastRefillNanos = nowNanos;
        }

        private synchronized RateLimitDecision tryAcquire(
                long nowNanos, double permitsPerNanosecond, double capacity) {
            long elapsedNanos = nowNanos - lastRefillNanos;
            if (elapsedNanos > 0) {
                tokens = Math.min(capacity, tokens + elapsedNanos * permitsPerNanosecond);
                lastRefillNanos = nowNanos;
            }
            if (tokens >= 1) {
                tokens -= 1;
                return RateLimitDecision.allow();
            }
            double nanosUntilOnePermit = (1 - tokens) / permitsPerNanosecond;
            return RateLimitDecision.throttle((long) Math.ceil(nanosUntilOnePermit / 1_000_000));
        }
    }
}
