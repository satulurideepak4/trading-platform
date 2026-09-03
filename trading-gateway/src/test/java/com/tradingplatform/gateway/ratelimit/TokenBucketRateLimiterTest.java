package com.tradingplatform.gateway.ratelimit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TokenBucketRateLimiterTest {
    private final AtomicLong nanoClock = new AtomicLong();

    @Test
    void allowsAFullBurstThenThrottles() {
        TokenBucketRateLimiter limiter = limiter(100, 5);

        for (int index = 0; index < 5; index++) {
            assertTrue(limiter.tryAcquire("ACC-1").allowed(), "permit " + index);
        }
        assertFalse(limiter.tryAcquire("ACC-1").allowed());
    }

    @Test
    void refillsAtTheConfiguredRate() {
        TokenBucketRateLimiter limiter = limiter(100, 1);
        assertTrue(limiter.tryAcquire("ACC-1").allowed());

        advanceMillis(5);
        assertFalse(limiter.tryAcquire("ACC-1").allowed());

        // 100 permits per second is one permit every 10ms.
        advanceMillis(5);
        assertTrue(limiter.tryAcquire("ACC-1").allowed());
    }

    @Test
    void doesNotAccumulateMoreThanTheBurstWhileIdle() {
        TokenBucketRateLimiter limiter = limiter(100, 3);

        advanceMillis(10_000);

        assertTrue(limiter.tryAcquire("ACC-1").allowed());
        assertTrue(limiter.tryAcquire("ACC-1").allowed());
        assertTrue(limiter.tryAcquire("ACC-1").allowed());
        assertFalse(limiter.tryAcquire("ACC-1").allowed());
    }

    @Test
    void reportsHowLongToWaitForTheNextPermit() {
        TokenBucketRateLimiter limiter = limiter(100, 1);
        limiter.tryAcquire("ACC-1");

        RateLimitDecision decision = limiter.tryAcquire("ACC-1");

        assertFalse(decision.allowed());
        assertEquals(10, decision.retryAfterMillis());
    }

    @Test
    void budgetsAreIndependentPerAccount() {
        TokenBucketRateLimiter limiter = limiter(100, 2);

        assertTrue(limiter.tryAcquire("ACC-1").allowed());
        assertTrue(limiter.tryAcquire("ACC-1").allowed());
        assertFalse(limiter.tryAcquire("ACC-1").allowed());

        assertTrue(limiter.tryAcquire("ACC-2").allowed());
        assertTrue(limiter.tryAcquire("ACC-2").allowed());
    }

    @Test
    void concurrentCallersNeverExceedTheBurst() throws Exception {
        int burst = 50;
        TokenBucketRateLimiter limiter = limiter(1, burst);
        AtomicInteger granted = new AtomicInteger();

        try (ExecutorService threads = Executors.newFixedThreadPool(8)) {
            CountDownLatch start = new CountDownLatch(1);
            for (int index = 0; index < burst * 4; index++) {
                threads.submit(() -> {
                    start.await();
                    if (limiter.tryAcquire("ACC-1").allowed()) {
                        granted.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
            threads.shutdown();
            assertTrue(threads.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertEquals(burst, granted.get());
    }

    private TokenBucketRateLimiter limiter(double permitsPerSecond, long burst) {
        return new TokenBucketRateLimiter(permitsPerSecond, burst, nanoClock::get);
    }

    private void advanceMillis(long millis) {
        nanoClock.addAndGet(millis * 1_000_000);
    }
}
