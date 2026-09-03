package com.tradingplatform.gateway.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Gateway configuration. Credentials and the instrument list are bound as single compact strings so
 * that every deployment target can supply them through one environment variable each, rather than
 * needing an indexed property per entry.
 *
 * @param apiKeys {@code apiKey:accountId} pairs separated by commas
 * @param instruments {@code SYMBOL:referencePriceTicks} pairs separated by commas; a reference
 *     price of 0 means "unknown until the instrument trades"
 */
@ConfigurationProperties(prefix = "trading")
public record GatewayProperties(
        String apiKeys, String instruments, Matching matching, Risk risk, RateLimit rateLimit) {

    /**
     * @param journalEnabled when false, the matching engine is exactly as durable as it was before
     *     Stage 7 — nothing, and every test written before this stage keeps that behaviour
     * @param journalDirectory where each worker's append-only command log lives; ignored when
     *     {@code journalEnabled} is false
     * @param journalFlushPolicy {@code EVERY_RECORD} (default, fsync per command) or
     *     {@code BUFFERED}; see {@code FlushPolicy} and ADR-015
     */
    public record Matching(
            int workerCount,
            int queueCapacity,
            Duration commandTimeout,
            boolean journalEnabled,
            String journalDirectory,
            String journalFlushPolicy) {}

    public record Risk(
            long maxOrderQuantity,
            long maxOrderNotional,
            int maxOpenOrders,
            long maxPositionQuantity) {}

    public record RateLimit(double permitsPerSecond, long burst) {}
}
