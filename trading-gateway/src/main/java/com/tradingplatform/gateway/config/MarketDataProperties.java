package com.tradingplatform.gateway.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param enabled when false, no simulator, hub or WebSocket endpoint is wired at all — the gateway
 *     trades exactly as it does without Stage 6, which is how the Stage 1-5 tests run with no
 *     market-data machinery present
 * @param symbols the simulated feed's universe
 * @param eventsPerSecond aggregate rate across every symbol, not per symbol
 * @param seed makes a run reproducible, the same discipline {@code OrderFlowSimulator} uses
 * @param subscriberQueueCapacity per-WebSocket-subscriber bounded queue size; see ADR-013
 * @param maxConsecutiveDrops consecutive dropped updates before a stuck subscriber is disconnected
 * @param markPriceFlushInterval how often {@code MarkPriceUpdater} writes changed marks to Postgres
 * @param codec {@code json} or {@code binary}; see ADR-014
 */
@ConfigurationProperties(prefix = "trading.marketdata")
public record MarketDataProperties(
        boolean enabled,
        List<String> symbols,
        int eventsPerSecond,
        long seed,
        int subscriberQueueCapacity,
        int maxConsecutiveDrops,
        Duration markPriceFlushInterval,
        String codec) {}
