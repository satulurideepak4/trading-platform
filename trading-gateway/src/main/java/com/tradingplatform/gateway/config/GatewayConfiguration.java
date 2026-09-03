package com.tradingplatform.gateway.config;

import com.tradingplatform.gateway.auth.ApiKeyAuthenticator;
import com.tradingplatform.gateway.auth.ClientAuthenticator;
import com.tradingplatform.gateway.ingress.OrderIngressService;
import com.tradingplatform.gateway.ingress.OrderRegistry;
import com.tradingplatform.gateway.metrics.GatewayMetrics;
import com.tradingplatform.gateway.ratelimit.TokenBucketRateLimiter;
import com.tradingplatform.matching.journal.FlushPolicy;
import com.tradingplatform.matching.routing.OrderRouter;
import com.tradingplatform.pipeline.publish.TradingEventPublisher;
import com.tradingplatform.risk.InstrumentRegistry;
import com.tradingplatform.risk.PreTradeRiskEngine;
import com.tradingplatform.risk.RiskLimits;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the trading components as plain objects.
 *
 * <p>None of the matching, routing or risk classes know about Spring. The container decides their
 * lifetime and nothing more, which keeps them usable from benchmarks and tests without a context.
 */
@Configuration
public class GatewayConfiguration {

    /**
     * Declared as a bean so the container closes it on shutdown, which drains the worker queues
     * rather than dropping whatever they still hold — and, when the journal is enabled, closes each
     * worker's journal file only after that drain, per {@code OrderRouter.close()}. See ADR-015 for
     * why this gateway journals by default rather than trading Stage 5/6 durability away silently on
     * a restart.
     */
    @Bean(destroyMethod = "close")
    public OrderRouter orderRouter(GatewayProperties properties) {
        GatewayProperties.Matching matching = properties.matching();
        if (!matching.journalEnabled()) {
            return new OrderRouter(matching.workerCount(), matching.queueCapacity());
        }
        return new OrderRouter(
                matching.workerCount(),
                matching.queueCapacity(),
                Path.of(matching.journalDirectory()),
                parseFlushPolicy(matching.journalFlushPolicy()));
    }

    private static FlushPolicy parseFlushPolicy(String configured) {
        if (configured == null || configured.isBlank()) {
            return FlushPolicy.EVERY_RECORD;
        }
        return FlushPolicy.valueOf(configured.toUpperCase(Locale.ROOT));
    }

    @Bean
    public InstrumentRegistry instrumentRegistry(GatewayProperties properties) {
        return new InstrumentRegistry(parseInstruments(properties.instruments()));
    }

    @Bean
    public PreTradeRiskEngine preTradeRiskEngine(
            InstrumentRegistry instruments, GatewayProperties properties) {
        GatewayProperties.Risk risk = properties.risk();
        return new PreTradeRiskEngine(
                instruments,
                new RiskLimits(
                        risk.maxOrderQuantity(),
                        risk.maxOrderNotional(),
                        risk.maxOpenOrders(),
                        risk.maxPositionQuantity()));
    }

    @Bean
    public OrderRegistry orderRegistry() {
        return new OrderRegistry();
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public GatewayMetrics gatewayMetrics(MeterRegistry meters) {
        return new GatewayMetrics(meters);
    }

    @Bean
    public OrderIngressService orderIngressService(
            OrderRouter router,
            PreTradeRiskEngine riskEngine,
            OrderRegistry registry,
            TradingEventPublisher publisher,
            GatewayProperties properties,
            Clock clock,
            GatewayMetrics metrics) {
        return new OrderIngressService(
                router,
                riskEngine,
                registry,
                publisher,
                properties.matching().commandTimeout(),
                clock,
                metrics);
    }

    @Bean
    public ClientAuthenticator clientAuthenticator(GatewayProperties properties) {
        return ApiKeyAuthenticator.fromConfiguration(properties.apiKeys());
    }

    @Bean
    public TokenBucketRateLimiter rateLimiter(GatewayProperties properties) {
        GatewayProperties.RateLimit rateLimit = properties.rateLimit();
        return new TokenBucketRateLimiter(rateLimit.permitsPerSecond(), rateLimit.burst());
    }

    /** Parses {@code SYMBOL:referencePrice} pairs separated by commas. */
    private static List<InstrumentRegistry.Instrument> parseInstruments(String configured) {
        if (configured == null || configured.isBlank()) {
            throw new IllegalArgumentException(
                    "no instruments configured; set GATEWAY_INSTRUMENTS to SYMBOL:price pairs");
        }
        List<InstrumentRegistry.Instrument> instruments = new ArrayList<>();
        for (String entry : configured.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split(":", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "instrument entry must be SYMBOL:referencePrice but was " + trimmed);
            }
            instruments.add(new InstrumentRegistry.Instrument(
                    parts[0].trim(), Long.parseLong(parts[1].trim())));
        }
        if (instruments.isEmpty()) {
            throw new IllegalArgumentException("no usable instruments were configured");
        }
        return instruments;
    }
}
