package com.tradingplatform.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradingplatform.gateway.marketdata.MarketDataWebSocketHandler;
import com.tradingplatform.marketdata.codec.BinaryMarketDataCodec;
import com.tradingplatform.marketdata.codec.JsonMarketDataCodec;
import com.tradingplatform.marketdata.codec.MarketDataCodec;
import com.tradingplatform.marketdata.process.MarketDataMetrics;
import com.tradingplatform.marketdata.process.MarketDataProcessor;
import com.tradingplatform.marketdata.publish.MarketDataHub;
import com.tradingplatform.marketdata.simulator.FeedImperfectionPolicy;
import com.tradingplatform.marketdata.simulator.MarketSimulator;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Wires the Stage 6 market-data path: a simulated feed, the processor that cleans it up, the hub
 * that fans it out, and the WebSocket endpoint that consumes it.
 *
 * <p>Gated on its own flag rather than {@code trading.pipeline.enabled} — market data has nothing
 * to do with Kafka or Postgres and can be switched off independently, which is also what lets the
 * Stage 1-5 tests that predate this class run with no market-data machinery present at all. The one
 * part of Stage 6 that does need Postgres — writing market prices into the durable portfolio — is
 * kept in {@link MarketDataPortfolioBridgeConfiguration}, imported below but conditional on
 * {@code trading.pipeline.enabled} too, so this class never has a hard dependency on
 * {@code PortfolioRepository} existing.
 *
 * <p>The actual WebSocket registration lives in {@link MarketDataWebSocketConfiguration}, also
 * imported below, rather than here — see that class's Javadoc for why this class cannot both build
 * {@link MarketDataWebSocketHandler} and receive it back through its own constructor.
 *
 * <p>No authentication on {@code /marketdata}, deliberately: unlike {@code /orders} or
 * {@code /positions}, this is undifferentiated reference data with no per-account state, the same
 * way a real exchange's public feed carries no notion of which client is reading it. See
 * {@link WebConfiguration}.
 */
@Configuration
@EnableConfigurationProperties(MarketDataProperties.class)
@ConditionalOnProperty(name = "trading.marketdata.enabled", havingValue = "true", matchIfMissing = true)
@Import({MarketDataWebSocketConfiguration.class, MarketDataPortfolioBridgeConfiguration.class})
public class MarketDataConfiguration {

    /**
     * Deliberately not a {@code @Bean}. A second context-wide {@code ObjectMapper} bean — alongside
     * {@code tradingEventObjectMapper} — leaves Spring's own Jackson autoconfiguration with two
     * equally good candidates for the HTTP message converter and no way to pick one. This feature
     * only ever needs its own mapper inside its own two factory methods below, so it stays a plain
     * helper instead; the reasoning for owning one at all is the same as
     * {@code tradingEventObjectMapper}'s: its wire format must not change as a side effect of
     * someone adjusting HTTP JSON settings.
     */
    private static ObjectMapper marketDataObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public MarketDataCodec marketDataCodec(MarketDataProperties properties) {
        String codec = properties.codec() == null ? "json" : properties.codec().toLowerCase(Locale.ROOT);
        return switch (codec) {
            case "binary" -> new BinaryMarketDataCodec();
            case "json" -> new JsonMarketDataCodec(marketDataObjectMapper());
            default -> throw new IllegalArgumentException(
                    "trading.marketdata.codec must be 'json' or 'binary', was " + properties.codec());
        };
    }

    @Bean
    public MarketDataMetrics marketDataMetrics(MeterRegistry meters) {
        return new MarketDataMetrics(meters);
    }

    @Bean
    public MarketDataHub marketDataHub(MarketDataProperties properties, MeterRegistry meters) {
        return new MarketDataHub(
                properties.subscriberQueueCapacity(), properties.maxConsecutiveDrops(), meters);
    }

    @Bean
    public MarketDataProcessor marketDataProcessor(MarketDataHub hub, MarketDataMetrics metrics) {
        return new MarketDataProcessor(hub, metrics);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public MarketSimulator marketSimulator(MarketDataProperties properties, MarketDataProcessor processor) {
        return new MarketSimulator(
                properties.symbols(),
                properties.eventsPerSecond(),
                properties.seed(),
                FeedImperfectionPolicy.DEFAULT,
                processor);
    }

    @Bean
    public MarketDataWebSocketHandler marketDataWebSocketHandler(
            MarketDataHub hub, MarketDataProcessor processor, MarketDataCodec codec) {
        return new MarketDataWebSocketHandler(hub, processor, codec, marketDataObjectMapper());
    }
}
