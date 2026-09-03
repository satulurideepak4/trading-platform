package com.tradingplatform.gateway.config;

import com.tradingplatform.gateway.auth.ClientAuthenticator;
import com.tradingplatform.gateway.ingress.OrderIngressService;
import com.tradingplatform.gateway.tcp.ExecutionPushingTradingEventPublisher;
import com.tradingplatform.gateway.tcp.TcpConnectionRegistry;
import com.tradingplatform.gateway.tcp.TcpServer;
import com.tradingplatform.pipeline.publish.TradingEventPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Wires the persistent TCP order-entry path (ADR-016) alongside the REST one. Absent entirely when
 * {@code trading.tcp.enabled=false} — every earlier stage's tests run with no TCP machinery
 * present, the same guarantee {@link MarketDataConfiguration} already makes for market data.
 *
 * <p>{@link ExecutionPushingTradingEventPublisher} is declared {@link Primary} here specifically so
 * {@code OrderIngressService}'s own {@code TradingEventPublisher} constructor parameter resolves to
 * it instead of the plain delegate, with no {@code @Qualifier} needed: Spring excludes a bean under
 * construction from its own dependency candidates, so this class's {@code delegate} parameter below
 * resolves unambiguously to whichever single {@code TradingEventPublisher} {@code
 * ExecutionPipelineConfiguration}/{@code KafkaPipelineConfiguration} already registered (they are
 * mutually exclusive via their own {@code trading.pipeline.enabled} conditions).
 */
@Configuration
@EnableConfigurationProperties(TcpProperties.class)
@ConditionalOnProperty(name = "trading.tcp.enabled", havingValue = "true", matchIfMissing = true)
public class TcpConfiguration {

    @Bean
    public TcpConnectionRegistry tcpConnectionRegistry(MeterRegistry meters) {
        return new TcpConnectionRegistry(meters);
    }

    @Bean
    @Primary
    public TradingEventPublisher tcpAwareTradingEventPublisher(
            TradingEventPublisher delegate, TcpConnectionRegistry registry) {
        return new ExecutionPushingTradingEventPublisher(delegate, registry);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public TcpServer tcpServer(
            TcpProperties properties,
            ClientAuthenticator authenticator,
            OrderIngressService ingress,
            TcpConnectionRegistry registry) {
        return new TcpServer(
                properties.port(),
                properties.maxConnections(),
                properties.dispatchPoolSize(),
                properties.dispatchQueueCapacity(),
                properties.authTimeout(),
                properties.idleTimeout(),
                properties.maxFrameLength(),
                properties.outboundQueueCapacity(),
                properties.maxConsecutiveDrops(),
                authenticator,
                ingress,
                registry);
    }
}
