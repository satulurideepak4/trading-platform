package com.tradingplatform.gateway.config;

import com.tradingplatform.gateway.api.AuthenticatedClientArgumentResolver;
import com.tradingplatform.gateway.api.ClientAuthenticationInterceptor;
import com.tradingplatform.gateway.auth.ClientAuthenticator;
import com.tradingplatform.gateway.ratelimit.TokenBucketRateLimiter;
import java.util.List;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfiguration implements WebMvcConfigurer {
    private final ClientAuthenticator authenticator;
    private final TokenBucketRateLimiter rateLimiter;

    public WebConfiguration(
            ClientAuthenticator authenticator, TokenBucketRateLimiter rateLimiter) {
        this.authenticator = authenticator;
        this.rateLimiter = rateLimiter;
    }

    /**
     * Trading paths are listed explicitly rather than intercepting everything and excluding health
     * endpoints, so a new path is unauthenticated only if someone deliberately leaves it off.
     *
     * <p>{@code /marketdata/**} and the {@code /marketdata} WebSocket endpoint are deliberately not
     * listed here. Every other path carries per-account state — an order, a position, a risk
     * limit — and needs to know who is asking; market data does not. It is undifferentiated
     * reference data, the same way a real exchange's public feed has no notion of which client is
     * reading it, so it is exempt from authentication and rate limiting by design, not oversight.
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new ClientAuthenticationInterceptor(authenticator, rateLimiter))
                .addPathPatterns(
                        "/orders/**",
                        "/orders",
                        "/orderbook/**",
                        "/positions",
                        "/executions",
                        "/pnl",
                        "/risk/exposure",
                        "/instruments/**",
                        // Stage 10's HTTP+Protobuf order-entry path: same account-scoped state as
                        // /orders, listed separately because it is a separate path, not because it
                        // needs different treatment.
                        "/protobuf/orders/**",
                        "/protobuf/orders");
    }

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(new AuthenticatedClientArgumentResolver());
    }
}
