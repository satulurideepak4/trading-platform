package com.tradingplatform.gateway.config;

import com.tradingplatform.gateway.marketdata.MarketDataWebSocketHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Registers the {@code /marketdata} endpoint against the handler {@link MarketDataConfiguration}
 * builds.
 *
 * <p>Kept as its own class rather than folded into {@link MarketDataConfiguration}: a
 * {@code @Configuration} class's own {@code @Bean} methods are instance methods on that same bean,
 * so a class cannot both define a bean and receive that same bean back through its own constructor
 * — Spring reports it as a {@code BeanCurrentlyInCreationException}, since building the instance
 * requires the constructor argument, and building that argument requires calling a method on the
 * not-yet-built instance. Splitting the handler's registration into a second class sidesteps it
 * without resorting to field injection.
 */
@Configuration
@EnableWebSocket
@ConditionalOnProperty(name = "trading.marketdata.enabled", havingValue = "true", matchIfMissing = true)
public class MarketDataWebSocketConfiguration implements WebSocketConfigurer {
    private final MarketDataWebSocketHandler handler;

    public MarketDataWebSocketConfiguration(MarketDataWebSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/marketdata").setAllowedOrigins("*");
    }
}
