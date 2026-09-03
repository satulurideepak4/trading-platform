package com.tradingplatform.gateway.config;

import com.tradingplatform.gateway.marketdata.MarkPriceUpdater;
import com.tradingplatform.marketdata.process.MarketDataProcessor;
import com.tradingplatform.portfolio.PortfolioRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The one piece of Stage 6 that touches Postgres: periodically writing the market-data feed's
 * reference price into {@code mark_prices} so unrealized P&L moves with the market. See
 * {@link MarkPriceUpdater}, whose {@code flush()} method carries the {@code @Scheduled} annotation
 * directly — not a wrapper method here — precisely so this class only ever needs to build the bean,
 * never receive it back through its own constructor; see {@link MarketDataWebSocketConfiguration}'s
 * Javadoc for what goes wrong when a {@code @Configuration} class does that to itself.
 *
 * <p>Conditional on <b>both</b> {@code trading.marketdata.enabled} and
 * {@code trading.pipeline.enabled}, not just the first. {@link PortfolioRepository} only exists
 * when the pipeline is switched on — see {@code PortfolioConfiguration} — and a gateway running
 * with the pipeline off (every Stage 1-3 behaviour test does this) must not fail to start just
 * because market data is otherwise enabled by default. Kept as its own class rather than folded
 * into {@link MarketDataConfiguration} for exactly that reason: that class's own beans must never
 * gain a hard dependency on Postgres being present.
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(
        name = {"trading.marketdata.enabled", "trading.pipeline.enabled"},
        havingValue = "true",
        matchIfMissing = true)
public class MarketDataPortfolioBridgeConfiguration {

    @Bean
    public MarkPriceUpdater markPriceUpdater(MarketDataProcessor processor, PortfolioRepository repository) {
        return new MarkPriceUpdater(processor, repository);
    }
}
