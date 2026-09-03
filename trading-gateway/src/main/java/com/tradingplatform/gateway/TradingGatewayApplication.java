package com.tradingplatform.gateway;

import com.tradingplatform.gateway.config.GatewayProperties;
import com.tradingplatform.gateway.config.MarketDataConfiguration;
import com.tradingplatform.pipeline.config.ExecutionPipelineConfiguration;
import com.tradingplatform.portfolio.config.PortfolioConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

/**
 * Runs the order gateway, the downstream event consumers, the durable portfolio, and the
 * market-data feed in one process.
 *
 * <p>Each is imported explicitly rather than found by component scanning, so that "this process also
 * consumes the execution stream and writes positions to Postgres" is visible here rather than
 * inferred from a package name. Co-location is deliberate: the risk projection the pipeline
 * consumers maintain is read on the order-admission path, and that read has to be a field access
 * rather than a network call.
 */
@SpringBootApplication
@EnableConfigurationProperties(GatewayProperties.class)
@Import({ExecutionPipelineConfiguration.class, PortfolioConfiguration.class, MarketDataConfiguration.class})
public class TradingGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradingGatewayApplication.class, args);
    }
}
