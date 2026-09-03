package com.tradingplatform.portfolio.config;

import com.tradingplatform.portfolio.PortfolioProcessor;
import com.tradingplatform.portfolio.PortfolioReadModel;
import com.tradingplatform.portfolio.PortfolioRepository;
import com.tradingplatform.portfolio.PortfolioUpdater;
import com.tradingplatform.portfolio.PositionReconciliation;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Wires the durable portfolio on top of whatever {@link javax.sql.DataSource} and Flyway migration
 * the host application configures.
 *
 * <p>This module declares no {@code DataSource} of its own. Owning connection pooling and schema
 * migration here would mean every host application inherits this module's opinion about pool size
 * and driver configuration; instead it consumes whatever `spring-boot-starter-jdbc` and Flyway
 * auto-configure from {@code spring.datasource.*}, which is the same mechanism any other Spring Boot
 * persistence module would use.
 *
 * <p>Gated by the same flag as the Kafka pipeline: every position here is derived from the
 * execution stream, so there is nothing to persist and nothing to consume when the pipeline is
 * switched off. This is also what lets the Stage 3 gateway tests run with no database at all.
 */
@Configuration
@ConditionalOnProperty(name = "trading.pipeline.enabled", havingValue = "true", matchIfMissing = true)
public class PortfolioConfiguration {

    @Bean
    public PortfolioRepository portfolioRepository(NamedParameterJdbcTemplate jdbc) {
        return new PortfolioRepository(jdbc);
    }

    @Bean
    public PortfolioUpdater portfolioUpdater(PortfolioRepository repository) {
        return new PortfolioUpdater(repository);
    }

    @Bean
    public PortfolioProcessor portfolioProcessor(PortfolioUpdater updater, MeterRegistry meters) {
        return new PortfolioProcessor(updater, meters);
    }

    @Bean
    public PositionReconciliation positionReconciliation(PortfolioRepository repository) {
        return new PositionReconciliation(repository);
    }

    @Bean
    public PortfolioReadModel portfolioReadModel(PortfolioRepository repository) {
        return new PortfolioReadModel(repository);
    }
}
