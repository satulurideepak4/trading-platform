package com.tradingplatform.pipeline.config;

import com.tradingplatform.pipeline.publish.NoOpTradingEventPublisher;
import com.tradingplatform.pipeline.publish.TradingEventPublisher;
import com.tradingplatform.pipeline.store.AuditLog;
import com.tradingplatform.pipeline.store.ExecutionStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The pipeline's entry point. Imported explicitly by the application rather than found by component
 * scanning, so it is obvious from the application class that these consumers are running in this
 * process.
 *
 * <p>The stores are declared here unconditionally. They are plain in-memory holders, and having
 * them present when the pipeline is switched off means nothing else has to be conditional.
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
@Import(KafkaPipelineConfiguration.class)
public class ExecutionPipelineConfiguration {

    @Bean
    public ExecutionStore executionStore() {
        return new ExecutionStore();
    }

    @Bean
    public AuditLog auditLog(PipelineProperties properties) {
        return new AuditLog(properties.auditLogMaxEntries());
    }

    /**
     * Matched only when the pipeline is explicitly disabled, rather than relying on bean-ordering
     * conditions, so that which publisher is active is decided by configuration alone.
     */
    @Bean
    @ConditionalOnProperty(name = "trading.pipeline.enabled", havingValue = "false")
    public TradingEventPublisher noOpTradingEventPublisher() {
        return new NoOpTradingEventPublisher();
    }
}
