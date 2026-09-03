package com.tradingplatform.portfolio;

import com.tradingplatform.portfolio.config.PortfolioConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Exists only so this module's tests can start a real Spring context — with transaction proxying,
 * JDBC and Flyway auto-configured from {@code spring.datasource.*} — without depending on
 * trading-gateway, which is the module that normally hosts {@link PortfolioConfiguration}.
 *
 * <p>The host application supplies {@code MeterRegistry} through {@code spring-boot-starter-
 * actuator}, which this module deliberately does not depend on. A plain in-memory registry stands
 * in for it here.
 *
 * <p>{@code KafkaAutoConfiguration} is excluded because {@link PortfolioProcessor}'s
 * {@code @KafkaListener} names a container factory that only exists in {@code execution-pipeline}'s
 * {@code KafkaPipelineConfiguration}, which these tests have no reason to bring in — they exercise
 * the JDBC and transaction behaviour directly, never through Kafka.
 */
@SpringBootApplication(exclude = KafkaAutoConfiguration.class)
@Import(PortfolioConfiguration.class)
public class PortfolioTestApplication {

    @Bean
    MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }
}
