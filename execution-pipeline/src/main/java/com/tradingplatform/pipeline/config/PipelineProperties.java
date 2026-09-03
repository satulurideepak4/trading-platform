package com.tradingplatform.pipeline.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param enabled when false the application trades normally but publishes nothing and starts no
 *     consumers, which is how the matching and gateway tests run without a broker
 * @param publishQueueCapacity events buffered between the matching workers and the Kafka producer
 * @param auditLogMaxEntries bound on the in-process audit window; Kafka retains the real record
 */
@ConfigurationProperties(prefix = "trading.pipeline")
public record PipelineProperties(
        boolean enabled,
        int publishQueueCapacity,
        int topicPartitions,
        short topicReplicas,
        int auditLogMaxEntries,
        Duration consumerIdleInterval,
        Retry retry) {

    /**
     * @param maxAttempts total delivery attempts before a record is dead-lettered, including the
     *     first; a finite value is the whole point, see ADR-009
     */
    public record Retry(
            int maxAttempts, Duration initialInterval, double multiplier, Duration maxInterval) {}
}
