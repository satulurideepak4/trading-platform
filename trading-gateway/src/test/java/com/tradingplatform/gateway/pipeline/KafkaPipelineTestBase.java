package com.tradingplatform.gateway.pipeline;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One Kafka broker, shared with one Postgres instance ({@link PortfolioTestBase}), for every
 * pipeline test in the JVM.
 *
 * <p>The container is static so it starts once rather than per class: starting a broker costs
 * seconds, and a suite that pays that repeatedly stops being run. Tests therefore have to assume
 * the topics already contain other tests' records, which is why each one uses its own accounts,
 * symbols and consumer groups rather than asserting on totals.
 *
 * <p>Not {@code @Container}-annotated, for the same reason as {@link PortfolioTestBase#POSTGRES}:
 * see its Javadoc.
 */
public abstract class KafkaPipelineTestBase extends PortfolioTestBase {

    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}
