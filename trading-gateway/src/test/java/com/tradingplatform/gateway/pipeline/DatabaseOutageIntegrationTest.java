package com.tradingplatform.gateway.pipeline;

import static com.tradingplatform.gateway.pipeline.ExecutionPipelineIntegrationTest.execution;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.portfolio.PortfolioReadModel;
import com.tradingplatform.portfolio.PositionKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.DockerClientFactory;

/**
 * What happens to the portfolio consumer when the database is the thing that is down.
 *
 * <p>This is the case {@link com.tradingplatform.pipeline.consume.DownstreamUnavailableException}
 * and the infinite-backoff wiring in {@code KafkaPipelineConfiguration} exist for. A trade is a
 * fact that cannot be reconstructed once it is discarded, so a record that fails only because
 * Postgres is briefly unreachable must be retried until the database returns — never dead-lettered,
 * which is what an ordinary failure would trigger and would turn a recoverable outage into a
 * permanently missing position.
 */
@SpringBootTest
@ActiveProfiles("pipeline-test")
class DatabaseOutageIntegrationTest extends KafkaPipelineTestBase {
    private static final Duration SETTLE_WITHIN = Duration.ofSeconds(30);

    @Autowired private PortfolioReadModel portfolio;
    @Autowired private KafkaTemplate<String, TradingEvent> eventTemplate;

    @Test
    void aRecordThatFailsOnlyBecauseTheDatabaseIsDownIsRetriedNotDeadLettered() {
        long executionId = 930_001L;
        ExecutionCreated execution = execution(executionId, "AAPL", "ACC-D1", "ACC-D2", 9, 19_000);

        pausePostgres();
        try {
            eventTemplate.send(TradingTopics.EXECUTIONS, execution.symbol(), execution);

            // Long enough that the consumer has certainly attempted the write against the paused
            // database and retried at least once. The read model is not queried here: it needs
            // Postgres too, and asking it anything while the database is paused would itself throw
            // rather than usefully answer "not yet applied".
            Thread.sleep(3_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            unpausePostgres();
        }

        // Once the database returns, the retried record is applied. No amount of retrying could
        // have made a *rejected* record succeed, which is exactly what distinguishes this path
        // from the dead-letter one: this record was always going to work once Postgres was back.
        await().atMost(SETTLE_WITHIN).untilAsserted(() -> assertEquals(
                9,
                portfolio
                        .position(new PositionKey("ACC-D1", "DEFAULT", "AAPL"))
                        .orElseThrow(() -> new AssertionError("position should now exist"))
                        .netQuantity()));

        List<String> deadLettered = drainDeadLetterTopic(TradingTopics.EXECUTIONS_DLQ);
        assertFalse(
                deadLettered.stream().anyMatch(record -> record.contains("930001")),
                "a record that only failed due to a database outage must never reach the DLQ, found: "
                        + deadLettered);
    }

    private static void pausePostgres() {
        DockerClientFactory.instance().client().pauseContainerCmd(POSTGRES.getContainerId()).exec();
    }

    private static void unpausePostgres() {
        DockerClientFactory.instance()
                .client()
                .unpauseContainerCmd(POSTGRES.getContainerId())
                .exec();
    }

    private List<String> drainDeadLetterTopic(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-reader-" + System.nanoTime());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(topic));
            List<String> payloads = new java.util.ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    payloads.add(record.value() == null ? "" : new String(record.value()));
                }
            }
            return payloads;
        }
    }
}
