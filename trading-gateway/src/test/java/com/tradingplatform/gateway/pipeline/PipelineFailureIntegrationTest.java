package com.tradingplatform.gateway.pipeline;

import static com.tradingplatform.gateway.pipeline.ExecutionPipelineIntegrationTest.execution;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.gateway.api.SubmitOrderRequest;
import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.portfolio.PortfolioProcessor;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;

/** The failure modes Kafka actually produces, exercised against a real broker and a real database. */
@SpringBootTest
@ActiveProfiles("pipeline-test")
class PipelineFailureIntegrationTest extends KafkaPipelineTestBase {
    private static final Duration SETTLE_WITHIN = Duration.ofSeconds(30);
    private static final String STRATEGY = SubmitOrderRequest.DEFAULT_STRATEGY;

    @Autowired private PortfolioReadModel portfolio;
    @Autowired private KafkaTemplate<String, TradingEvent> eventTemplate;
    @Autowired private KafkaListenerEndpointRegistry listenerRegistry;

    @Test
    void anUnparseableRecordIsDeadLetteredAndDoesNotBlockThePartition() {
        // Written with a raw producer, because the typed template could not create this record.
        publishRaw(TradingTopics.EXECUTIONS, "TSLA", "{\"eventType\":\"ExecutionCreated\", broken");

        ExecutionCreated healthy = execution(910_001L, "TSLA", "ACC-1", "ACC-2", 3, 24_000);
        eventTemplate.send(TradingTopics.EXECUTIONS, healthy.symbol(), healthy);

        // The good record behind the poison one still lands: the partition kept moving.
        await().atMost(SETTLE_WITHIN)
                .untilAsserted(() -> assertEquals(3, netPosition("ACC-1", "TSLA")));

        List<String> deadLettered = drainDeadLetterTopic(TradingTopics.EXECUTIONS_DLQ);
        assertTrue(
                deadLettered.stream().anyMatch(record -> record.contains("broken")),
                "the unparseable record should be on the dead-letter topic, found: " + deadLettered);
    }

    @Test
    void aStructurallyValidButImpossibleRecordIsDeadLettered() {
        ExecutionCreated negativeQuantity = new ExecutionCreated(
                "exe-910002",
                "test",
                910_002L,
                "NVDA",
                12_000,
                -5, // no retry will make this a real trade
                1,
                "ACC-1",
                STRATEGY,
                2,
                "ACC-2",
                STRATEGY,
                1,
                2,
                0,
                0,
                java.time.Instant.parse("2026-08-18T10:00:00Z"));

        eventTemplate.send(TradingTopics.EXECUTIONS, negativeQuantity.symbol(), negativeQuantity);

        await().atMost(SETTLE_WITHIN).untilAsserted(() -> assertTrue(
                drainDeadLetterTopic(TradingTopics.EXECUTIONS_DLQ).stream()
                        .anyMatch(record -> record.contains("910002")),
                "an impossible execution should be dead-lettered rather than retried forever"));
    }

    @Test
    void aRestartedConsumerResumesFromItsCommittedOffsetWithoutDoubleCounting() {
        MessageListenerContainer container =
                listenerRegistry.getListenerContainer(PortfolioProcessor.GROUP);

        ExecutionCreated before = execution(910_003L, "MSFT", "ACC-1", "ACC-2", 4, 42_000);
        eventTemplate.send(TradingTopics.EXECUTIONS, before.symbol(), before);
        await().atMost(SETTLE_WITHIN)
                .untilAsserted(() -> assertEquals(4, netPosition("ACC-1", "MSFT")));

        container.stop();
        ExecutionCreated during = execution(910_004L, "MSFT", "ACC-1", "ACC-2", 6, 42_000);
        eventTemplate.send(TradingTopics.EXECUTIONS, during.symbol(), during);
        container.start();

        // Everything published while it was down is delivered, and nothing already applied is
        // applied a second time. The execution's Postgres primary key is what guarantees that.
        await().during(Duration.ofSeconds(3)).atMost(SETTLE_WITHIN)
                .untilAsserted(() -> assertEquals(10, netPosition("ACC-1", "MSFT")));
    }

    @Test
    void aRebalanceNeitherLosesNorDuplicatesExecutions() {
        MessageListenerContainer container =
                listenerRegistry.getListenerContainer(PortfolioProcessor.GROUP);

        ExecutionCreated first = execution(910_005L, "AAPL", "ACC-3", "ACC-4", 2, 19_000);
        eventTemplate.send(TradingTopics.EXECUTIONS, first.symbol(), first);
        await().atMost(SETTLE_WITHIN)
                .untilAsserted(() -> assertEquals(2, netPosition("ACC-3", "AAPL")));

        // Changing concurrency forces the group to redistribute its partitions.
        container.stop();
        ((org.springframework.kafka.listener.ConcurrentMessageListenerContainer<?, ?>) container)
                .setConcurrency(3);
        container.start();

        for (long executionId = 910_010L; executionId < 910_016L; executionId++) {
            ExecutionCreated execution =
                    execution(executionId, "AAPL", "ACC-3", "ACC-4", 1, 19_000);
            eventTemplate.send(TradingTopics.EXECUTIONS, execution.symbol(), execution);
        }

        await().during(Duration.ofSeconds(3)).atMost(SETTLE_WITHIN).untilAsserted(() -> {
            assertEquals(8, netPosition("ACC-3", "AAPL"));
            assertEquals(-8, netPosition("ACC-4", "AAPL"));
        });
    }

    @Test
    void aBacklogIsWorkedThroughRatherThanDropped() {
        int burst = 300;
        for (long index = 0; index < burst; index++) {
            ExecutionCreated execution =
                    execution(920_000L + index, "NOPRICE", "ACC-1", "ACC-2", 1, 100);
            eventTemplate.send(TradingTopics.EXECUTIONS, execution.symbol(), execution);
        }

        // Consumers are slower than a producer that can publish a burst in one go; the contract is
        // that they catch up, not that they keep pace.
        await().atMost(SETTLE_WITHIN)
                .untilAsserted(() -> assertEquals(burst, netPosition("ACC-1", "NOPRICE")));
    }

    private long netPosition(String accountId, String symbol) {
        return portfolio
                .position(new PositionKey(accountId, STRATEGY, symbol))
                .map(PortfolioReadModel.PositionView::netQuantity)
                .orElse(0L);
    }

    private void publishRaw(String topic, String key, String payload) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, payload));
            producer.flush();
        }
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
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    payloads.add(record.value() == null ? "" : new String(record.value()));
                }
                if (!payloads.isEmpty()) {
                    break;
                }
            }
            return payloads;
        }
    }
}
