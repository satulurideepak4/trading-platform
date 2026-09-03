package com.tradingplatform.pipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.consume.AuditProcessor;
import com.tradingplatform.pipeline.consume.DownstreamUnavailableException;
import com.tradingplatform.pipeline.consume.ExecutionProcessor;
import com.tradingplatform.pipeline.consume.InvalidEventException;
import com.tradingplatform.pipeline.consume.ReplayReadiness;
import com.tradingplatform.pipeline.consume.RiskStateUpdater;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.pipeline.publish.BufferedKafkaEventPublisher;
import com.tradingplatform.pipeline.store.AuditLog;
import com.tradingplatform.pipeline.store.ExecutionStore;
import com.tradingplatform.risk.PreTradeRiskEngine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/** Everything that only exists when the pipeline is switched on. */
@Configuration
@EnableKafka
@ConditionalOnProperty(name = "trading.pipeline.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaPipelineConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPipelineConfiguration.class);

    /**
     * A mapper owned by the pipeline rather than the shared web one, so the wire format of the
     * event log cannot change as a side effect of someone adjusting HTTP serialization.
     */
    @Bean
    public ObjectMapper tradingEventObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public KafkaTemplate<String, TradingEvent> tradingEventKafkaTemplate(
            KafkaProperties kafkaProperties,
            ObjectMapper tradingEventObjectMapper,
            MeterRegistry meters) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        // A trade that the brokers have not durably taken is a trade the downstream will never see,
        // so the weaker ack modes are not an option here.
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        // Keeps per-partition ordering intact even with several requests in flight, and stops a
        // producer retry from writing the same record twice.
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        // The dispatcher thread must not sit in send() for a minute when a broker disappears.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);

        JsonSerializer<TradingEvent> valueSerializer =
                new JsonSerializer<>(tradingEventObjectMapper);
        // The eventType property inside the payload is the discriminator. Spring's own type headers
        // are switched off so the records stay readable by a consumer that is not this application.
        valueSerializer.setAddTypeInfo(false);

        DefaultKafkaProducerFactory<String, TradingEvent> factory =
                new DefaultKafkaProducerFactory<>(config, new StringSerializer(), valueSerializer);
        // Publishes the Kafka client's own producer metrics. Spring Boot only attaches this to the
        // factory it auto-configures, and this one is built by hand.
        factory.addListener(new MicrometerProducerListener<>(meters));
        return new KafkaTemplate<>(factory);
    }

    /**
     * The template used only for dead-lettering.
     *
     * <p>A dead-lettered record can arrive in two shapes and the serializer has to cope with both.
     * A record that failed to deserialize has no object left to re-serialize, so its value is the
     * original bytes; a record that deserialized but was refused still holds a real event. The key
     * is independent of that — a value can be undeserializable while its key parsed cleanly — so
     * both sides delegate by runtime type rather than assuming one encoding.
     */
    @Bean
    public KafkaTemplate<Object, Object> deadLetterKafkaTemplate(
            KafkaProperties kafkaProperties, ObjectMapper tradingEventObjectMapper) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        JsonSerializer<Object> eventSerializer = new JsonSerializer<>(tradingEventObjectMapper);
        eventSerializer.setAddTypeInfo(false);

        Serializer<Object> keySerializer = new DelegatingByTypeSerializer(
                Map.of(byte[].class, new ByteArraySerializer(), String.class, new StringSerializer()),
                true);
        Serializer<Object> valueSerializer = new DelegatingByTypeSerializer(
                Map.of(byte[].class, new ByteArraySerializer(), TradingEvent.class, eventSerializer),
                true);

        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(config, keySerializer, valueSerializer));
    }

    @Bean
    public BufferedKafkaEventPublisher tradingEventPublisher(
            KafkaTemplate<String, TradingEvent> tradingEventKafkaTemplate,
            PipelineProperties properties,
            MeterRegistry meters) {
        BufferedKafkaEventPublisher publisher = new BufferedKafkaEventPublisher(
                tradingEventKafkaTemplate, properties.publishQueueCapacity(), meters);
        publisher.start();
        return publisher;
    }

    @Bean
    public ConsumerFactory<String, TradingEvent> tradingEventConsumerFactory(
            KafkaProperties kafkaProperties,
            ObjectMapper tradingEventObjectMapper,
            MeterRegistry meters) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        // Projections are rebuilt by reading the log from the start; a new consumer group that
        // began at the tail would silently hold an incomplete position.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<TradingEvent> valueDeserializer =
                new JsonDeserializer<>(TradingEvent.class, tradingEventObjectMapper, false);
        valueDeserializer.setUseTypeHeaders(false);

        // Wrapping both deserializers means a malformed record surfaces as a failed record the
        // error handler can dead-letter, instead of an exception that stops the container.
        DefaultKafkaConsumerFactory<String, TradingEvent> factory = new DefaultKafkaConsumerFactory<>(
                config,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(valueDeserializer));
        // Brings the client's own metrics into the registry, consumer lag included. Without it
        // there is no way to see a consumer falling behind.
        factory.addListener(new MicrometerConsumerListener<>(meters));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TradingEvent>
            tradingEventListenerContainerFactory(
                    ConsumerFactory<String, TradingEvent> tradingEventConsumerFactory,
                    DefaultErrorHandler tradingEventErrorHandler,
                    PipelineProperties properties) {
        ConcurrentKafkaListenerContainerFactory<String, TradingEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(tradingEventConsumerFactory);
        factory.setCommonErrorHandler(tradingEventErrorHandler);
        ContainerProperties containerProperties = factory.getContainerProperties();
        // Commit after each record. Processing is idempotent, so at-least-once is safe either way,
        // and per-record commits keep the redelivery window after a crash as small as possible.
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
        // Idle events are how the pipeline decides a startup replay has caught up.
        containerProperties.setIdleEventInterval(properties.consumerIdleInterval().toMillis());
        return factory;
    }

    @Bean
    public DefaultErrorHandler tradingEventErrorHandler(
            KafkaTemplate<Object, Object> deadLetterKafkaTemplate,
            PipelineProperties properties,
            MeterRegistry meters) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterKafkaTemplate,
                // Same partition index as the original, so a key's failures stay together and the
                // DLQ can be read alongside the topic it came from.
                (record, exception) -> new TopicPartition(
                        TradingTopics.deadLetterFor(record.topic()), record.partition()));

        PipelineProperties.Retry retry = properties.retry();
        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(Math.max(0, retry.maxAttempts() - 1));
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        Counter deadLettered = Counter.builder("trading.consumer.dead.lettered")
                .description("Records given up on and forwarded to a dead-letter topic")
                .register(meters);
        // Wrapping the recoverer rather than the handler is what makes this count give-ups: the
        // recoverer runs once, after the retry budget is spent, not on every failed attempt.
        ConsumerRecordRecoverer countingRecoverer = (record, exception) -> {
            deadLettered.increment();
            LOG.error(
                    "event=record_dead_lettered topic={} partition={} offset={} cause={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    exception.getClass().getSimpleName(),
                    exception);
            recoverer.accept(record, exception);
        };

        DefaultErrorHandler handler = new DefaultErrorHandler(countingRecoverer, backOff);
        // Neither of these can succeed on a retry, so they skip the backoff and are dead-lettered
        // on the first attempt rather than holding up the partition for the full retry budget.
        handler.addNotRetryableExceptions(
                InvalidEventException.class, DeserializationException.class);

        // The inverse case: a record that failed only because a downstream dependency (the
        // portfolio database, say) was unreachable must never be dead-lettered, because the record
        // is fine and dead-lettering it would turn a temporary outage into a permanent missing
        // trade. It is retried on a fixed interval with no attempt limit, which blocks the
        // partition until the dependency returns — a stalled consumer is visible in lag and
        // recoverable by fixing the dependency; a dropped trade is not recoverable at all.
        BackOff unavailableBackOff =
                new FixedBackOff(retry.maxInterval().toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS);
        handler.setBackOffFunction((record, exception) ->
                isDownstreamUnavailable(exception) ? unavailableBackOff : backOff);
        return handler;
    }

    private static boolean isDownstreamUnavailable(Throwable exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof DownstreamUnavailableException) {
                return true;
            }
        }
        return false;
    }

    @Bean
    public ReplayReadiness replayReadiness() {
        return new ReplayReadiness();
    }

    @Bean
    public ExecutionProcessor executionProcessor(ExecutionStore store, MeterRegistry meters) {
        return new ExecutionProcessor(store, meters);
    }

    @Bean
    public AuditProcessor auditProcessor(AuditLog auditLog, MeterRegistry meters) {
        return new AuditProcessor(auditLog, meters);
    }

    @Bean
    public RiskStateUpdater riskStateUpdater(PreTradeRiskEngine riskEngine, MeterRegistry meters) {
        return new RiskStateUpdater(riskEngine, meters);
    }

    @Bean
    public NewTopic ordersTopic(PipelineProperties properties) {
        return topic(TradingTopics.ORDERS, properties);
    }

    @Bean
    public NewTopic executionsTopic(PipelineProperties properties) {
        return topic(TradingTopics.EXECUTIONS, properties);
    }

    /** Same partition count as its source topic, because the recoverer preserves the index. */
    @Bean
    public NewTopic ordersDeadLetterTopic(PipelineProperties properties) {
        return topic(TradingTopics.ORDERS_DLQ, properties);
    }

    @Bean
    public NewTopic executionsDeadLetterTopic(PipelineProperties properties) {
        return topic(TradingTopics.EXECUTIONS_DLQ, properties);
    }

    private static NewTopic topic(String name, PipelineProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }
}
