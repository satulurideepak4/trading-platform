package com.tradingplatform.pipeline.publish;

import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.events.TradingEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Hands events to Kafka without ever making a matching worker wait.
 *
 * <p>{@code KafkaTemplate.send} is asynchronous in the normal case, but it blocks for up to
 * {@code max.block.ms} when the producer's accumulator is full or metadata is unavailable — exactly
 * the conditions a broker problem creates. A matching worker must not be the thread that discovers
 * that. So events cross a bounded in-process queue and a single dispatcher thread owns every call
 * into the producer.
 *
 * <p>The dispatcher is deliberately single-threaded. Events reach the queue in the order the
 * matching workers produced them, and one dispatcher preserves that order on the way into the
 * producer. Combined with an idempotent producer, which keeps ordering per partition even with
 * requests in flight, the order a symbol's events are appended to its partition is the order the
 * engine produced them.
 *
 * <p><b>Overflow loses events.</b> A full queue means Kafka has been unreachable for long enough to
 * exhaust both the producer's own buffer and this one. Blocking instead would stall matching for
 * every symbol on that worker, so the publisher drops and counts. There is no local durable record
 * to recover from yet; that is what the Stage 7 journal is for, and it is the honest limitation of
 * this design today.
 */
public final class BufferedKafkaEventPublisher implements TradingEventPublisher, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BufferedKafkaEventPublisher.class);
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, TradingEvent> kafkaTemplate;
    private final BlockingQueue<TradingEvent> pending;
    private final Thread dispatcher;
    private final AtomicLong droppedEvents = new AtomicLong();

    private final Counter published;
    private final Counter dropped;
    private final Counter failed;
    private final Timer acknowledgementTimer;

    private volatile boolean stopping;

    public BufferedKafkaEventPublisher(
            KafkaTemplate<String, TradingEvent> kafkaTemplate,
            int queueCapacity,
            MeterRegistry meters) {
        this.kafkaTemplate = kafkaTemplate;
        this.pending = new ArrayBlockingQueue<>(queueCapacity);
        this.published = Counter.builder("trading.events.published")
                .description("Events handed to the Kafka producer")
                .register(meters);
        this.dropped = Counter.builder("trading.events.dropped")
                .description("Events discarded because the publish buffer was full")
                .register(meters);
        this.failed = Counter.builder("trading.events.publish.failed")
                .description("Events the broker did not acknowledge")
                .register(meters);
        this.acknowledgementTimer = Timer.builder("trading.events.publish.latency")
                .description("Time from handing a record to the producer until the broker acked it")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .register(meters);
        Gauge.builder("trading.events.queue.depth", pending, BlockingQueue::size)
                .description("Events waiting to be handed to the Kafka producer")
                .register(meters);

        this.dispatcher = Thread.ofPlatform()
                .name("trading-event-dispatcher")
                .daemon(false)
                .unstarted(this::dispatchLoop);
    }

    public void start() {
        dispatcher.start();
    }

    @Override
    public void publish(TradingEvent event) {
        if (pending.offer(event)) {
            return;
        }
        dropped.increment();
        long total = droppedEvents.incrementAndGet();
        // One line per drop would itself become the bottleneck during an outage.
        if (Long.bitCount(total) == 1) {
            LOG.error(
                    "event=event_publish_buffer_full droppedTotal={} eventId={} "
                            + "detail=\"downstream state is now behind the matching engine\"",
                    total,
                    event.eventId());
        }
    }

    public long droppedEventCount() {
        return droppedEvents.get();
    }

    public int queueDepth() {
        return pending.size();
    }

    @Override
    public void close() {
        stopping = true;
        dispatcher.interrupt();
        try {
            dispatcher.join(DRAIN_TIMEOUT.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        if (dispatcher.isAlive()) {
            LOG.warn("event=event_dispatcher_drain_timeout pending={}", pending.size());
        }
    }

    private void dispatchLoop() {
        while (!stopping || !pending.isEmpty()) {
            try {
                TradingEvent event = pending.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    send(event);
                }
            } catch (InterruptedException interrupted) {
                // Only expected during shutdown; the loop condition decides whether to keep going
                // so that whatever is already queued still gets published.
                if (!stopping) {
                    LOG.warn("event=event_dispatcher_interrupted_while_active");
                    Thread.currentThread().interrupt();
                    return;
                }
            } catch (RuntimeException failure) {
                // A send that fails synchronously must not kill the only dispatcher thread.
                failed.increment();
                LOG.error("event=event_publish_failed", failure);
            }
        }
    }

    private void send(TradingEvent event) {
        Timer.Sample sample = Timer.start();
        kafkaTemplate
                .send(TradingTopics.topicFor(event), event.symbol(), event)
                .whenComplete((result, failure) -> {
                    sample.stop(acknowledgementTimer);
                    if (failure == null) {
                        published.increment();
                    } else {
                        failed.increment();
                        LOG.error(
                                "event=event_not_acknowledged eventId={} symbol={}",
                                event.eventId(),
                                event.symbol(),
                                failure);
                    }
                });
    }
}
