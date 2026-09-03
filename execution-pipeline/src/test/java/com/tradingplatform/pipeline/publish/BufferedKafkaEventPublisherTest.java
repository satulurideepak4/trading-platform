package com.tradingplatform.pipeline.publish;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.pipeline.events.OrderAccepted;
import com.tradingplatform.pipeline.events.TradingEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * The behavior described in {@link BufferedKafkaEventPublisher}'s own Javadoc: a single dispatcher
 * thread preserves submission order into the producer, a full queue drops and counts rather than
 * blocking a matching worker, and {@code close()} does not hang forever behind a stuck producer
 * call.
 */
class BufferedKafkaEventPublisherTest {
    private static final Instant OCCURRED_AT = Instant.parse("2026-08-22T09:30:00Z");

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, TradingEvent> kafkaTemplate = mock(KafkaTemplate.class);

    @Test
    void publishedEventsAreDispatchedInSubmissionOrder() {
        List<TradingEvent> dispatched = new CopyOnWriteArrayList<>();
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenAnswer(invocation -> {
            dispatched.add(invocation.getArgument(2));
            return CompletableFuture.completedFuture(null);
        });

        BufferedKafkaEventPublisher publisher =
                new BufferedKafkaEventPublisher(kafkaTemplate, 16, new SimpleMeterRegistry());
        publisher.start();
        try {
            List<TradingEvent> submitted =
                    List.of(orderAccepted(1), orderAccepted(2), orderAccepted(3), orderAccepted(4), orderAccepted(5));
            submitted.forEach(publisher::publish);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .until(() -> dispatched.size() == submitted.size());
            assertEquals(submitted, dispatched);
        } finally {
            publisher.close();
        }
    }

    @Test
    void queueOverflowDropsAndCountsRatherThanBlockingOrThrowing() {
        CountDownLatch blockFirstSend = new CountDownLatch(1);
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenAnswer(invocation -> {
            blockFirstSend.await();
            return CompletableFuture.completedFuture(null);
        });

        // Capacity 1: once the dispatcher pulls the first event out to send it (and blocks there),
        // the queue can hold exactly one more before a publish() must be dropped.
        BufferedKafkaEventPublisher publisher =
                new BufferedKafkaEventPublisher(kafkaTemplate, 1, new SimpleMeterRegistry());
        publisher.start();
        try {
            publisher.publish(orderAccepted(1));
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .until(() -> publisher.queueDepth() == 0);

            publisher.publish(orderAccepted(2)); // fills the now-empty capacity-1 queue
            publisher.publish(orderAccepted(3)); // must overflow: dropped and counted

            assertEquals(1, publisher.droppedEventCount());
            assertEquals(1, publisher.queueDepth());
        } finally {
            blockFirstSend.countDown();
            publisher.close();
        }
    }

    @Test
    void closeDrainsPendingEventsBeforeReturning() {
        List<TradingEvent> dispatched = new CopyOnWriteArrayList<>();
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenAnswer(invocation -> {
            dispatched.add(invocation.getArgument(2));
            return CompletableFuture.completedFuture(null);
        });

        BufferedKafkaEventPublisher publisher =
                new BufferedKafkaEventPublisher(kafkaTemplate, 16, new SimpleMeterRegistry());
        publisher.start();
        List<TradingEvent> submitted =
                List.of(orderAccepted(1), orderAccepted(2), orderAccepted(3));
        submitted.forEach(publisher::publish);

        publisher.close();

        assertEquals(submitted.size(), dispatched.size());
    }

    @Test
    void closeInterruptsABlockedDispatcherRatherThanHangingForever() {
        CountDownLatch neverReleasedByTheTest = new CountDownLatch(1);
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenAnswer(invocation -> {
            try {
                neverReleasedByTheTest.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("dispatcher interrupted mid-send", interrupted);
            }
            return CompletableFuture.completedFuture(null);
        });

        BufferedKafkaEventPublisher publisher =
                new BufferedKafkaEventPublisher(kafkaTemplate, 4, new SimpleMeterRegistry());
        publisher.start();
        publisher.publish(orderAccepted(1));
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> publisher.queueDepth() == 0); // dispatcher now blocked inside send()

        // close() must interrupt a dispatcher stuck in a producer call rather than wait the full
        // drain timeout every time a broker call never returns.
        assertTimeout(Duration.ofSeconds(5), publisher::close);
    }

    private static OrderAccepted orderAccepted(long orderId) {
        return new OrderAccepted(
                "acc-" + orderId,
                "corr-" + orderId,
                orderId,
                "client-" + orderId,
                "acc-buy",
                "strat-buy",
                "AAPL",
                Side.BUY,
                OrderType.LIMIT,
                OrderStatus.NEW,
                10,
                10,
                10_000,
                OCCURRED_AT);
    }
}
