package com.tradingplatform.pipeline.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.pipeline.consume.DownstreamUnavailableException;
import com.tradingplatform.pipeline.events.OrderAccepted;
import java.lang.reflect.Method;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The two pieces of {@link KafkaPipelineConfiguration} that are pure logic rather than Spring
 * wiring: the event object mapper's date format (consumers outside this codebase must be able to
 * read the topic) and the retryable-vs-not-retryable classification that decides whether a failure
 * gets dead-lettered immediately or retried indefinitely (see ADR-009 and the class's own Javadoc).
 * The bean-wiring itself (consumer factories, container factories, DLQ recoverer routing) is
 * exercised end to end by the Testcontainers-based tests in {@code trading-gateway}, which need a
 * real broker to mean anything.
 */
class KafkaPipelineConfigurationTest {

    @Test
    void tradingEventObjectMapperWritesInstantsAsIso8601NotEpochMillis() throws Exception {
        ObjectMapper mapper = new KafkaPipelineConfiguration().tradingEventObjectMapper();
        OrderAccepted event = new OrderAccepted(
                "acc-1", "corr-1", 1, "client-1", "acc-buy", "strat-buy", "AAPL", Side.BUY,
                OrderType.LIMIT, OrderStatus.NEW, 10, 10, 10_000, Instant.parse("2026-08-22T09:30:00Z"));

        String json = mapper.writeValueAsString(event);

        assertTrue(json.contains("\"2026-08-22T09:30:00Z\""), "expected an ISO-8601 timestamp, got: " + json);
        OrderAccepted roundTripped = mapper.readValue(json, OrderAccepted.class);
        assertEquals(event, roundTripped);
    }

    @Test
    void directDownstreamUnavailableExceptionIsClassifiedAsDownstreamUnavailable() throws Exception {
        assertTrue(isDownstreamUnavailable(new DownstreamUnavailableException("db down", null)));
    }

    @Test
    void nestedDownstreamUnavailableExceptionIsClassifiedByWalkingTheCauseChain() throws Exception {
        RuntimeException wrapped = new RuntimeException(
                "listener failed", new DownstreamUnavailableException("db down", null));

        assertTrue(isDownstreamUnavailable(wrapped));
    }

    @Test
    void anUnrelatedExceptionIsNotClassifiedAsDownstreamUnavailable() throws Exception {
        assertFalse(isDownstreamUnavailable(new IllegalStateException("not a downstream outage")));
    }

    /**
     * {@code isDownstreamUnavailable} decides retryable-forever vs. dead-lettered-immediately for
     * every consumer failure in the pipeline, but it is private because nothing outside this class
     * should call it directly. Reflection is the honest way to unit-test that specific branch
     * without promoting it to public API just for a test.
     */
    private static boolean isDownstreamUnavailable(Throwable exception) throws Exception {
        Method method =
                KafkaPipelineConfiguration.class.getDeclaredMethod("isDownstreamUnavailable", Throwable.class);
        method.setAccessible(true);
        return (boolean) method.invoke(null, exception);
    }
}
