package com.tradingplatform.pipeline.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Instant;

/**
 * A fact that has already happened. Events are immutable and are never used to ask another
 * component to do something, which is why they are all named in the past tense.
 *
 * <p>Two properties matter more than the payloads:
 *
 * <ul>
 *   <li>{@link #eventId()} is derived from the domain fact rather than generated, so re-publishing
 *       the same fact produces the same id and a consumer can recognise it as a duplicate. A random
 *       id would make redelivery indistinguishable from a second, genuinely new fact.
 *   <li>{@link #symbol()} is the Kafka partition key for every event type. See ADR-007.
 * </ul>
 *
 * <p>The discriminator is written as an explicit {@code eventType} property so that a consumer,
 * a DLQ inspector or a human reading the topic can tell what a record is without inferring it from
 * the shape of the payload.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderAccepted.class, name = "OrderAccepted"),
    @JsonSubTypes.Type(value = OrderRejected.class, name = "OrderRejected"),
    @JsonSubTypes.Type(value = OrderCancelled.class, name = "OrderCancelled"),
    @JsonSubTypes.Type(value = OrderReplaced.class, name = "OrderReplaced"),
    @JsonSubTypes.Type(value = ExecutionCreated.class, name = "ExecutionCreated"),
    @JsonSubTypes.Type(value = OrderFilled.class, name = "OrderFilled")
})
public sealed interface TradingEvent
        permits OrderAccepted,
                OrderRejected,
                OrderCancelled,
                OrderReplaced,
                ExecutionCreated,
                OrderFilled {

    String eventId();

    /** The Kafka partition key. Every event belongs to exactly one instrument. */
    String symbol();

    Instant occurredAt();

    /** Ties the event back to the client request that caused it. */
    String correlationId();
}
