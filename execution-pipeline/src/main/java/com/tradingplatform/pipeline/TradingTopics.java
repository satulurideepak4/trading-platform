package com.tradingplatform.pipeline;

import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;

/**
 * Topic names and the routing rule between them. See ADR-006 for why there are two streams rather
 * than one shared topic or one topic per event type.
 *
 * <p>Names carry an explicit schema version. Adding a field is backwards compatible and stays on
 * {@code v1}; anything a consumer cannot ignore requires a {@code v2} topic that old and new
 * consumers can straddle during a migration.
 */
public final class TradingTopics {
    public static final String ORDERS = "trading.orders.v1";
    public static final String EXECUTIONS = "trading.executions.v1";
    public static final String ORDERS_DLQ = ORDERS + ".dlq";
    public static final String EXECUTIONS_DLQ = EXECUTIONS + ".dlq";

    private TradingTopics() {}

    public static String topicFor(TradingEvent event) {
        return event instanceof ExecutionCreated ? EXECUTIONS : ORDERS;
    }

    public static String deadLetterFor(String topic) {
        return topic.endsWith(".dlq") ? topic : topic + ".dlq";
    }
}
