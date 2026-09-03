package com.tradingplatform.pipeline.consume;

import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.pipeline.store.ExecutionStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/** Keeps the canonical record of trades. Its store is keyed by execution id, so it deduplicates. */
public class ExecutionProcessor {
    public static final String GROUP = "execution-processor";
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionProcessor.class);

    private final ExecutionStore executions;
    private final ProcessingMetrics metrics;

    public ExecutionProcessor(ExecutionStore executions, MeterRegistry meters) {
        this.executions = executions;
        this.metrics = new ProcessingMetrics(meters, GROUP);
    }

    @KafkaListener(
            id = GROUP,
            topics = TradingTopics.EXECUTIONS,
            groupId = GROUP,
            containerFactory = "tradingEventListenerContainerFactory")
    public void onEvent(TradingEvent event) {
        ExecutionCreated execution = Executions.require(event, metrics);
        metrics.time(() -> {
            boolean recorded = executions.record(execution);
            if (!recorded) {
                LOG.debug(
                        "event=duplicate_execution consumer={} executionId={}",
                        GROUP,
                        execution.executionId());
            }
            return recorded;
        });
    }
}
