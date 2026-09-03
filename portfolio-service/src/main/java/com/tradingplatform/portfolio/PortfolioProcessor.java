package com.tradingplatform.portfolio;

import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.consume.InvalidEventException;
import com.tradingplatform.pipeline.consume.ProcessingMetrics;
import com.tradingplatform.pipeline.consume.TradingEventValidator;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Turns the execution stream into durable positions and P&L.
 *
 * <p>Replaces the in-memory position store from Stage 4 rather than sitting alongside it. Two
 * independent position calculations would be two things to keep in agreement, and the one that is
 * not durable adds nothing once the durable one exists.
 *
 * <p>Runs off the matching path entirely: the matching engine has never heard of this consumer, and
 * no database write happens anywhere near a matching worker.
 */
public class PortfolioProcessor {
    public static final String GROUP = "position-processor";

    private final PortfolioUpdater updater;
    private final ProcessingMetrics metrics;

    public PortfolioProcessor(PortfolioUpdater updater, MeterRegistry meters) {
        this.updater = updater;
        this.metrics = new ProcessingMetrics(meters, GROUP);
    }

    @KafkaListener(
            id = GROUP,
            topics = TradingTopics.EXECUTIONS,
            groupId = GROUP,
            containerFactory = "tradingEventListenerContainerFactory")
    public void onEvent(TradingEvent event) {
        ExecutionCreated execution = requireExecution(event);
        metrics.time(() -> updater.apply(execution));
    }

    private ExecutionCreated requireExecution(TradingEvent event) {
        try {
            TradingEventValidator.requireUsable(event);
        } catch (InvalidEventException invalid) {
            metrics.recordInvalid();
            throw invalid;
        }
        if (event instanceof ExecutionCreated execution) {
            return execution;
        }
        metrics.recordInvalid();
        throw new InvalidEventException(
                "expected an execution on the executions topic but got "
                        + event.getClass().getSimpleName());
    }
}
