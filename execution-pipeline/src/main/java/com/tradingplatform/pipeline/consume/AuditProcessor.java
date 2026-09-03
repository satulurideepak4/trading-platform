package com.tradingplatform.pipeline.consume;

import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.pipeline.store.AuditLog;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Records every event from both streams.
 *
 * <p>This is the one consumer that reads both topics, because an audit trail that can explain a
 * client's day needs the rejections and cancellations as well as the trades.
 */
public class AuditProcessor {
    public static final String GROUP = "audit-processor";

    private final AuditLog auditLog;
    private final ProcessingMetrics metrics;

    public AuditProcessor(AuditLog auditLog, MeterRegistry meters) {
        this.auditLog = auditLog;
        this.metrics = new ProcessingMetrics(meters, GROUP);
    }

    @KafkaListener(
            id = GROUP,
            topics = {TradingTopics.ORDERS, TradingTopics.EXECUTIONS},
            groupId = GROUP,
            containerFactory = "tradingEventListenerContainerFactory")
    public void onEvent(TradingEvent event) {
        try {
            TradingEventValidator.requireUsable(event);
        } catch (InvalidEventException invalid) {
            metrics.recordInvalid();
            throw invalid;
        }
        metrics.time(() -> auditLog.append(event));
    }
}
