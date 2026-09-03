package com.tradingplatform.pipeline.consume;

import com.tradingplatform.domain.Side;
import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.risk.PreTradeRiskEngine;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Keeps the pre-trade risk engine's positions in step with the execution stream.
 *
 * <h2>Why this is a reconciliation path and not the primary one</h2>
 *
 * The gateway already applies a fill to the risk engine synchronously, because an admission
 * decision has to see the fill that just happened, not the fill that will arrive over a broker in a
 * few milliseconds. Making Kafka the only writer would put broker latency and broker availability
 * in front of pre-trade risk, which is precisely where they must not be.
 *
 * <p>This consumer exists for the property the synchronous path cannot provide: positions that
 * survive a restart. Both paths call {@link PreTradeRiskEngine#recordFill} with the same execution
 * id, and that call applies a fill at most once, so the two converge instead of double counting.
 * In steady state this consumer therefore does almost nothing and reports duplicates. After a
 * restart, replaying the executions topic from the beginning is what rebuilds the positions the
 * gateway lost.
 */
public class RiskStateUpdater {
    public static final String GROUP = "risk-state-updater";

    private final PreTradeRiskEngine riskEngine;
    private final ProcessingMetrics metrics;

    public RiskStateUpdater(PreTradeRiskEngine riskEngine, MeterRegistry meters) {
        this.riskEngine = riskEngine;
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
            // Both sides are applied. Evaluated eagerly rather than short-circuited, because the
            // sell side must be applied even when the buy side was already known.
            boolean buyApplied = riskEngine.recordFill(
                    execution.buyAccountId(),
                    execution.executionId(),
                    execution.symbol(),
                    Side.BUY,
                    execution.quantity());
            boolean sellApplied = riskEngine.recordFill(
                    execution.sellAccountId(),
                    execution.executionId(),
                    execution.symbol(),
                    Side.SELL,
                    execution.quantity());
            return buyApplied || sellApplied;
        });
    }
}
