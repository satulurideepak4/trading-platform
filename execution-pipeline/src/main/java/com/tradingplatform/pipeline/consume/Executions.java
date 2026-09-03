package com.tradingplatform.pipeline.consume;

import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;

/** Shared entry check for the consumers that only ever expect trades. */
final class Executions {

    private Executions() {}

    static ExecutionCreated require(TradingEvent event, ProcessingMetrics metrics) {
        try {
            TradingEventValidator.requireUsable(event);
        } catch (InvalidEventException invalid) {
            metrics.recordInvalid();
            throw invalid;
        }
        if (event instanceof ExecutionCreated execution) {
            return execution;
        }
        // Something is publishing the wrong event type to the executions topic. Retrying cannot
        // change that, so it is dead-lettered rather than blocking the partition.
        metrics.recordInvalid();
        throw new InvalidEventException(
                "expected an execution on the executions topic but got "
                        + event.getClass().getSimpleName());
    }
}
