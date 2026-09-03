package com.tradingplatform.gateway.api;

import com.tradingplatform.pipeline.consume.ReplayReadiness;
import com.tradingplatform.pipeline.consume.RiskStateUpdater;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

/**
 * Reports the gateway as not ready until the risk projection has been rebuilt from the execution
 * stream.
 *
 * <p>Without this, a restarted gateway would start accepting orders while its positions were still
 * being replayed, and would happily let an account trade past a limit it had already reached before
 * the restart. Answering "not ready" keeps a load balancer from sending traffic until the state
 * behind the limit checks is whole.
 */
@Component
@ConditionalOnBean(ReplayReadiness.class)
public class PipelineReplayHealthIndicator implements HealthIndicator {
    private final ReplayReadiness readiness;

    public PipelineReplayHealthIndicator(ReplayReadiness readiness) {
        this.readiness = readiness;
    }

    @Override
    public Health health() {
        boolean caughtUp = readiness.isCaughtUp(RiskStateUpdater.GROUP);
        return (caughtUp ? Health.up() : Health.outOfService())
                .withDetail("riskProjectionReplayed", caughtUp)
                .withDetail("caughtUpConsumers", readiness.caughtUpGroups())
                .build();
    }
}
