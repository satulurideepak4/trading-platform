package com.tradingplatform.gateway.config;

import com.tradingplatform.gateway.metrics.RouterMetricsBinder;
import com.tradingplatform.matching.routing.OrderRouter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers {@link RouterMetricsBinder}'s per-worker meters once, at startup.
 *
 * <p>An {@link InitializingBean} lambda rather than side-effecting from this class's own
 * constructor: the binding has no bean of its own to be, but it still needs Spring to run it after
 * {@code OrderRouter} exists, which is exactly what a bean's lifecycle callback is for.
 */
@Configuration
public class RouterMetricsConfiguration {

    @Bean
    InitializingBean routerMetricsBinding(OrderRouter router, MeterRegistry meters) {
        return () -> RouterMetricsBinder.bind(router, meters);
    }
}
