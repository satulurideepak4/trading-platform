package com.tradingplatform.gateway.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Proves the exposition path end to end, not just that meters exist somewhere in-process: a real
 * scrape of {@code /actuator/prometheus} has to return Prometheus text format containing the
 * Stage 8 meters this module registers. {@code GatewayMetricsTest} and
 * {@code RouterMetricsBinderTest} cover the meters' own correctness; this covers the one thing
 * neither of them touches — that Micrometer's Prometheus registry is actually wired in and
 * reachable over HTTP with no authentication required, the same as every other actuator endpoint.
 *
 * <p>{@code @AutoConfigureObservability} is required and easy to miss: Spring Boot's test support
 * disables metrics (and tracing) export by default for every {@code @SpringBootTest} (so a unit
 * test never accidentally starts a real Prometheus/StatsD/etc. export pipeline), which without this
 * annotation quietly leaves the injected {@code MeterRegistry} a plain {@code SimpleMeterRegistry}
 * and {@code /actuator/prometheus} unmapped — a 500 via the static-resource fallback handler, not
 * an obviously metrics-shaped error. Found by running this test, not by inspection: the older
 * {@code @AutoConfigureMetrics} annotation that used to do this in Spring Boot 2.x no longer exists
 * in this Spring Boot version.
 */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureObservability
@ActiveProfiles("test")
class PrometheusExpositionTest {
    @Autowired private MockMvc mockMvc;

    @Test
    void prometheusEndpointExposesJvmAndMatchingMeters() throws Exception {
        // At least one command first, so trading.matching.* has something to report rather than
        // asserting on a meter that may not exist until it has been observed once.
        mockMvc.perform(post("/orders")
                .header("X-Api-Key", "key-one")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"clientOrderId":"prom-scrape-1","symbol":"AAPL","side":"BUY",\
                        "type":"LIMIT","quantity":1,"price":19000}
                        """));

        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("trading_matching_latency")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("trading_matching_queue_depth")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("jvm_memory_used_bytes")));
    }
}
