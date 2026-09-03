package com.tradingplatform.gateway.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * Runs in its own context so the burst can be small without throttling the behaviour tests. Each
 * test spends a different account's budget, because buckets survive for the life of the context.
 */
@SpringBootTest(properties = {
        "trading.rate-limit.permits-per-second=1",
        "trading.rate-limit.burst=3"})
@AutoConfigureMockMvc
@ActiveProfiles("test")
class RateLimitApiTest {

    @Autowired private MockMvc mockMvc;

    @Test
    void throttlesOnceTheBurstIsSpentAndSaysWhenToRetry() throws Exception {
        for (int index = 0; index < 3; index++) {
            mockMvc.perform(submit("key-one", "burst-" + index)).andExpect(status().isCreated());
        }

        mockMvc.perform(submit("key-one", "burst-over"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"))
                .andExpect(jsonPath("$.source").value("RATE_LIMIT"))
                .andExpect(jsonPath("$.reason").value("RATE_LIMIT_EXCEEDED"));
    }

    @Test
    void oneAccountRunningOutOfBudgetDoesNotAffectAnother() throws Exception {
        for (int index = 0; index < 3; index++) {
            mockMvc.perform(submit("key-three", "three-" + index))
                    .andExpect(status().isCreated());
        }
        mockMvc.perform(submit("key-three", "three-extra"))
                .andExpect(status().isTooManyRequests());

        mockMvc.perform(get("/orderbook/AAPL").header("X-Api-Key", "key-two"))
                .andExpect(status().isOk());
    }

    /** A throttled request must not consume an order id or reach risk. */
    @Test
    void aThrottledSubmissionCanBeRetriedWithTheSameClientOrderId() throws Exception {
        for (int index = 0; index < 3; index++) {
            mockMvc.perform(submit("key-four", "four-" + index)).andExpect(status().isCreated());
        }
        mockMvc.perform(submit("key-four", "four-retryable"))
                .andExpect(status().isTooManyRequests());

        // One permit per second, so waiting just over a second restores exactly one.
        Thread.sleep(1_100);
        mockMvc.perform(submit("key-four", "four-retryable")).andExpect(status().isCreated());
    }

    private static MockHttpServletRequestBuilder submit(String apiKey, String clientOrderId) {
        String body = """
                {"clientOrderId":"%s","symbol":"AAPL","side":"BUY","type":"LIMIT",\
                "quantity":1,"price":10000}
                """.formatted(clientOrderId);
        return post("/orders")
                .header("X-Api-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }
}
