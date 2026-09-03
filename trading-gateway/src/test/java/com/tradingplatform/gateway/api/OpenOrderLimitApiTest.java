package com.tradingplatform.gateway.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** Tight account-level limits, in their own context so the numbers stay small and readable. */
@SpringBootTest(properties = {
        "trading.risk.max-open-orders=3",
        "trading.risk.max-position-quantity=100"})
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OpenOrderLimitApiTest {

    @Autowired private MockMvc mockMvc;
    @Autowired private ObjectMapper objectMapper;

    @Test
    void restingOrdersCountTowardsTheOpenOrderLimitUntilTheyAreCancelled() throws Exception {
        MvcResult first = mockMvc.perform(submit("key-one", "open-1", 1, 10_000))
                .andExpect(status().isCreated())
                .andReturn();
        mockMvc.perform(submit("key-one", "open-2", 1, 10_001)).andExpect(status().isCreated());
        mockMvc.perform(submit("key-one", "open-3", 1, 10_002)).andExpect(status().isCreated());

        mockMvc.perform(submit("key-one", "open-4", 1, 10_003))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("MAX_OPEN_ORDERS_EXCEEDED"));

        mockMvc.perform(delete("/orders/" + orderId(first)).header("X-Api-Key", "key-one"))
                .andExpect(status().isOk());

        mockMvc.perform(submit("key-one", "open-5", 1, 10_004)).andExpect(status().isCreated());
    }

    @Test
    void aFilledOrderStopsCountingAgainstTheOpenOrderLimitForBothSides() throws Exception {
        mockMvc.perform(submit("key-two", "maker-1", 5, 30_000)).andExpect(status().isCreated());
        mockMvc.perform(submit("key-two", "maker-2", 5, 30_001)).andExpect(status().isCreated());
        mockMvc.perform(submit("key-two", "maker-3", 5, 30_002)).andExpect(status().isCreated());
        mockMvc.perform(submit("key-two", "maker-4", 5, 30_003))
                .andExpect(status().isUnprocessableEntity());

        // A different account lifts all three resting bids, which frees the maker's open slots.
        mockMvc.perform(sell("key-three", "taker-1", 15, 30_000))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("FILLED"));

        mockMvc.perform(submit("key-two", "maker-5", 5, 30_000)).andExpect(status().isCreated());
    }

    private long orderId(MvcResult result) throws Exception {
        return objectMapper
                .readTree(result.getResponse().getContentAsString())
                .get("orderId")
                .asLong();
    }

    private static MockHttpServletRequestBuilder submit(
            String apiKey, String clientOrderId, long quantity, long price) {
        return order(apiKey, clientOrderId, "BUY", quantity, price);
    }

    private static MockHttpServletRequestBuilder sell(
            String apiKey, String clientOrderId, long quantity, long price) {
        return order(apiKey, clientOrderId, "SELL", quantity, price);
    }

    private static MockHttpServletRequestBuilder order(
            String apiKey, String clientOrderId, String side, long quantity, long price) {
        String body = """
                {"clientOrderId":"%s","symbol":"MSFT","side":"%s","type":"LIMIT",\
                "quantity":%d,"price":%d}
                """.formatted(clientOrderId, side, quantity, price);
        return post("/orders")
                .header("X-Api-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }
}
