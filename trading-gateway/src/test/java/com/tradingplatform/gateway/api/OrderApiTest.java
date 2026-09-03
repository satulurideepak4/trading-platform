package com.tradingplatform.gateway.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.tradingplatform.gateway.CorrelationId;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * Exercises the gateway end to end over HTTP against a real router and matching engine. Each test
 * uses its own clientOrderIds and, where account state matters, its own instrument, because the
 * context is shared across the class.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OrderApiTest {
    private static final String ACCOUNT_ONE_KEY = "key-one";
    private static final String ACCOUNT_TWO_KEY = "key-two";

    // Static because JUnit builds a new test instance per method while the context, and therefore
    // the gateway's clientOrderId registry, is shared across the whole class.
    private static final AtomicInteger CLIENT_ORDER_IDS = new AtomicInteger();

    @Autowired private MockMvc mockMvc;
    @Autowired private ObjectMapper objectMapper;

    @Test
    void acceptsALimitOrderAndReturnsItsAllocatedIdentity() throws Exception {
        String clientOrderId = nextClientOrderId();

        mockMvc.perform(submission(ACCOUNT_ONE_KEY, limitBody(clientOrderId, "AAPL", "BUY", 10, 18_000)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.clientOrderId").value(clientOrderId))
                .andExpect(jsonPath("$.status").value("NEW"))
                .andExpect(jsonPath("$.remainingQuantity").value(10))
                .andExpect(jsonPath("$.duplicate").value(false))
                .andExpect(jsonPath("$.orderId").isNumber());
    }

    @Test
    void echoesTheSuppliedCorrelationId() throws Exception {
        mockMvc.perform(submission(
                                ACCOUNT_ONE_KEY,
                                limitBody(nextClientOrderId(), "AAPL", "BUY", 1, 18_000))
                        .header(CorrelationId.HEADER, "trace-abc-123"))
                .andExpect(status().isCreated())
                .andExpect(header().string(CorrelationId.HEADER, "trace-abc-123"));
    }

    @Test
    void repeatingASubmissionReturnsTheSameOrderRatherThanCreatingASecond() throws Exception {
        String clientOrderId = nextClientOrderId();
        String body = limitBody(clientOrderId, "AAPL", "BUY", 5, 17_000);

        MvcResult first = mockMvc.perform(submission(ACCOUNT_ONE_KEY, body))
                .andExpect(status().isCreated())
                .andReturn();
        MvcResult retry = mockMvc.perform(submission(ACCOUNT_ONE_KEY, body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true))
                .andReturn();

        assertEquals(orderId(first), orderId(retry));
    }

    @Test
    void reusingAClientOrderIdWithDifferentContentIsAConflict() throws Exception {
        String clientOrderId = nextClientOrderId();
        mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(clientOrderId, "AAPL", "BUY", 5, 17_000)))
                .andExpect(status().isCreated());

        mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(clientOrderId, "AAPL", "BUY", 6, 17_000)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("CLIENT_ORDER_ID_REUSED"))
                .andExpect(jsonPath("$.source").value("IDEMPOTENCY"));
    }

    @Test
    void theSameClientOrderIdFromTwoAccountsCreatesTwoOrders() throws Exception {
        String clientOrderId = nextClientOrderId();
        String body = limitBody(clientOrderId, "AAPL", "BUY", 1, 16_000);

        MvcResult first = mockMvc.perform(submission(ACCOUNT_ONE_KEY, body))
                .andExpect(status().isCreated())
                .andReturn();
        MvcResult second = mockMvc.perform(submission(ACCOUNT_TWO_KEY, body))
                .andExpect(status().isCreated())
                .andReturn();

        assertNotEquals(orderId(first), orderId(second));
    }

    @Test
    void concurrentIdenticalSubmissionsCreateExactlyOneOrder() throws Exception {
        String clientOrderId = nextClientOrderId();
        String body = limitBody(clientOrderId, "AAPL", "BUY", 3, 15_000);
        int callers = 16;
        AtomicInteger created = new AtomicInteger();
        AtomicInteger replayed = new AtomicInteger();

        try (ExecutorService threads = Executors.newFixedThreadPool(callers)) {
            CountDownLatch start = new CountDownLatch(1);
            for (int index = 0; index < callers; index++) {
                threads.submit(() -> {
                    start.await();
                    int statusCode = mockMvc.perform(submission(ACCOUNT_ONE_KEY, body))
                            .andReturn()
                            .getResponse()
                            .getStatus();
                    if (statusCode == 201) {
                        created.incrementAndGet();
                    } else if (statusCode == 200) {
                        replayed.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
            threads.shutdown();
            assertTrue(threads.awaitTermination(30, TimeUnit.SECONDS));
        }

        assertEquals(1, created.get());
        assertEquals(callers - 1, replayed.get());
    }

    @Test
    void matchesAgainstRestingLiquidityAndReportsBothFills() throws Exception {
        mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY,
                        limitBody(nextClientOrderId(), "MSFT", "SELL", 10, 42_000)))
                .andExpect(status().isCreated());

        mockMvc.perform(submission(
                        ACCOUNT_TWO_KEY, limitBody(nextClientOrderId(), "MSFT", "BUY", 4, 42_000)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("FILLED"))
                .andExpect(jsonPath("$.executions.length()").value(1))
                .andExpect(jsonPath("$.executions[0].quantity").value(4))
                .andExpect(jsonPath("$.executions[0].price").value(42_000))
                .andExpect(jsonPath("$.executions[0].liquidity").value("TAKER"));
    }

    @Test
    void rejectsOrdersAboveTheRiskLimitWithoutCreatingThem() throws Exception {
        mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY,
                        limitBody(nextClientOrderId(), "AAPL", "BUY", 1_001, 19_000)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.source").value("RISK"))
                .andExpect(jsonPath("$.reason").value("MAX_ORDER_QUANTITY_EXCEEDED"))
                .andExpect(jsonPath("$.orderId").isNumber());
    }

    @Test
    void rejectsInstrumentsThatAreNotEnabled() throws Exception {
        mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(nextClientOrderId(), "GOOG", "BUY", 1, 10_000)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("INSTRUMENT_NOT_TRADABLE"));
    }

    @Test
    void rejectsMarketOrdersOnInstrumentsWithNoReferencePrice() throws Exception {
        String body = """
                {"clientOrderId":"%s","symbol":"NOPRICE","side":"BUY","type":"MARKET","quantity":1}
                """.formatted(nextClientOrderId());

        mockMvc.perform(submission(ACCOUNT_ONE_KEY, body))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("REFERENCE_PRICE_UNAVAILABLE"));
    }

    @Test
    void repeatingARiskRejectedSubmissionReplaysTheSameRejection() throws Exception {
        String body = limitBody(nextClientOrderId(), "AAPL", "BUY", 1_001, 19_000);

        mockMvc.perform(submission(ACCOUNT_ONE_KEY, body))
                .andExpect(status().isUnprocessableEntity());
        mockMvc.perform(submission(ACCOUNT_ONE_KEY, body))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("MAX_ORDER_QUANTITY_EXCEEDED"));
    }

    @Test
    void rejectsMalformedRequests() throws Exception {
        mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(nextClientOrderId(), "AAPL", "BUY", 0, 19_000)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("INVALID_REQUEST"));

        String limitWithoutPrice = """
                {"clientOrderId":"%s","symbol":"AAPL","side":"BUY","type":"LIMIT","quantity":5}
                """.formatted(nextClientOrderId());
        mockMvc.perform(submission(ACCOUNT_ONE_KEY, limitWithoutPrice))
                .andExpect(status().isBadRequest());

        mockMvc.perform(submission(ACCOUNT_ONE_KEY, "{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("MALFORMED_REQUEST"));
    }

    @Test
    void refusesRequestsWithoutAValidApiKey() throws Exception {
        String body = limitBody(nextClientOrderId(), "AAPL", "BUY", 1, 19_000);

        mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.reason").value("UNAUTHENTICATED"));
        mockMvc.perform(submission("not-a-real-key", body)).andExpect(status().isUnauthorized());
    }

    @Test
    void cancelsAnOrderAndRefusesToCancelItTwice() throws Exception {
        MvcResult created = mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(nextClientOrderId(), "AAPL", "BUY", 4, 14_000)))
                .andExpect(status().isCreated())
                .andReturn();
        long orderId = orderId(created);

        mockMvc.perform(delete("/orders/" + orderId).header("X-Api-Key", ACCOUNT_ONE_KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mockMvc.perform(delete("/orders/" + orderId).header("X-Api-Key", ACCOUNT_ONE_KEY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("ORDER_NOT_ACTIVE"));
    }

    @Test
    void replacesAnOrderAndKeepsTheClientIdentity() throws Exception {
        String clientOrderId = nextClientOrderId();
        MvcResult created = mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(clientOrderId, "AAPL", "BUY", 4, 13_000)))
                .andExpect(status().isCreated())
                .andReturn();

        mockMvc.perform(put("/orders/" + orderId(created))
                        .header("X-Api-Key", ACCOUNT_ONE_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"quantity\":9,\"price\":13500}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clientOrderId").value(clientOrderId))
                .andExpect(jsonPath("$.quantity").value(9))
                .andExpect(jsonPath("$.price").value(13_500));
    }

    @Test
    void rejectsAReplacementThatWouldBreachARiskLimit() throws Exception {
        MvcResult created = mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(nextClientOrderId(), "AAPL", "BUY", 4, 12_000)))
                .andExpect(status().isCreated())
                .andReturn();

        mockMvc.perform(put("/orders/" + orderId(created))
                        .header("X-Api-Key", ACCOUNT_ONE_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"quantity\":1001,\"price\":12000}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("MAX_ORDER_QUANTITY_EXCEEDED"));
    }

    @Test
    void oneAccountCannotSeeOrTouchAnotherAccountsOrder() throws Exception {
        MvcResult created = mockMvc.perform(submission(
                        ACCOUNT_ONE_KEY, limitBody(nextClientOrderId(), "AAPL", "BUY", 1, 11_000)))
                .andExpect(status().isCreated())
                .andReturn();
        long orderId = orderId(created);

        mockMvc.perform(get("/orders/" + orderId).header("X-Api-Key", ACCOUNT_TWO_KEY))
                .andExpect(status().isNotFound());
        mockMvc.perform(delete("/orders/" + orderId).header("X-Api-Key", ACCOUNT_TWO_KEY))
                .andExpect(status().isNotFound());
        mockMvc.perform(get("/orders/" + orderId).header("X-Api-Key", ACCOUNT_ONE_KEY))
                .andExpect(status().isOk());
    }

    @Test
    void returnsAggregatedBookDepth() throws Exception {
        for (int index = 0; index < 3; index++) {
            mockMvc.perform(submission(
                            ACCOUNT_ONE_KEY,
                            limitBody(nextClientOrderId(), "AAPL", "SELL", 2, 25_000)))
                    .andExpect(status().isCreated());
        }

        mockMvc.perform(get("/orderbook/AAPL").header("X-Api-Key", ACCOUNT_ONE_KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.symbol").value("AAPL"))
                .andExpect(jsonPath("$.asks[0].price").value(25_000))
                .andExpect(jsonPath("$.asks[0].quantity").value(6))
                .andExpect(jsonPath("$.asks[0].orderCount").value(3));
    }

    @Test
    void unknownInstrumentsHaveNoBook() throws Exception {
        mockMvc.perform(get("/orderbook/GOOG").header("X-Api-Key", ACCOUNT_ONE_KEY))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.reason").value("INSTRUMENT_NOT_FOUND"));
    }

    private static MockHttpServletRequestBuilder submission(String apiKey, String body) {
        return post("/orders")
                .header("X-Api-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }

    private static String limitBody(
            String clientOrderId, String symbol, String side, long quantity, long price) {
        return """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"LIMIT",\
                "quantity":%d,"price":%d}
                """.formatted(clientOrderId, symbol, side, quantity, price);
    }

    private static String nextClientOrderId() {
        return "cl-" + CLIENT_ORDER_IDS.incrementAndGet();
    }

    private long orderId(MvcResult result) throws Exception {
        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        return body.get("orderId").asLong();
    }
}
