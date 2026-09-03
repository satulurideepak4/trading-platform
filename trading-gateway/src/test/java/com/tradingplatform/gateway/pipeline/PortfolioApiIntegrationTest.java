package com.tradingplatform.gateway.pipeline;

import static com.tradingplatform.gateway.pipeline.ExecutionPipelineIntegrationTest.nextClientOrderId;
import static com.tradingplatform.gateway.pipeline.ExecutionPipelineIntegrationTest.order;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * The read side of the durable portfolio, exercised over HTTP: a trade made through the ordinary
 * order endpoints has to be visible through {@code /positions}, {@code /executions} and
 * {@code /pnl} once the pipeline has caught up, and never visible to an account that was not party
 * to it.
 *
 * <p>Assertions parse the response and locate a row by symbol in Java rather than using a jsonPath
 * filter expression, because a filter always evaluates to an array even when exactly one row
 * matches, and the account in a shared-context test can accumulate positions in more than one
 * symbol from other test methods.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("pipeline-test")
class PortfolioApiIntegrationTest extends KafkaPipelineTestBase {
    private static final Duration SETTLE_WITHIN = Duration.ofSeconds(20);

    @Autowired private MockMvc mockMvc;
    @Autowired private ObjectMapper objectMapper;

    @Test
    void aTradeAppearsInPositionsExecutionsAndPnlForBothSides() throws Exception {
        mockMvc.perform(order("key-one", nextClientOrderId(), "TSLA", "SELL", 10, 24_000))
                .andExpect(status().isCreated());
        mockMvc.perform(order("key-two", nextClientOrderId(), "TSLA", "BUY", 6, 24_000))
                .andExpect(status().isCreated());

        await().atMost(SETTLE_WITHIN).untilAsserted(() ->
                assertEquals(6, positionFor("key-two", "TSLA").get("netQuantity").asLong()));

        // The maker offered 10 but only 6 crossed with the taker's buy; the other 4 are still
        // resting on the book and have not sold, so the position reflects only what executed.
        JsonNode makerPosition = positionFor("key-one", "TSLA");
        assertEquals(-6, makerPosition.get("netQuantity").asLong());
        assertEquals(0, makerPosition.get("boughtQuantity").asLong());
        assertEquals(6, makerPosition.get("soldQuantity").asLong());

        JsonNode takerTrade = executionsFor("key-two").get(0);
        assertEquals("TSLA", takerTrade.get("symbol").asText());
        assertEquals("BUY", takerTrade.get("side").asText());
        assertEquals("TAKER", takerTrade.get("liquidity").asText());
        assertEquals(6, takerTrade.get("quantity").asLong());

        JsonNode makerTrade = executionsFor("key-one").get(0);
        assertEquals("SELL", makerTrade.get("side").asText());
        assertEquals("MAKER", makerTrade.get("liquidity").asText());

        JsonNode pnl = readJson(mockMvc.perform(get("/pnl").header("X-Api-Key", "key-two"))
                .andExpect(status().isOk())
                .andReturn());
        assertEquals("ACC-2", pnl.get("accountId").asText());
        assertEquals(6, positionIn(pnl.get("positions"), "TSLA").get("netQuantity").asLong());
    }

    @Test
    void positionsAreNeverVisibleToAnAccountThatWasNotPartyToThem() throws Exception {
        mockMvc.perform(order("key-three", nextClientOrderId(), "NOPRICE", "SELL", 5, 200))
                .andExpect(status().isCreated());
        mockMvc.perform(order("key-four", nextClientOrderId(), "NOPRICE", "BUY", 5, 200))
                .andExpect(status().isCreated());

        await().atMost(SETTLE_WITHIN).untilAsserted(() ->
                assertEquals(5, positionFor("key-four", "NOPRICE").get("netQuantity").asLong()));

        // key-one/ACC-1 never traded NOPRICE and must not see either side's position.
        JsonNode positions = readJson(mockMvc.perform(get("/positions").header("X-Api-Key", "key-one"))
                .andExpect(status().isOk())
                .andReturn());
        assertFalse(findPosition(positions, "NOPRICE").isPresent());
    }

    @Test
    void aManuallySetMarkPriceChangesUnrealizedPnlWithoutATrade() throws Exception {
        mockMvc.perform(order("key-one", nextClientOrderId(), "MSFT", "SELL", 3, 42_000))
                .andExpect(status().isCreated());
        mockMvc.perform(order("key-two", nextClientOrderId(), "MSFT", "BUY", 3, 42_000))
                .andExpect(status().isCreated());
        await().atMost(SETTLE_WITHIN).untilAsserted(() ->
                assertEquals(3, positionFor("key-two", "MSFT").get("netQuantity").asLong()));

        mockMvc.perform(put("/instruments/MSFT/mark-price")
                        .header("X-Api-Key", "key-two")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"price\":43000}"))
                .andExpect(status().isNoContent());

        JsonNode position = positionFor("key-two", "MSFT");
        assertEquals(43_000, position.get("markPrice").asLong());
        // 3 units * (43000 - 42000) = 3000 unrealized, without a second trade.
        assertEquals(3_000, position.get("unrealizedPnl").asLong());
    }

    @Test
    void theRiskExposureEndpointReflectsWorkingOrdersImmediatelyRatherThanAfterKafka()
            throws Exception {
        mockMvc.perform(order("key-one", nextClientOrderId(), "AAPL", "BUY", 7, 19_000))
                .andExpect(status().isCreated());

        // No await(): the risk view is synchronous, so it must already be correct.
        mockMvc.perform(get("/risk/exposure").header("X-Api-Key", "key-one"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accountId").value("ACC-1"));

        JsonNode exposures = readJson(mockMvc.perform(get("/risk/exposure").header("X-Api-Key", "key-one"))
                .andReturn());
        JsonNode aapl = findPosition(exposures.get("exposures"), "AAPL")
                .orElseThrow(() -> new AssertionError("AAPL exposure missing"));
        assertTrue(aapl.get("workingBuyQuantity").asLong() >= 7);
    }

    @Test
    void portfolioEndpointsRequireAuthenticationLikeEveryOtherTradingEndpoint() throws Exception {
        mockMvc.perform(get("/positions")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/executions")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/pnl")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/risk/exposure")).andExpect(status().isUnauthorized());
    }

    @Test
    void settingAMarkPriceForAnUnknownInstrumentIsNotFound() throws Exception {
        mockMvc.perform(put("/instruments/DOESNOTEXIST/mark-price")
                        .header("X-Api-Key", "key-one")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"price\":100}"))
                .andExpect(status().isNotFound());
    }

    private JsonNode positionFor(String apiKey, String symbol) throws Exception {
        JsonNode positions = readJson(mockMvc.perform(get("/positions").header("X-Api-Key", apiKey))
                .andExpect(status().isOk())
                .andReturn());
        return findPosition(positions, symbol)
                .orElseThrow(() -> new AssertionError("no " + symbol + " position for " + apiKey));
    }

    private JsonNode executionsFor(String apiKey) throws Exception {
        return readJson(mockMvc.perform(get("/executions").header("X-Api-Key", apiKey))
                .andExpect(status().isOk())
                .andReturn());
    }

    private static java.util.Optional<JsonNode> findPosition(JsonNode array, String symbol) {
        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            JsonNode candidate = elements.next();
            if (candidate.get("symbol").asText().equals(symbol)) {
                return java.util.Optional.of(candidate);
            }
        }
        return java.util.Optional.empty();
    }

    private static JsonNode positionIn(JsonNode array, String symbol) {
        return findPosition(array, symbol)
                .orElseThrow(() -> new AssertionError("no " + symbol + " entry"));
    }

    private JsonNode readJson(MvcResult result) throws Exception {
        return objectMapper.readTree(result.getResponse().getContentAsString());
    }
}
