package com.tradingplatform.gateway.matching;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingplatform.gateway.TradingGatewayApplication;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * The gateway-level proof to go with {@code OrderRouterRecoveryTest} in {@code matching-engine}:
 * not just that the class recovers state in a unit test, but that a real {@link
 * TradingGatewayApplication} context, started twice against the same journal directory, is the
 * same book the second time.
 *
 * <p>Deliberately two independent {@link ConfigurableApplicationContext}s rather than one
 * {@code @SpringBootTest} — a single context reused across assertions would not prove anything
 * about surviving a restart, only that the object graph works. No Kafka or Postgres here: matching-
 * engine recovery is orthogonal to the pipeline, and the {@code test} profile (pipeline and market
 * data both off) is enough to prove it with neither Testcontainer.
 */
class MatchingRestartIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path journalDir;

    @Test
    void aRestartedGatewayRecoversARestingOrder() throws Exception {
        ConfigurableApplicationContext first = startGateway();
        try {
            TestRestTemplate client = restTemplate(first);
            ResponseEntity<String> response = submitOrder(
                    client, "key-one", "restart-order-1", "AAPL", "SELL", 10, 19_500);
            assertEquals(HttpStatus.CREATED, response.getStatusCode());
        } finally {
            // Not a graceful drain on purpose: closing the context here is the process boundary
            // this test needs, not a claim about what a clean shutdown would additionally do.
            first.close();
        }

        ConfigurableApplicationContext second = startGateway();
        try {
            JsonNode book = getBook(restTemplate(second), "AAPL");
            JsonNode topAsk = book.get("asks").get(0);
            assertEquals(19_500, topAsk.get("price").asLong());
            assertEquals(10, topAsk.get("quantity").asLong());
            assertEquals(1, topAsk.get("orderCount").asInt());
        } finally {
            second.close();
        }
    }

    @Test
    void aFreshJournalDirectoryStartsWithAnEmptyBook() throws Exception {
        ConfigurableApplicationContext context = startGateway();
        try {
            JsonNode book = getBook(restTemplate(context), "AAPL");
            assertEquals(0, book.get("asks").size());
            assertEquals(0, book.get("bids").size());
        } finally {
            context.close();
        }
    }

    private ConfigurableApplicationContext startGateway() {
        // Command-line-style args, not .properties(...): SpringApplicationBuilder#properties adds
        // *default* properties, lower priority than application.yml, so server.port=8080 there
        // would win over it and every second context in a test would fail to bind. Args are the
        // highest-priority source, which is what overriding a configured default actually needs.
        return new SpringApplicationBuilder(TradingGatewayApplication.class)
                .profiles("test")
                .run(
                        "--server.port=0",
                        "--trading.matching.journal-enabled=true",
                        "--trading.matching.journal-directory=" + journalDir);
    }

    private static TestRestTemplate restTemplate(ConfigurableApplicationContext context) {
        String port = context.getEnvironment().getProperty("local.server.port");
        return new TestRestTemplate(new org.springframework.boot.web.client.RestTemplateBuilder()
                .rootUri("http://localhost:" + port));
    }

    private static ResponseEntity<String> submitOrder(
            TestRestTemplate client,
            String apiKey,
            String clientOrderId,
            String symbol,
            String side,
            long quantity,
            long price) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", apiKey);
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"LIMIT",\
                "quantity":%d,"price":%d}
                """.formatted(clientOrderId, symbol, side, quantity, price);
        return client.postForEntity("/orders", new HttpEntity<>(body, headers), String.class);
    }

    private static JsonNode getBook(TestRestTemplate client, String symbol) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", "key-one");
        ResponseEntity<String> response = client.exchange(
                "/orderbook/" + symbol,
                org.springframework.http.HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        return JSON.readTree(response.getBody());
    }
}
