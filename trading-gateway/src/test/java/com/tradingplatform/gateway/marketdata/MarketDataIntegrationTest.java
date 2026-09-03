package com.tradingplatform.gateway.marketdata;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingplatform.gateway.pipeline.KafkaPipelineTestBase;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * The Stage 6 market-data path end to end: a live feed reachable with no authentication, over both
 * WebSocket and a REST snapshot, and — the piece that closes the gap Stage 5 left open — a
 * position's unrealized P&L moving on its own once the feed starts ticking, with no second trade.
 *
 * <p>Symbol {@code MDFEED} is reserved exclusively for this class, the same convention
 * {@code OUTAGE} follows in {@code KafkaOutageIntegrationTest}: the container-shared Postgres/Kafka
 * mean every other pipeline test's symbol is off limits for assertions that need an exact value.
 * Market data itself is switched off by default for the shared {@code pipeline-test} profile (see
 * {@code application-pipeline-test.yml}) precisely so it cannot perturb those other tests' mark
 * prices; this class turns it back on for itself alone, with its own dedicated symbol.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "trading.marketdata.enabled=true",
            "trading.marketdata.symbols=MDFEED",
            "trading.marketdata.events-per-second=200",
            "trading.marketdata.seed=42",
            "trading.marketdata.mark-price-flush-interval=200ms",
            "trading.instruments=AAPL:19000,MSFT:42000,NVDA:12000,TSLA:24000,NOPRICE:0,OUTAGE:15000,MDFEED:1000000"
        })
@ActiveProfiles("pipeline-test")
class MarketDataIntegrationTest extends KafkaPipelineTestBase {
    private static final Duration SETTLE_WITHIN = Duration.ofSeconds(10);

    @LocalServerPort private int port;
    @Autowired private TestRestTemplate rest;
    @Autowired private ObjectMapper objectMapper;

    @Test
    void aSubscribedWebSocketClientReceivesUpdatesWithNoAuthentication() throws Exception {
        var messages = new LinkedBlockingQueue<String>();
        StandardWebSocketClient client = new StandardWebSocketClient();
        WebSocketSession session = client
                .execute(new TextWebSocketHandler() {
                    @Override
                    protected void handleTextMessage(WebSocketSession s, TextMessage message) {
                        messages.offer(message.getPayload());
                    }
                }, "ws://localhost:" + port + "/marketdata")
                .get(5, TimeUnit.SECONDS);
        try {
            session.sendMessage(new TextMessage("{\"type\":\"subscribe\",\"symbols\":[\"MDFEED\"]}"));

            String received = messages.poll(5, TimeUnit.SECONDS);
            assertNotNull(received, "expected at least one market-data update over the socket");
            JsonNode update = objectMapper.readTree(received);
            assertEquals("MDFEED", update.get("symbol").asText());
        } finally {
            session.close();
        }
    }

    @Test
    void theRestSnapshotEndpointReflectsTheLiveFeedWithNoAuthentication() {
        await().atMost(SETTLE_WITHIN).untilAsserted(() -> {
            ResponseEntity<String> response = rest.getForEntity("/marketdata/MDFEED", String.class);
            assertEquals(HttpStatus.OK, response.getStatusCode());
        });
    }

    @Test
    void anUntrackedSymbolIsNotFound() {
        ResponseEntity<String> response = rest.getForEntity("/marketdata/NEVER-TRACKED", String.class);
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void theMarkPriceFeedMovesUnrealizedPnlWithoutASecondTrade() throws Exception {
        // Comfortably clear of the simulated feed's starting range (a few hundred to a few
        // thousand ticks) and within the shared profile's max-order-notional limit.
        long tradePrice = 5_000_000L;
        submitOrder("key-one", "mdfeed-maker", "MDFEED", "SELL", 4, tradePrice);
        submitOrder("key-two", "mdfeed-taker", "MDFEED", "BUY", 4, tradePrice);

        await().atMost(SETTLE_WITHIN).untilAsserted(
                () -> assertEquals(4, positionFor("key-two").get("netQuantity").asLong()));

        // No second trade from here. The feed has been ticking since the context started, and
        // MarkPriceUpdater flushes its reference price into the same row every 200ms, so the mark
        // eventually stops being the trade price on its own — proof the gap Stage 5 left open
        // (mark prices frozen at whatever last traded) is actually closed. This is deliberately
        // not asserted immediately after the trade: that instant is a race against the very feed
        // being tested, and could go either way depending on flush timing.
        await().atMost(SETTLE_WITHIN).untilAsserted(
                () -> assertNotEquals(tradePrice, positionFor("key-two").get("markPrice").asLong()));
    }

    private void submitOrder(
            String apiKey, String clientOrderId, String symbol, String side, long qty, long price) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", apiKey);
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"LIMIT",\
                "quantity":%d,"price":%d}
                """.formatted(clientOrderId, symbol, side, qty, price);
        ResponseEntity<String> response =
                rest.postForEntity("/orders", new HttpEntity<>(body, headers), String.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
    }

    private JsonNode positionFor(String apiKey) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", apiKey);
        ResponseEntity<String> response =
                rest.exchange("/positions", org.springframework.http.HttpMethod.GET, new HttpEntity<>(headers), String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        JsonNode positions = objectMapper.readTree(response.getBody());
        Iterator<JsonNode> elements = positions.elements();
        while (elements.hasNext()) {
            JsonNode candidate = elements.next();
            if (candidate.get("symbol").asText().equals("MDFEED")) {
                return candidate;
            }
        }
        throw new AssertionError("no MDFEED position for " + apiKey);
    }
}
