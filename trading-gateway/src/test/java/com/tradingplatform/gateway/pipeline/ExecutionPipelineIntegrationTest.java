package com.tradingplatform.gateway.pipeline;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradingplatform.gateway.api.SubmitOrderRequest;
import com.tradingplatform.pipeline.TradingTopics;
import com.tradingplatform.pipeline.consume.ExecutionProcessor;
import com.tradingplatform.pipeline.consume.ReplayReadiness;
import com.tradingplatform.pipeline.consume.RiskStateUpdater;
import com.tradingplatform.pipeline.events.EventIds;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.pipeline.store.AuditLog;
import com.tradingplatform.pipeline.store.ExecutionStore;
import com.tradingplatform.portfolio.PortfolioProcessor;
import com.tradingplatform.portfolio.PortfolioReadModel;
import com.tradingplatform.portfolio.PortfolioReadModel.PositionView;
import com.tradingplatform.portfolio.PositionKey;
import com.tradingplatform.risk.AccountRiskSnapshot;
import com.tradingplatform.risk.PreTradeRiskEngine;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * End-to-end: an HTTP order becomes a trade, the trade becomes Kafka records, and every downstream
 * projection — including the durable Postgres one — converges on the same answer.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("pipeline-test")
class ExecutionPipelineIntegrationTest extends KafkaPipelineTestBase {
    private static final AtomicInteger CLIENT_ORDER_IDS = new AtomicInteger();
    private static final Duration SETTLE_WITHIN = Duration.ofSeconds(20);
    private static final String STRATEGY = SubmitOrderRequest.DEFAULT_STRATEGY;

    @Autowired private MockMvc mockMvc;
    @Autowired private ExecutionStore executions;
    @Autowired private PortfolioReadModel portfolio;
    @Autowired private AuditLog auditLog;
    @Autowired private PreTradeRiskEngine riskEngine;
    @Autowired private ReplayReadiness replayReadiness;
    @Autowired private KafkaTemplate<String, TradingEvent> eventTemplate;

    @Test
    void aTradeReachesEveryDownstreamProjectionIncludingPostgres() throws Exception {
        String maker = nextClientOrderId();
        String taker = nextClientOrderId();

        mockMvc.perform(order("key-one", maker, "NVDA", "SELL", 10, 12_000))
                .andExpect(status().isCreated());
        mockMvc.perform(order("key-two", taker, "NVDA", "BUY", 4, 12_000))
                .andExpect(status().isCreated());

        await().atMost(SETTLE_WITHIN).untilAsserted(() -> {
            assertEquals(4, positionOf("ACC-2", "NVDA").netQuantity());
            assertEquals(-4, positionOf("ACC-1", "NVDA").netQuantity());
        });

        // The trade is recorded once, on both sides, by consumers that never spoke to each other.
        assertEquals(1, positionOf("ACC-2", "NVDA").executionCount());
        assertEquals(4, positionOf("ACC-2", "NVDA").boughtQuantity());
        assertEquals(4, positionOf("ACC-1", "NVDA").soldQuantity());
        assertTrue(executions.size() >= 1);
    }

    @Test
    void theAuditTrailKeepsRejectionsAsWellAsTrades() throws Exception {
        String rejected = nextClientOrderId();

        mockMvc.perform(order("key-one", rejected, "TSLA", "BUY", 1_001, 24_000))
                .andExpect(status().isUnprocessableEntity());

        await().atMost(SETTLE_WITHIN).untilAsserted(() -> assertTrue(
                auditLog.recent(500).stream()
                        .anyMatch(event -> event.eventId().startsWith("rej-")),
                "a risk rejection should reach the audit trail"));
    }

    @Test
    void redeliveringTheSameExecutionDoesNotMoveThePositionTwice() {
        long executionId = 900_001L;
        ExecutionCreated execution = execution(executionId, "TSLA", "ACC-3", "ACC-4", 7, 24_000);

        eventTemplate.send(TradingTopics.EXECUTIONS, execution.symbol(), execution);
        await().atMost(SETTLE_WITHIN)
                .untilAsserted(() -> assertEquals(7, positionOf("ACC-3", "TSLA").netQuantity()));

        // Kafka is allowed to redeliver, and a producer retry looks exactly like this. The
        // execution's primary key in Postgres is what makes the second and third insert no-ops.
        eventTemplate.send(TradingTopics.EXECUTIONS, execution.symbol(), execution);
        eventTemplate.send(TradingTopics.EXECUTIONS, execution.symbol(), execution);

        await().during(Duration.ofSeconds(3)).atMost(SETTLE_WITHIN).untilAsserted(() -> {
            assertEquals(7, positionOf("ACC-3", "TSLA").netQuantity());
            assertEquals(-7, positionOf("ACC-4", "TSLA").netQuantity());
            assertEquals(1, positionOf("ACC-3", "TSLA").executionCount());
        });
    }

    @Test
    void anAccountOnBothSidesOfATradeBooksBothLegs() {
        long executionId = 900_002L;
        ExecutionCreated selfTrade = execution(executionId, "AAPL", "ACC-2", "ACC-2", 5, 19_000);

        eventTemplate.send(TradingTopics.EXECUTIONS, selfTrade.symbol(), selfTrade);

        // Both legs land on one position, so they must cancel out rather than book only once.
        await().atMost(SETTLE_WITHIN).untilAsserted(() -> {
            assertEquals(5, positionOf("ACC-2", "AAPL").boughtQuantity());
            assertEquals(5, positionOf("ACC-2", "AAPL").soldQuantity());
            assertEquals(0, positionOf("ACC-2", "AAPL").netQuantity());
        });
    }

    @Test
    void theRiskProjectionAgreesWithTheDurablePortfolio() throws Exception {
        mockMvc.perform(order("key-three", nextClientOrderId(), "MSFT", "SELL", 8, 42_000))
                .andExpect(status().isCreated());
        mockMvc.perform(order("key-four", nextClientOrderId(), "MSFT", "BUY", 8, 42_000))
                .andExpect(status().isCreated());

        // The gateway applied this synchronously to risk; the portfolio consumer applies the same
        // fill to Postgres. If the two disagreed on the execution id, one of them would double up.
        await().during(Duration.ofSeconds(3)).atMost(SETTLE_WITHIN).untilAsserted(() -> {
            assertEquals(8, riskPositionOf("ACC-4", "MSFT"));
            assertEquals(-8, riskPositionOf("ACC-3", "MSFT"));
            assertEquals(8, positionOf("ACC-4", "MSFT").netQuantity());
            assertEquals(-8, positionOf("ACC-3", "MSFT").netQuantity());
        });
    }

    @Test
    void averageEntryPriceAndRealizedPnlAreReportedAfterAPartialClose() throws Exception {
        // A private strategy id, not just a private symbol: the containers are shared with every
        // other test in this class and with PipelineFailureIntegrationTest, and ACC-1/ACC-2/AAPL
        // are all reused elsewhere. Positions are keyed by (account, strategy, symbol), so a
        // strategy no other test uses is what actually guarantees this position starts flat.
        String strategy = "pnl-partial-close";

        // ACC-1 opens a long of 10 against ACC-2's offer, then closes 4 of it at a better price
        // against ACC-2's bid. What is left should still average the original entry price.
        mockMvc.perform(orderFor(strategy, "key-two", nextClientOrderId(), "AAPL", "SELL", 10, 19_000))
                .andExpect(status().isCreated());
        mockMvc.perform(orderFor(strategy, "key-one", nextClientOrderId(), "AAPL", "BUY", 10, 19_000))
                .andExpect(status().isCreated());
        mockMvc.perform(orderFor(strategy, "key-two", nextClientOrderId(), "AAPL", "BUY", 4, 19_200))
                .andExpect(status().isCreated());
        mockMvc.perform(orderFor(strategy, "key-one", nextClientOrderId(), "AAPL", "SELL", 4, 19_200))
                .andExpect(status().isCreated());

        await().atMost(SETTLE_WITHIN).untilAsserted(() -> {
            PositionView position = portfolio
                    .position(new PositionKey("ACC-1", strategy, "AAPL"))
                    .orElseThrow(() -> new AssertionError("no position yet"));
            assertEquals(6, position.netQuantity());
            assertEquals(0, java.math.BigDecimal.valueOf(19_000)
                    .compareTo(position.averageEntryPrice()));
            // (19200 - 19000) * 4 realized on the part closed; the other 6 are still open.
            assertEquals(800, position.realizedPnl());
        });
    }

    @Test
    void consumersReportThemselvesCaughtUpSoTheGatewayCanBecomeReady() {
        await().atMost(SETTLE_WITHIN).untilAsserted(() -> {
            assertTrue(replayReadiness.isCaughtUp(RiskStateUpdater.GROUP));
            assertTrue(replayReadiness.isCaughtUp(ExecutionProcessor.GROUP));
            assertTrue(replayReadiness.isCaughtUp(PortfolioProcessor.GROUP));
        });
    }

    private PositionView positionOf(String accountId, String symbol) {
        return portfolio
                .position(new PositionKey(accountId, STRATEGY, symbol))
                .orElseThrow(() -> new AssertionError("no position for " + accountId + " " + symbol));
    }

    private long riskPositionOf(String accountId, String symbol) {
        return riskEngine.snapshot(accountId).exposures().stream()
                .filter(exposure -> exposure.symbol().equals(symbol))
                .map(AccountRiskSnapshot.SymbolExposure::netPosition)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no risk exposure for " + accountId));
    }

    static ExecutionCreated execution(
            long executionId,
            String symbol,
            String buyAccount,
            String sellAccount,
            long quantity,
            long price) {
        return new ExecutionCreated(
                EventIds.execution(executionId),
                "test-correlation",
                executionId,
                symbol,
                price,
                quantity,
                executionId * 10,
                buyAccount,
                STRATEGY,
                executionId * 10 + 1,
                sellAccount,
                STRATEGY,
                executionId * 10 + 1,
                executionId * 10,
                0,
                0,
                Instant.parse("2026-08-18T10:00:00Z"));
    }

    static String nextClientOrderId() {
        return "pipe-" + CLIENT_ORDER_IDS.incrementAndGet();
    }

    static MockHttpServletRequestBuilder order(
            String apiKey, String clientOrderId, String symbol, String side, long qty, long price) {
        String body = """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"LIMIT",\
                "quantity":%d,"price":%d}
                """.formatted(clientOrderId, symbol, side, qty, price);
        return post("/orders")
                .header("X-Api-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }

    static MockHttpServletRequestBuilder orderFor(
            String strategyId,
            String apiKey,
            String clientOrderId,
            String symbol,
            String side,
            long qty,
            long price) {
        String body = """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"LIMIT",\
                "quantity":%d,"price":%d,"strategyId":"%s"}
                """.formatted(clientOrderId, symbol, side, qty, price, strategyId);
        return post("/orders")
                .header("X-Api-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }
}
