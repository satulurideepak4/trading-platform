package com.tradingplatform.gateway.pipeline;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.MatchingEngine;
import com.tradingplatform.portfolio.PortfolioReadModel;
import com.tradingplatform.portfolio.PositionKey;
import com.tradingplatform.risk.AccountRiskSnapshot;
import com.tradingplatform.risk.PreTradeRiskEngine;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

/**
 * The Master-Prompt's Stage 11 end-to-end shape, generalized from fixed scenarios to a seeded
 * randomized workload: generate market activity, submit orders over HTTP, let risk and matching
 * decide the outcome, let the outcome cross Kafka, and verify the resulting positions converge to
 * an independently computed answer rather than one hand-picked trade at a time.
 *
 * <p>The independent answer comes from running the identical generated order sequence through a
 * bare {@link MatchingEngine} — the same algorithm the gateway uses internally, but with no HTTP,
 * risk, Kafka or Postgres in between — and reducing its {@link Execution} results to a net
 * quantity per account. If the real, full-stack path agrees with that bare-engine reference for
 * every account, risk's synchronous view and the durable Postgres view, then price-time priority,
 * routing, the event pipeline and position accounting all agree end to end on the same input.
 *
 * <p>Extends {@link ExecutionPipelineIntegrationTest}'s test base and reuses its package-private
 * {@code order}/{@code orderFor}/{@code nextClientOrderId} helpers rather than duplicating them.
 *
 * <p>Each full-stack run below uses its own never-before-traded symbol (SEEDA/SEEDB/SEEDC,
 * reserved in {@code application-pipeline-test.yml}), not just its own strategy id. Positions are
 * isolated by strategy, but the real order book is shared per symbol across every test class in
 * this JVM (one Spring context, one matching engine), and a resting order left behind by another
 * test — or by an earlier run in this same class — would make the bare-engine reference below
 * wrong, since it assumes an empty starting book.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("pipeline-test")
class SeededWorkloadIntegrationTest extends KafkaPipelineTestBase {
    private static final Duration SETTLE_WITHIN = Duration.ofSeconds(20);
    private static final Instant BASE_TIME = Instant.parse("2026-08-22T09:30:00Z");
    private static final int ORDER_COUNT = 50;
    // Three of the four fixed test accounts/API keys configured in application-pipeline-test.yml.
    private static final List<String> ACCOUNTS = List.of("ACC-1", "ACC-2", "ACC-3");
    private static final Map<String, String> API_KEY_BY_ACCOUNT =
            Map.of("ACC-1", "key-one", "ACC-2", "key-two", "ACC-3", "key-three");

    @Autowired private MockMvc mockMvc;
    @Autowired private PortfolioReadModel portfolio;
    @Autowired private PreTradeRiskEngine riskEngine;

    @Test
    void aSeededRandomOrderSequenceConvergesToAConsistentFinalStateAcrossRiskAndPortfolio()
            throws Exception {
        List<GeneratedOrder> generated = generate(2026_08_22L);
        Map<String, Long> expectedNet = expectedNetQuantities(generated);

        submitSequence(generated, "SEEDA", "seeded-workload");

        await().atMost(SETTLE_WITHIN).untilAsserted(() -> {
            for (String account : ACCOUNTS) {
                assertEquals(
                        expectedNet.getOrDefault(account, 0L),
                        durablePositionNetQuantity(account, "SEEDA", "seeded-workload"),
                        "durable portfolio net quantity mismatch for " + account);
                assertEquals(
                        expectedNet.getOrDefault(account, 0L),
                        riskExposureNetPosition(account, "SEEDA"),
                        "risk exposure net position mismatch for " + account);
            }
        });

        // Every execution has exactly one buyer and one seller among the same closed set of
        // accounts, so net exposure across the whole set must sum to zero.
        assertEquals(0L, expectedNet.values().stream().mapToLong(Long::longValue).sum());
    }

    @Test
    void replayingTheSameSeededSequenceThroughTheFullStackReachesTheSameFinalPositions()
            throws Exception {
        List<GeneratedOrder> generated = generate(2026_08_22L);
        Map<String, Long> expectedNet = expectedNetQuantities(generated);

        // Two independent runs of the identical logical sequence, each against its own reserved,
        // never-before-traded symbol so the real order book starts empty both times exactly like
        // the reference engine's does. Both must converge on the same reference answer computed
        // once above — proving the real HTTP -> risk -> matching -> Kafka -> Postgres path is
        // deterministic for the same input, not just the bare engine.
        submitSequence(generated, "SEEDB", "seeded-workload-replay-run1");
        submitSequence(generated, "SEEDC", "seeded-workload-replay-run2");

        await().atMost(SETTLE_WITHIN).untilAsserted(() -> {
            for (String account : ACCOUNTS) {
                long expected = expectedNet.getOrDefault(account, 0L);
                assertEquals(
                        expected,
                        durablePositionNetQuantity(account, "SEEDB", "seeded-workload-replay-run1"),
                        "run 1 mismatch for " + account);
                assertEquals(
                        expected,
                        durablePositionNetQuantity(account, "SEEDC", "seeded-workload-replay-run2"),
                        "run 2 mismatch for " + account);
            }
        });
    }

    private void submitSequence(List<GeneratedOrder> generated, String symbol, String strategyId)
            throws Exception {
        for (GeneratedOrder order : generated) {
            mockMvc.perform(ExecutionPipelineIntegrationTest.orderFor(
                            strategyId,
                            API_KEY_BY_ACCOUNT.get(order.account()),
                            ExecutionPipelineIntegrationTest.nextClientOrderId(),
                            symbol,
                            order.side().name(),
                            order.quantity(),
                            order.price()))
                    .andExpect(status().isCreated());
        }
    }

    private long durablePositionNetQuantity(String account, String symbol, String strategyId) {
        return portfolio
                .position(new PositionKey(account, strategyId, symbol))
                .map(PortfolioReadModel.PositionView::netQuantity)
                .orElse(0L);
    }

    private long riskExposureNetPosition(String account, String symbol) {
        return riskEngine.snapshot(account).exposures().stream()
                .filter(exposure -> exposure.symbol().equals(symbol))
                .map(AccountRiskSnapshot.SymbolExposure::netPosition)
                .findFirst()
                .orElse(0L);
    }

    // --- generation and the independent reference answer -----------------------------------

    private record GeneratedOrder(String account, Side side, long quantity, long price) {}

    /** A tight band around one mid so generated limit orders actually cross often. */
    private static long randomPrice(SplittableRandom random) {
        return 11_900 + random.nextInt(11) * 20L;
    }

    private static List<GeneratedOrder> generate(long seed) {
        SplittableRandom random = new SplittableRandom(seed);
        List<GeneratedOrder> generated = new ArrayList<>(ORDER_COUNT);
        for (int i = 0; i < ORDER_COUNT; i++) {
            String account = ACCOUNTS.get(random.nextInt(ACCOUNTS.size()));
            Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
            long quantity = 1 + random.nextInt(10);
            generated.add(new GeneratedOrder(account, side, quantity, randomPrice(random)));
        }
        return generated;
    }

    /**
     * Runs the identical sequence through a bare engine (no HTTP, risk, Kafka or Postgres) and
     * reduces its executions to a net quantity per account — the answer the full stack must match.
     */
    private static Map<String, Long> expectedNetQuantities(List<GeneratedOrder> generated) {
        // The symbol string is arbitrary here: the bare engine's matching outcome does not depend
        // on it, only on arrival order, side, quantity and price within one book.
        String referenceSymbol = "REFERENCE";
        MatchingEngine reference = new MatchingEngine();
        Map<Long, String> accountByOrderId = new LinkedHashMap<>();
        Map<String, Long> netQuantity = new LinkedHashMap<>();
        for (String account : ACCOUNTS) {
            netQuantity.put(account, 0L);
        }

        long orderId = 1;
        for (GeneratedOrder generatedOrder : generated) {
            accountByOrderId.put(orderId, generatedOrder.account());
            SubmitOrder command = SubmitOrder.limit(
                    orderId,
                    "seed-" + orderId,
                    referenceSymbol,
                    generatedOrder.side(),
                    generatedOrder.quantity(),
                    generatedOrder.price(),
                    BASE_TIME.plusNanos(orderId));
            CommandResult result = reference.submit(command);
            for (Execution execution : result.executions()) {
                String buyAccount = accountByOrderId.get(execution.buyOrderId());
                String sellAccount = accountByOrderId.get(execution.sellOrderId());
                netQuantity.merge(buyAccount, execution.quantity(), Long::sum);
                netQuantity.merge(sellAccount, -execution.quantity(), Long::sum);
            }
            orderId++;
        }
        return netQuantity;
    }
}
