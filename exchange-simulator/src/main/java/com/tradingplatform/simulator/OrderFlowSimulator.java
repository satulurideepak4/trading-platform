package com.tradingplatform.simulator;

import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.MatchingEngine;
import java.time.Instant;
import java.util.Locale;
import java.util.SplittableRandom;

public final class OrderFlowSimulator {
    private static final int DEFAULT_ORDER_COUNT = 10_000;
    private static final long DEFAULT_SEED = 7_921L;
    private static final Instant BASE_TIME = Instant.parse("2025-01-01T00:00:00Z");
    private static final String[] SYMBOLS = {"AAPL", "MSFT", "NVDA", "EURUSD"};

    private OrderFlowSimulator() {}

    public static void main(String[] args) {
        int orderCount = parseOrderCount(args);
        long seed = args.length > 1 ? Long.parseLong(args[1]) : DEFAULT_SEED;
        SplittableRandom random = new SplittableRandom(seed);
        MatchingEngine engine = new MatchingEngine();

        long accepted = 0;
        long rejected = 0;
        long executions = 0;
        long startedAt = System.nanoTime();
        for (long orderId = 1; orderId <= orderCount; orderId++) {
            SubmitOrder order = generatedOrder(orderId, random);
            CommandResult result = engine.submit(order);
            if (result.accepted()) {
                accepted++;
                executions += result.executions().size();
            } else {
                rejected++;
            }
        }
        long elapsedNanos = System.nanoTime() - startedAt;
        double elapsedMillis = elapsedNanos / 1_000_000.0;

        System.out.printf(
                Locale.ROOT,
                "event=simulation_complete orders=%d accepted=%d rejected=%d executions=%d seed=%d elapsedMs=%.3f%n",
                orderCount,
                accepted,
                rejected,
                executions,
                seed,
                elapsedMillis);
        System.out.println(
                "note=This is a functional workload smoke test, not a controlled benchmark result.");
    }

    private static SubmitOrder generatedOrder(long orderId, SplittableRandom random) {
        String symbol = SYMBOLS[random.nextInt(SYMBOLS.length)];
        Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
        long quantity = random.nextLong(1, 101);
        Instant timestamp = BASE_TIME.plusNanos(orderId);
        if (random.nextInt(20) == 0) {
            return SubmitOrder.market(
                    orderId, "sim-" + orderId, symbol, side, quantity, timestamp);
        }
        long price = random.nextLong(9_900, 10_101);
        return SubmitOrder.limit(
                orderId, "sim-" + orderId, symbol, side, quantity, price, timestamp);
    }

    private static int parseOrderCount(String[] args) {
        if (args.length == 0) {
            return DEFAULT_ORDER_COUNT;
        }
        int count = Integer.parseInt(args[0]);
        if (count <= 0) {
            throw new IllegalArgumentException("order count must be positive");
        }
        return count;
    }
}
