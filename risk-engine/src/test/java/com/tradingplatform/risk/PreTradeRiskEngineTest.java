package com.tradingplatform.risk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PreTradeRiskEngineTest {
    private static final String ACCOUNT = "ACC-1";

    private final InstrumentRegistry instruments = new InstrumentRegistry(List.of(
            new InstrumentRegistry.Instrument("AAPL", 10_000),
            new InstrumentRegistry.Instrument("MSFT", 0)));

    @Test
    void rejectsInstrumentsThatAreNotEnabled() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 5_000));

        RiskDecision decision = engine.reserve(ACCOUNT, limit(1, "TSLA", Side.BUY, 10, 10_000));

        assertRejected(decision, RiskRejectionReason.INSTRUMENT_NOT_TRADABLE);
    }

    @Test
    void rejectsOrdersAboveTheMaximumQuantity() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(100, 100_000_000, 10, 5_000));

        assertTrue(engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 100, 10_000)).approved());
        assertRejected(
                engine.reserve(ACCOUNT, limit(2, "AAPL", Side.BUY, 101, 10_000)),
                RiskRejectionReason.MAX_ORDER_QUANTITY_EXCEEDED);
    }

    @Test
    void rejectsOrdersAboveTheMaximumNotional() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 1_000_000, 10, 5_000));

        assertTrue(engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 100, 10_000)).approved());
        assertRejected(
                engine.reserve(ACCOUNT, limit(2, "AAPL", Side.BUY, 101, 10_000)),
                RiskRejectionReason.MAX_ORDER_NOTIONAL_EXCEEDED);
    }

    @Test
    void sizesMarketOrdersWithTheInstrumentReferencePrice() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 1_000_000, 10, 5_000));

        assertTrue(engine.reserve(ACCOUNT, market(1, "AAPL", Side.BUY, 100)).approved());
        assertRejected(
                engine.reserve(ACCOUNT, market(2, "AAPL", Side.BUY, 101)),
                RiskRejectionReason.MAX_ORDER_NOTIONAL_EXCEEDED);
    }

    @Test
    void rejectsMarketOrdersOnInstrumentsWithoutAReferencePrice() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 5_000));

        assertRejected(
                engine.reserve(ACCOUNT, market(1, "MSFT", Side.BUY, 10)),
                RiskRejectionReason.REFERENCE_PRICE_UNAVAILABLE);
        assertTrue(engine.reserve(ACCOUNT, limit(2, "MSFT", Side.BUY, 10, 9_000)).approved());
    }

    @Test
    void recordedTradesMakeMarketOrdersPriceableAgain() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 5_000));
        instruments.recordTradedPrice("MSFT", 42_000);

        assertTrue(engine.reserve(ACCOUNT, market(1, "MSFT", Side.BUY, 10)).approved());
    }

    @Test
    void rejectsMoreThanTheMaximumOpenOrders() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 2, 5_000));

        assertTrue(engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 1, 10_000)).approved());
        assertTrue(engine.reserve(ACCOUNT, limit(2, "AAPL", Side.BUY, 1, 10_000)).approved());
        assertRejected(
                engine.reserve(ACCOUNT, limit(3, "AAPL", Side.BUY, 1, 10_000)),
                RiskRejectionReason.MAX_OPEN_ORDERS_EXCEEDED);

        engine.release(ACCOUNT, 1);
        assertTrue(engine.reserve(ACCOUNT, limit(3, "AAPL", Side.BUY, 1, 10_000)).approved());
    }

    @Test
    void positionLimitCountsWorkingOrdersNotJustFilledQuantity() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 100));

        assertTrue(engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 60, 10_000)).approved());
        assertRejected(
                engine.reserve(ACCOUNT, limit(2, "AAPL", Side.BUY, 41, 10_000)),
                RiskRejectionReason.POSITION_LIMIT_EXCEEDED);
        assertTrue(engine.reserve(ACCOUNT, limit(2, "AAPL", Side.BUY, 40, 10_000)).approved());
    }

    @Test
    void workingBuysAndSellsDoNotOffsetEachOtherInTheLimitCheck() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 100));

        assertTrue(engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 100, 10_000)).approved());
        // A working sell of the same size nets to zero but the buy could still fill alone.
        assertTrue(engine.reserve(ACCOUNT, limit(2, "AAPL", Side.SELL, 100, 10_100)).approved());
        assertRejected(
                engine.reserve(ACCOUNT, limit(3, "AAPL", Side.BUY, 1, 10_000)),
                RiskRejectionReason.POSITION_LIMIT_EXCEEDED);
    }

    @Test
    void limitsApplyPerAccount() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 1, 100));

        assertTrue(engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 100, 10_000)).approved());
        assertTrue(engine.reserve("ACC-2", limit(2, "AAPL", Side.BUY, 100, 10_000)).approved());
    }

    @Test
    void fillsMovePositionAndKeepTheRemainderReserved() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 5_000));
        engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 100, 10_000));

        engine.recordFill(ACCOUNT, 1, "AAPL", Side.BUY, 30);
        engine.recordRemainingQuantity(ACCOUNT, 1, 70);

        AccountRiskSnapshot.SymbolExposure exposure = onlyExposure(engine);
        assertEquals(30, exposure.netPosition());
        assertEquals(70, exposure.workingBuyQuantity());
        assertEquals(100, exposure.longExposure());
        assertEquals(1, engine.snapshot(ACCOUNT).openOrders());
    }

    @Test
    void fullyFilledOrdersStopBeingOpenOrders() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 5_000));
        engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 100, 10_000));

        engine.recordFill(ACCOUNT, 2, "AAPL", Side.BUY, 100);
        engine.recordRemainingQuantity(ACCOUNT, 1, 0);

        AccountRiskSnapshot snapshot = engine.snapshot(ACCOUNT);
        assertEquals(0, snapshot.openOrders());
        assertEquals(100, onlyExposure(engine).netPosition());
        assertEquals(0, onlyExposure(engine).workingBuyQuantity());
    }

    @Test
    void repeatingTheSameSettlementDoesNotDoubleCount() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 5_000));
        engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 100, 10_000));

        engine.recordRemainingQuantity(ACCOUNT, 1, 70);
        engine.recordRemainingQuantity(ACCOUNT, 1, 70);
        engine.recordRemainingQuantity(ACCOUNT, 1, 70);

        assertEquals(70, onlyExposure(engine).workingBuyQuantity());
    }

    @Test
    void replacingAWorkingOrderResizesItsReservationInsteadOfAddingOne() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 100));
        engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 60, 10_000));

        assertTrue(engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 90, 10_000)).approved());

        AccountRiskSnapshot snapshot = engine.snapshot(ACCOUNT);
        assertEquals(1, snapshot.openOrders());
        assertEquals(90, snapshot.exposures().getFirst().workingBuyQuantity());
    }

    @Test
    void aRejectedReplacementLeavesTheOriginalReservationIntact() {
        PreTradeRiskEngine engine = engineWith(new RiskLimits(1_000, 100_000_000, 10, 100));
        engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 60, 10_000));

        assertRejected(
                engine.reserve(ACCOUNT, limit(1, "AAPL", Side.BUY, 101, 10_000)),
                RiskRejectionReason.POSITION_LIMIT_EXCEEDED);

        assertEquals(60, onlyExposure(engine).workingBuyQuantity());
    }

    @Test
    void concurrentSubmissionsCannotExceedThePositionLimit() throws Exception {
        int limitQuantity = 500;
        int attempts = 200;
        PreTradeRiskEngine engine =
                engineWith(new RiskLimits(1_000, 100_000_000, attempts, limitQuantity));
        AtomicInteger approvals = new AtomicInteger();

        try (ExecutorService threads = Executors.newFixedThreadPool(8)) {
            CountDownLatch start = new CountDownLatch(1);
            for (int index = 0; index < attempts; index++) {
                long orderId = index + 1L;
                threads.submit(() -> {
                    start.await();
                    if (engine.reserve(ACCOUNT, limit(orderId, "AAPL", Side.BUY, 10, 10_000))
                            .approved()) {
                        approvals.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
            threads.shutdown();
            assertTrue(threads.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertEquals(limitQuantity / 10, approvals.get());
        assertEquals(limitQuantity, onlyExposure(engine).workingBuyQuantity());
    }

    private PreTradeRiskEngine engineWith(RiskLimits limits) {
        return new PreTradeRiskEngine(instruments, limits);
    }

    private static AccountRiskSnapshot.SymbolExposure onlyExposure(PreTradeRiskEngine engine) {
        List<AccountRiskSnapshot.SymbolExposure> exposures = engine.snapshot(ACCOUNT).exposures();
        assertEquals(1, exposures.size());
        return exposures.getFirst();
    }

    private static void assertRejected(RiskDecision decision, RiskRejectionReason reason) {
        assertFalse(decision.approved(), () -> "expected rejection but got " + decision);
        assertEquals(reason, decision.reason().orElseThrow());
    }

    private static RiskCheckRequest limit(
            long orderId, String symbol, Side side, long quantity, long price) {
        return new RiskCheckRequest(orderId, symbol, side, OrderType.LIMIT, quantity, price);
    }

    private static RiskCheckRequest market(
            long orderId, String symbol, Side side, long quantity) {
        return new RiskCheckRequest(orderId, symbol, side, OrderType.MARKET, quantity, 0);
    }
}
