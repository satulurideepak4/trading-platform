package com.tradingplatform.gateway.ingress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderContent;
import com.tradingplatform.gateway.metrics.GatewayMetrics;
import com.tradingplatform.matching.routing.OrderRouter;
import com.tradingplatform.matching.routing.OrderRoutingRejectedException;
import com.tradingplatform.matching.routing.RoutingRejectionReason;
import com.tradingplatform.pipeline.publish.NoOpTradingEventPublisher;
import com.tradingplatform.risk.AccountRiskSnapshot;
import com.tradingplatform.risk.InstrumentRegistry;
import com.tradingplatform.risk.PreTradeRiskEngine;
import com.tradingplatform.risk.RiskLimits;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the parts of ingress that are hard to provoke over HTTP: what happens to risk state and to
 * the idempotency registration when routing refuses a command or never answers.
 */
class OrderIngressServiceTest {
    private static final String MAKER = "ACC-MAKER";
    private static final String TAKER = "ACC-TAKER";

    private final InstrumentRegistry instruments = new InstrumentRegistry(
            List.of(new InstrumentRegistry.Instrument("AAPL", 10_000)));
    private final PreTradeRiskEngine riskEngine = new PreTradeRiskEngine(
            instruments, new RiskLimits(10_000, 1_000_000_000, 100, 100_000));
    private final OrderRegistry registry = new OrderRegistry();
    private OrderRouter router;

    @AfterEach
    void closeRouter() {
        if (router != null) {
            router.close();
        }
    }

    @Test
    void aFillMovesBothSidesOfTheTradeEvenThoughOnlyTheTakerIsWaiting() {
        OrderIngressService ingress = serviceWithRealRouter();

        ingress.submit(MAKER, "m-1", limit("AAPL", Side.SELL, 10, 10_000));
        OrderOutcome takerOutcome = ingress.submit(TAKER, "t-1", limit("AAPL", Side.BUY, 4, 10_000));

        assertTrue(takerOutcome.accepted());
        assertEquals(1, takerOutcome.executions().size());

        AccountRiskSnapshot.SymbolExposure makerExposure = onlyExposure(MAKER);
        assertEquals(-4, makerExposure.netPosition());
        assertEquals(6, makerExposure.workingSellQuantity());
        assertEquals(1, riskEngine.snapshot(MAKER).openOrders());

        AccountRiskSnapshot.SymbolExposure takerExposure = onlyExposure(TAKER);
        assertEquals(4, takerExposure.netPosition());
        assertEquals(0, takerExposure.workingBuyQuantity());
        assertEquals(0, riskEngine.snapshot(TAKER).openOrders());
    }

    @Test
    void aTradeUpdatesTheInstrumentReferencePrice() {
        OrderIngressService ingress = serviceWithRealRouter();

        ingress.submit(MAKER, "m-1", limit("AAPL", Side.SELL, 10, 12_345));
        ingress.submit(TAKER, "t-1", limit("AAPL", Side.BUY, 1, 12_345));

        assertEquals(12_345, instruments.referencePrice("AAPL"));
    }

    @Test
    void cancellingReleasesTheReservationSoTheAccountCanTradeAgain() {
        OrderIngressService ingress = serviceWithRealRouter();

        OrderOutcome submitted = ingress.submit(MAKER, "m-1", limit("AAPL", Side.BUY, 100, 10_000));
        assertEquals(100, onlyExposure(MAKER).workingBuyQuantity());

        OrderOutcome cancelled = ingress.cancel(MAKER, submitted.orderId());

        assertEquals(OrderStatus.CANCELLED, cancelled.order().orElseThrow().status());
        assertEquals(0, riskEngine.snapshot(MAKER).openOrders());
        assertEquals(0, onlyExposure(MAKER).workingBuyQuantity());
    }

    @Test
    void aSaturatedQueueReleasesTheReservationAndFreesTheClientOrderId() {
        OrderRouter saturated = mock(OrderRouter.class);
        when(saturated.submit(any(SubmitOrder.class)))
                .thenThrow(new OrderRoutingRejectedException(
                        RoutingRejectionReason.QUEUE_FULL, 0, "matching queue is full"))
                .thenReturn(CompletableFuture.completedFuture(null));
        OrderIngressService ingress = serviceWith(saturated);

        OrderRoutingRejectedException rejection = assertThrows(
                OrderRoutingRejectedException.class,
                () -> ingress.submit(MAKER, "m-1", limit("AAPL", Side.BUY, 50, 10_000)));

        assertEquals(RoutingRejectionReason.QUEUE_FULL, rejection.reason());
        assertEquals(0, riskEngine.snapshot(MAKER).openOrders());
        assertTrue(riskEngine.snapshot(MAKER).exposures().isEmpty()
                || onlyExposure(MAKER).workingBuyQuantity() == 0);
        // The clientOrderId was never consumed, so the retry is a fresh submission rather than a
        // replay of a failure.
        assertEquals(0, registry.registeredClientOrderIds());
    }

    @Test
    void aTimedOutSubmissionKeepsItsRegistrationSoARetryCannotCreateASecondOrder() {
        OrderRouter silent = mock(OrderRouter.class);
        when(silent.submit(any(SubmitOrder.class))).thenReturn(new CompletableFuture<>());
        OrderIngressService ingress = serviceWith(silent, Duration.ofMillis(50));

        MatchingTimeoutException first = assertThrows(
                MatchingTimeoutException.class,
                () -> ingress.submit(MAKER, "m-1", limit("AAPL", Side.BUY, 5, 10_000)));
        MatchingTimeoutException retry = assertThrows(
                MatchingTimeoutException.class,
                () -> ingress.submit(MAKER, "m-1", limit("AAPL", Side.BUY, 5, 10_000)));

        assertEquals(first.orderId(), retry.orderId());
        assertEquals(1, registry.registeredClientOrderIds());
    }

    @Test
    void anotherAccountCannotCancelOrReadAnOrder() {
        OrderIngressService ingress = serviceWithRealRouter();
        OrderOutcome submitted = ingress.submit(MAKER, "m-1", limit("AAPL", Side.BUY, 1, 10_000));

        assertThrows(
                OrderNotFoundException.class, () -> ingress.cancel(TAKER, submitted.orderId()));
        assertThrows(
                OrderNotFoundException.class, () -> ingress.findOrder(TAKER, submitted.orderId()));
        assertFalse(ingress.findOrder(MAKER, submitted.orderId()).duplicate());
    }

    private OrderIngressService serviceWithRealRouter() {
        router = new OrderRouter(2, 128);
        return serviceWith(router);
    }

    private OrderIngressService serviceWith(OrderRouter orderRouter) {
        return serviceWith(orderRouter, Duration.ofSeconds(5));
    }

    private OrderIngressService serviceWith(OrderRouter orderRouter, Duration timeout) {
        return new OrderIngressService(
                orderRouter,
                riskEngine,
                registry,
                new NoOpTradingEventPublisher(),
                timeout,
                Clock.systemUTC(),
                new GatewayMetrics(new SimpleMeterRegistry()));
    }

    private AccountRiskSnapshot.SymbolExposure onlyExposure(String accountId) {
        List<AccountRiskSnapshot.SymbolExposure> exposures =
                riskEngine.snapshot(accountId).exposures();
        assertEquals(1, exposures.size());
        return exposures.getFirst();
    }

    private static OrderContent limit(String symbol, Side side, long quantity, long price) {
        return new OrderContent(symbol, side, OrderType.LIMIT, quantity, price, "DEFAULT");
    }
}
