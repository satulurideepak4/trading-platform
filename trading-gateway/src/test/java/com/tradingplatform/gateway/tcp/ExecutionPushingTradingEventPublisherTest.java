package com.tradingplatform.gateway.tcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage;
import com.tradingplatform.pipeline.events.EventIds;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.OrderAccepted;
import com.tradingplatform.pipeline.events.TradingEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExecutionPushingTradingEventPublisherTest {
    private static final Instant NOW = Instant.parse("2026-01-01T09:30:00Z");

    @Test
    void everyEventStillReachesTheRealDelegateUnchanged() {
        List<TradingEvent> forwarded = new ArrayList<>();
        ExecutionPushingTradingEventPublisher decorator = new ExecutionPushingTradingEventPublisher(
                forwarded::add, new TcpConnectionRegistry(new SimpleMeterRegistry()));
        ExecutionCreated execution = takerBuys(10, 3, 12, 0);

        decorator.publish(execution);

        assertEquals(1, forwarded.size());
        assertSame(execution, forwarded.get(0));
    }

    @Test
    void aNonExecutionEventIsForwardedButNeverPushed() {
        List<TradingEvent> forwarded = new ArrayList<>();
        TcpConnectionRegistry registry = new TcpConnectionRegistry(new SimpleMeterRegistry());
        ExecutionPushingTradingEventPublisher decorator =
                new ExecutionPushingTradingEventPublisher(forwarded::add, registry);
        TradingEvent notAnExecution = new OrderAccepted(
                EventIds.accepted(1), "corr-1", 1, "client-1", "ACC-1", "DEFAULT", "AAPL", Side.BUY,
                OrderType.LIMIT, OrderStatus.NEW, 10, 10, 19_000, NOW);

        decorator.publish(notAnExecution);

        assertEquals(1, forwarded.size());
    }

    /**
     * The buy side was the taker here (makerOrderId is the sell side's order id) - the push each
     * side receives must reflect that side's own liquidity, not a single shared flag, and neither
     * push carries the other side's order id or account.
     */
    @Test
    void buyerAndSellerPushesReflectEachSidesOwnLiquidityAndOmitTheOtherSide() {
        ExecutionCreated execution = takerBuys(10, 3, 12, 0);

        ServerMessage.ExecutionPush buyerPush = ExecutionPushingTradingEventPublisher.buyerPush(execution);
        ServerMessage.ExecutionPush sellerPush = ExecutionPushingTradingEventPublisher.sellerPush(execution);

        assertEquals(execution.buyOrderId(), buyerPush.orderId());
        assertFalse(buyerPush.maker(), "the buy side was the taker in this trade");
        assertEquals(execution.buyRemainingQuantity(), buyerPush.remainingQuantity());

        assertEquals(execution.sellOrderId(), sellerPush.orderId());
        assertTrue(sellerPush.maker(), "the sell side was already resting");
        assertEquals(execution.sellRemainingQuantity(), sellerPush.remainingQuantity());

        assertEquals(execution.executionId(), buyerPush.executionId());
        assertEquals(execution.executionId(), sellerPush.executionId());
        assertEquals(execution.symbol(), buyerPush.symbol());
        assertEquals(execution.price(), buyerPush.price());
        assertEquals(execution.quantity(), buyerPush.quantity());
    }

    private static ExecutionCreated takerBuys(
            long buyOrderId, long sellOrderId, long buyRemaining, long sellRemaining) {
        long executionId = 555;
        return new ExecutionCreated(
                EventIds.execution(executionId),
                "corr-1",
                executionId,
                "AAPL",
                19_000,
                7,
                buyOrderId,
                "ACC-BUYER",
                "DEFAULT",
                sellOrderId,
                "ACC-SELLER",
                "DEFAULT",
                sellOrderId, // makerOrderId: the resting side, i.e. the seller
                buyOrderId, // takerOrderId: the aggressor, i.e. the buyer
                buyRemaining,
                sellRemaining,
                NOW);
    }
}
