package com.tradingplatform.gateway.ingress;

import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderIdentity;
import com.tradingplatform.pipeline.events.EventIds;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.OrderAccepted;
import com.tradingplatform.pipeline.events.OrderCancelled;
import com.tradingplatform.pipeline.events.OrderFilled;
import com.tradingplatform.pipeline.events.OrderRejected;
import com.tradingplatform.pipeline.events.OrderReplaced;
import com.tradingplatform.pipeline.publish.TradingEventPublisher;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns matching outcomes into the events the downstream pipeline consumes.
 *
 * <p>Lives at the gateway rather than in the matching engine because an event needs the account and
 * the client's own order id, and the engine deliberately knows neither. The engine's job ends at
 * "this order matched that order".
 *
 * <p>Fill notifications are derived from the executions, never from the aggressing order's final
 * snapshot. An execution names both sides, so working from it covers the resting order's owner too
 * — an account that is not waiting on any request and would otherwise never be told it had traded.
 */
final class TradingEventEmitter {
    private static final Logger LOG = LoggerFactory.getLogger(TradingEventEmitter.class);

    private final TradingEventPublisher publisher;
    private final OrderRegistry registry;

    TradingEventEmitter(TradingEventPublisher publisher, OrderRegistry registry) {
        this.publisher = publisher;
        this.registry = registry;
    }

    void emitAccepted(
            String correlationId,
            String accountId,
            String strategyId,
            String clientOrderId,
            OrderSnapshot order) {
        publisher.publish(new OrderAccepted(
                EventIds.accepted(order.orderId()),
                correlationId,
                order.orderId(),
                clientOrderId,
                accountId,
                strategyId,
                order.symbol(),
                order.side(),
                order.type(),
                order.status(),
                order.quantity(),
                order.remainingQuantity(),
                order.price(),
                order.updatedAt()));

        // A market order whose remainder could not be filled comes back already cancelled. Emitting
        // the terminal transition as well means a lifecycle consumer sees the same shape of ending
        // whether the order was killed on arrival or cancelled later by its owner.
        if (order.status() == OrderStatus.CANCELLED) {
            emitCancelled(correlationId, accountId, strategyId, clientOrderId, order);
        }
    }

    void emitCancelled(
            String correlationId,
            String accountId,
            String strategyId,
            String clientOrderId,
            OrderSnapshot order) {
        publisher.publish(new OrderCancelled(
                EventIds.cancelled(order.orderId()),
                correlationId,
                order.orderId(),
                clientOrderId,
                accountId,
                strategyId,
                order.symbol(),
                order.remainingQuantity(),
                order.updatedAt()));
    }

    void emitReplaced(
            String correlationId,
            String accountId,
            String strategyId,
            String clientOrderId,
            OrderSnapshot order) {
        publisher.publish(new OrderReplaced(
                EventIds.replaced(order.orderId(), order.prioritySequence()),
                correlationId,
                order.orderId(),
                clientOrderId,
                accountId,
                strategyId,
                order.symbol(),
                order.quantity(),
                order.price(),
                order.remainingQuantity(),
                order.prioritySequence(),
                order.updatedAt()));
    }

    void emitRejected(
            String correlationId,
            String accountId,
            String strategyId,
            long orderId,
            String clientOrderId,
            String symbol,
            OrderOutcome.Rejection rejection,
            Instant occurredAt) {
        publisher.publish(new OrderRejected(
                EventIds.rejected(orderId),
                correlationId,
                orderId,
                clientOrderId,
                accountId,
                strategyId,
                symbol,
                rejection.source().name(),
                rejection.reason(),
                rejection.detail(),
                occurredAt));
    }

    void emitExecutions(String correlationId, List<Execution> executions) {
        for (Execution execution : executions) {
            OrderIdentity buyer = registry.identify(execution.buyOrderId());
            OrderIdentity seller = registry.identify(execution.sellOrderId());
            if (buyer == null || seller == null) {
                // Every order on this venue was issued by this gateway, so an unknown side means
                // the registry and the engine have diverged. Publishing a trade with an unknown
                // owner would corrupt positions, so it is refused loudly instead.
                LOG.error(
                        "event=execution_with_unknown_owner executionId={} buyOrderId={} "
                                + "sellOrderId={}",
                        execution.executionId(),
                        execution.buyOrderId(),
                        execution.sellOrderId());
                continue;
            }

            publisher.publish(new ExecutionCreated(
                    EventIds.execution(execution.executionId()),
                    correlationId,
                    execution.executionId(),
                    execution.symbol(),
                    execution.price(),
                    execution.quantity(),
                    execution.buyOrderId(),
                    buyer.accountId(),
                    buyer.strategyId(),
                    execution.sellOrderId(),
                    seller.accountId(),
                    seller.strategyId(),
                    execution.makerOrderId(),
                    execution.takerOrderId(),
                    execution.buyRemainingQuantity(),
                    execution.sellRemainingQuantity(),
                    execution.timestamp()));

            emitFillIfComplete(correlationId, buyer, Side.BUY, execution, execution.buyRemainingQuantity());
            emitFillIfComplete(correlationId, seller, Side.SELL, execution, execution.sellRemainingQuantity());
        }
    }

    private void emitFillIfComplete(
            String correlationId,
            OrderIdentity order,
            Side side,
            Execution execution,
            long remainingQuantity) {
        if (remainingQuantity > 0) {
            return;
        }
        publisher.publish(new OrderFilled(
                EventIds.filled(order.orderId()),
                correlationId,
                order.orderId(),
                order.clientOrderId(),
                order.accountId(),
                order.strategyId(),
                execution.symbol(),
                side,
                execution.executionId(),
                execution.timestamp()));
    }
}
