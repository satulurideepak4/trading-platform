package com.tradingplatform.gateway.tcp;

import com.tradingplatform.gateway.tcp.protocol.ServerMessage;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import com.tradingplatform.pipeline.events.TradingEvent;
import com.tradingplatform.pipeline.publish.TradingEventPublisher;

/**
 * Decorates the real {@link TradingEventPublisher} so a fill also reaches any TCP connection
 * currently open for either side of the trade — without a Kafka round trip.
 *
 * <p>{@code OrderIngressService.applyExecutions()} already has both sides' account ids in-process,
 * on the matching worker thread, at the exact moment a fill happens; it publishes an {@link
 * ExecutionCreated} through this same interface right afterward. Pushing from here, in the same
 * decorator that forwards to the real publisher, means the TCP path never depends on Kafka being
 * reachable (works identically with {@code trading.pipeline.enabled=false}) and never pays a
 * broker round trip for what should be the fastest notification path in the system — see ADR-016.
 *
 * <p>Pushed to the maker's side only. The taker's side is whichever command's own {@code
 * OrderIngressService.submit/replace} call produced this exact fill — that connection already has
 * the same information synchronously, inline in its own {@code Ack}'s executions. Pushing to it too
 * would be a redundant, and worse, a racy second delivery of the same fact: the async push and the
 * synchronous Ack are produced by two different threads with no ordering guarantee between them, so
 * a taker could see its own fill announced by an unsolicited push before the Ack it was waiting for
 * ever arrives. Only the maker's side is genuinely unsolicited — its owner submitted nothing just
 * now, so a push is the only way it learns about this fill promptly at all.
 *
 * <p>This push is best-effort by design: {@link TcpConnectionRegistry#pushToAccount} drops on a
 * full per-connection queue rather than blocking (see {@code TcpConnection}), and this method never
 * lets a failure to push stop the real event from being forwarded to {@code delegate} below, which
 * remains the system of record. A missed push costs a client an unsolicited notification, not the
 * fill itself — the fill is already durable via the untouched Kafka pipeline and always answerable
 * from {@code GET /orders/{orderId}}.
 */
public final class ExecutionPushingTradingEventPublisher implements TradingEventPublisher {
    private final TradingEventPublisher delegate;
    private final TcpConnectionRegistry registry;

    public ExecutionPushingTradingEventPublisher(
            TradingEventPublisher delegate, TcpConnectionRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
    }

    /** Package-visible purely for {@code TcpAwareTradingEventPublisherWiringTest} to confirm which
     * concrete delegate Spring resolved this decorator around. */
    TradingEventPublisher delegateForTesting() {
        return delegate;
    }

    @Override
    public void publish(TradingEvent event) {
        delegate.publish(event);
        if (event instanceof ExecutionCreated execution) {
            pushExecution(execution);
        }
    }

    private void pushExecution(ExecutionCreated execution) {
        if (execution.buyOrderId() == execution.makerOrderId()) {
            registry.pushToAccount(execution.buyAccountId(), buyerPush(execution));
        } else {
            registry.pushToAccount(execution.sellAccountId(), sellerPush(execution));
        }
    }

    /** Package-visible and pure specifically so the maker/taker-per-side logic is unit-testable
     * without a real socket or registry - see {@code ExecutionPushingTradingEventPublisherTest}. */
    static ServerMessage.ExecutionPush buyerPush(ExecutionCreated execution) {
        return new ServerMessage.ExecutionPush(
                execution.buyOrderId(),
                execution.executionId(),
                execution.symbol(),
                execution.price(),
                execution.quantity(),
                execution.buyRemainingQuantity(),
                execution.buyOrderId() == execution.makerOrderId(),
                execution.occurredAt());
    }

    static ServerMessage.ExecutionPush sellerPush(ExecutionCreated execution) {
        return new ServerMessage.ExecutionPush(
                execution.sellOrderId(),
                execution.executionId(),
                execution.symbol(),
                execution.price(),
                execution.quantity(),
                execution.sellRemainingQuantity(),
                execution.sellOrderId() == execution.makerOrderId(),
                execution.occurredAt());
    }
}
