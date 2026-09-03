package com.tradingplatform.gateway.ingress;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.gateway.CorrelationId;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderContent;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderIdentity;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderReservation;
import com.tradingplatform.gateway.metrics.GatewayMetrics;
import com.tradingplatform.matching.OrderBookSnapshot;
import com.tradingplatform.matching.routing.OrderRouter;
import com.tradingplatform.matching.routing.RoutedResult;
import com.tradingplatform.pipeline.publish.TradingEventPublisher;
import com.tradingplatform.risk.InstrumentRegistry;
import com.tradingplatform.risk.PreTradeRiskEngine;
import com.tradingplatform.risk.RiskCheckRequest;
import com.tradingplatform.risk.RiskDecision;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The order-ingress path: identity, idempotency, pre-trade risk, routing and settlement of risk
 * state against the matching outcome.
 *
 * <p>Ordering is deliberate. Identity is established first so that a duplicate request never runs
 * risk twice. Risk runs before routing so that a rejected order never reaches a matching worker and
 * never consumes queue capacity. Settlement runs last, from the outcome the engine actually
 * produced, because the pre-trade reservation is only an estimate of the worst case.
 *
 * <h2>Threading</h2>
 *
 * A submission's outcome is settled by whichever thread observes the router's completion — the
 * matching worker. That keeps the outcome tied to the command rather than to the HTTP request, so a
 * client that times out and retries still sees the real result. The cost is a small amount of
 * non-matching work on the worker thread. Cancel and replace have no idempotency registration to
 * publish to, so they settle on the request thread.
 *
 * <p>Settlement also hands the resulting events to the publisher, which buffers them and returns
 * immediately. Risk state is still updated here rather than from the event stream, because an
 * admission decision must see the fill that just happened rather than the one that will arrive over
 * a broker; the execution consumer applies the same fills idempotently so the two agree, and so the
 * positions can be rebuilt by replay after a restart.
 */
public final class OrderIngressService {
    private static final Logger LOG = LoggerFactory.getLogger(OrderIngressService.class);

    private final OrderRouter router;
    private final PreTradeRiskEngine riskEngine;
    private final InstrumentRegistry instruments;
    private final OrderRegistry registry;
    private final TradingEventEmitter events;
    private final Duration commandTimeout;
    private final Clock clock;
    private final GatewayMetrics metrics;

    public OrderIngressService(
            OrderRouter router,
            PreTradeRiskEngine riskEngine,
            OrderRegistry registry,
            TradingEventPublisher publisher,
            Duration commandTimeout,
            Clock clock,
            GatewayMetrics metrics) {
        this.router = router;
        this.riskEngine = riskEngine;
        this.instruments = riskEngine.instruments();
        this.registry = registry;
        this.events = new TradingEventEmitter(publisher, registry);
        this.commandTimeout = commandTimeout;
        this.clock = clock;
        this.metrics = metrics;
    }

    public OrderOutcome submit(String accountId, String clientOrderId, OrderContent content) {
        OrderReservation reservation = registry.reserve(accountId, clientOrderId, content);
        if (reservation.fresh()) {
            beginSubmission(accountId, clientOrderId, reservation.orderId(), content);
        } else {
            LOG.info(
                    "event=duplicate_submission account={} clientOrderId={} orderId={}",
                    accountId,
                    clientOrderId,
                    reservation.orderId());
        }
        return awaitSubmission(reservation);
    }

    public OrderOutcome cancel(String accountId, long orderId) {
        OrderIdentity identity = registry.requireOwned(accountId, orderId);
        CompletableFuture<RoutedResult<CommandResult>> routed =
                router.cancel(new CancelOrder(orderId, clock.instant()));
        return awaitAndSettle("cancel", accountId, identity, orderId, routed, Operation.CANCEL);
    }

    public OrderOutcome replace(String accountId, long orderId, long quantity, long price) {
        OrderIdentity identity = registry.requireOwned(accountId, orderId);

        // A replacement raises exposure before the engine has confirmed it, so it is risk-checked
        // like a new order. Reserving under the same order id resizes the existing reservation
        // instead of adding a second one.
        RiskDecision decision = riskEngine.reserve(
                accountId,
                new RiskCheckRequest(
                        orderId,
                        identity.symbol(),
                        identity.side(),
                        OrderType.LIMIT,
                        quantity,
                        price));
        if (!decision.approved()) {
            String reason = decision.reason().orElseThrow().name();
            metrics.recordRiskRejection("replace", reason);
            LOG.info(
                    "event=order_rejected stage=risk operation=replace account={} orderId={} "
                            + "reason={}",
                    accountId,
                    orderId,
                    reason);
            OrderOutcome outcome = OrderOutcome.rejected(
                    orderId,
                    identity.clientOrderId(),
                    RejectionSource.RISK,
                    reason,
                    decision.detail());
            events.emitRejected(
                    CorrelationId.current(),
                    accountId,
                    identity.strategyId(),
                    orderId,
                    identity.clientOrderId(),
                    identity.symbol(),
                    outcome.rejection().orElseThrow(),
                    clock.instant());
            return outcome;
        }

        CompletableFuture<RoutedResult<CommandResult>> routed =
                router.replace(new ReplaceOrder(orderId, quantity, price, clock.instant()));
        return awaitAndSettle("replace", accountId, identity, orderId, routed, Operation.REPLACE);
    }

    public OrderOutcome findOrder(String accountId, long orderId) {
        OrderIdentity identity = registry.requireOwned(accountId, orderId);
        OrderSnapshot snapshot = awaitRouted("find", orderId, router.findOrder(orderId))
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        return OrderOutcome.accepted(identity.clientOrderId(), snapshot, List.of());
    }

    public OrderBookSnapshot book(String symbol) {
        if (!instruments.isTradable(symbol)) {
            throw new InstrumentNotFoundException(symbol);
        }
        return awaitRouted("book", 0, router.book(symbol));
    }

    private void beginSubmission(
            String accountId, String clientOrderId, long orderId, OrderContent content) {
        RiskDecision decision = riskEngine.reserve(
                accountId,
                new RiskCheckRequest(
                        orderId,
                        content.symbol(),
                        content.side(),
                        content.type(),
                        content.quantity(),
                        content.price()));
        if (!decision.approved()) {
            String reason = decision.reason().orElseThrow().name();
            metrics.recordRiskRejection("submit", reason);
            LOG.info(
                    "event=order_rejected stage=risk operation=submit account={} orderId={} "
                            + "clientOrderId={} symbol={} reason={}",
                    accountId,
                    orderId,
                    clientOrderId,
                    content.symbol(),
                    reason);
            OrderOutcome outcome = OrderOutcome.rejected(
                    orderId, clientOrderId, RejectionSource.RISK, reason, decision.detail());
            // Same reasoning as applySubmissionOutcome's try/catch: a bug in emitRejected here must
            // still complete the caller's future one way or the other, or the caller blocks for the
            // full commandTimeout on a rejection everyone already agreed on.
            try {
                events.emitRejected(
                        CorrelationId.current(),
                        accountId,
                        content.strategyId(),
                        orderId,
                        clientOrderId,
                        content.symbol(),
                        outcome.rejection().orElseThrow(),
                        clock.instant());
                registry.complete(accountId, clientOrderId, outcome);
            } catch (RuntimeException emitFailure) {
                LOG.error(
                        "event=risk_rejection_emit_failed account={} orderId={} clientOrderId={}",
                        accountId,
                        orderId,
                        clientOrderId,
                        emitFailure);
                registry.fail(accountId, clientOrderId, emitFailure);
            }
            return;
        }

        SubmitOrder command = new SubmitOrder(
                orderId,
                engineClientOrderId(accountId, clientOrderId),
                content.symbol(),
                content.side(),
                content.type(),
                content.quantity(),
                content.price(),
                clock.instant());

        CompletableFuture<RoutedResult<CommandResult>> routed;
        try {
            routed = router.submit(command);
        } catch (RuntimeException notAdmitted) {
            // The command never entered a queue, so both the reservation and the clientOrderId can
            // be given back and the client is free to retry the same identifier.
            riskEngine.release(accountId, orderId);
            registry.withdraw(accountId, clientOrderId, notAdmitted);
            throw notAdmitted;
        }
        // Settlement runs on the worker thread, which has no MDC of its own. Carrying the id across
        // is what keeps a single correlation id covering the whole path from request to execution.
        String correlationId = CorrelationId.current();
        routed.whenComplete((result, failure) -> settleSubmission(
                correlationId, accountId, content.strategyId(), clientOrderId, orderId, result, failure));
    }

    private void settleSubmission(
            String correlationId,
            String accountId,
            String strategyId,
            String clientOrderId,
            long orderId,
            RoutedResult<CommandResult> routed,
            Throwable failure) {
        try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
            applySubmissionOutcome(accountId, strategyId, clientOrderId, orderId, routed, failure);
        }
    }

    private void applySubmissionOutcome(
            String accountId,
            String strategyId,
            String clientOrderId,
            long orderId,
            RoutedResult<CommandResult> routed,
            Throwable failure) {
        try {
            if (failure != null) {
                riskEngine.release(accountId, orderId);
                registry.withdraw(accountId, clientOrderId, failure);
                return;
            }
            logRouted("submit", orderId, routed);
            metrics.recordRouted("submit", routed);
            registry.complete(
                    accountId, clientOrderId, settle(accountId, strategyId, orderId, clientOrderId, routed.value(), Operation.SUBMIT));
        } catch (RuntimeException settlementFailure) {
            // A bug here would otherwise leave the caller blocked on a future nobody completes. The
            // registration is kept because the command did reach the engine.
            LOG.error(
                    "event=settlement_failed account={} orderId={} clientOrderId={}",
                    accountId,
                    orderId,
                    clientOrderId,
                    settlementFailure);
            registry.fail(accountId, clientOrderId, settlementFailure);
        }
    }

    /**
     * Cancel and replace's counterpart to {@link #beginSubmission}/{@link #settleSubmission}: the
     * router future is settled from whichever thread observes its completion, not from this calling
     * thread, so a client-side timeout on the {@code get()} below never leaves the risk reservation
     * unreconciled or the resulting lifecycle event (CANCELLED/REPLACED) unpublished. Settlement does
     * not depend on this thread still being there to run it when the router's future resolves.
     */
    private OrderOutcome awaitAndSettle(
            String operation,
            String accountId,
            OrderIdentity identity,
            long orderId,
            CompletableFuture<RoutedResult<CommandResult>> routed,
            Operation operationKind) {
        String correlationId = CorrelationId.current();
        CompletableFuture<OrderOutcome> outcomeFuture = new CompletableFuture<>();
        routed.whenComplete((result, failure) -> settleRoutedCommand(
                correlationId, operation, accountId, identity, orderId, result, failure,
                operationKind, outcomeFuture));
        try {
            return outcomeFuture.get(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            throw new MatchingTimeoutException(orderId);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new MatchingTimeoutException(orderId);
        } catch (ExecutionException failed) {
            throw asRuntimeException(failed.getCause(), operation);
        }
    }

    private void settleRoutedCommand(
            String correlationId,
            String operation,
            String accountId,
            OrderIdentity identity,
            long orderId,
            RoutedResult<CommandResult> routed,
            Throwable failure,
            Operation operationKind,
            CompletableFuture<OrderOutcome> outcomeFuture) {
        try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
            if (failure != null) {
                // The router itself failed rather than the engine producing a CommandResult (queue
                // saturation after admission, a worker fault) - there is no result to settle against,
                // and nothing here changed risk state, so nothing needs to be released.
                outcomeFuture.completeExceptionally(failure);
                return;
            }
            logRouted(operation, orderId, routed);
            metrics.recordRouted(operation, routed);
            outcomeFuture.complete(settle(
                    accountId, identity.strategyId(), orderId, identity.clientOrderId(),
                    routed.value(), operationKind));
        } catch (RuntimeException settlementFailure) {
            LOG.error(
                    "event=settlement_failed operation={} account={} orderId={}",
                    operation,
                    accountId,
                    orderId,
                    settlementFailure);
            outcomeFuture.completeExceptionally(settlementFailure);
        }
    }

    private OrderOutcome settle(
            String accountId,
            String strategyId,
            long orderId,
            String clientOrderId,
            CommandResult result,
            Operation operation) {
        applyExecutions(result.executions());
        String correlationId = CorrelationId.current();
        String symbol = result.order().map(OrderSnapshot::symbol).orElse("UNKNOWN");
        String operationTag = operation.name().toLowerCase(Locale.ROOT);
        metrics.recordOutcome(operationTag, symbol, result);

        if (!result.accepted()) {
            settleReservation(accountId, orderId, result.order().orElse(null));
            String reason = result.rejectionReason().orElseThrow().name();
            LOG.info(
                    "event=order_rejected stage=matching account={} orderId={} clientOrderId={} "
                            + "reason={}",
                    accountId,
                    orderId,
                    clientOrderId,
                    reason);
            OrderOutcome outcome = OrderOutcome.rejected(
                    orderId,
                    clientOrderId,
                    RejectionSource.MATCHING_ENGINE,
                    reason,
                    "the matching engine refused the command");
            events.emitRejected(
                    correlationId,
                    accountId,
                    strategyId,
                    orderId,
                    clientOrderId,
                    symbol,
                    outcome.rejection().orElseThrow(),
                    clock.instant());
            return outcome;
        }

        OrderSnapshot order = result.order().orElseThrow();
        settleReservation(accountId, orderId, order);

        // The order's own lifecycle event goes first. Emitting the executions first would put the
        // OrderFilled they produce ahead of the OrderAccepted for the same order on the orders
        // topic, so a lifecycle consumer would see an order finish before it started. Both are
        // keyed by symbol and land on one partition, so the publish order is the observed order.
        switch (operation) {
            case SUBMIT -> events.emitAccepted(
                    correlationId, accountId, strategyId, clientOrderId, order);
            case CANCEL -> events.emitCancelled(
                    correlationId, accountId, strategyId, clientOrderId, order);
            case REPLACE -> events.emitReplaced(
                    correlationId, accountId, strategyId, clientOrderId, order);
        }
        events.emitExecutions(correlationId, result.executions());

        LOG.info(
                "event=order_accepted operation={} account={} orderId={} clientOrderId={} symbol={} "
                        + "status={} remainingQuantity={} executions={}",
                operation,
                accountId,
                orderId,
                clientOrderId,
                order.symbol(),
                order.status(),
                order.remainingQuantity(),
                result.executions().size());
        return OrderOutcome.accepted(clientOrderId, order, result.executions());
    }

    /** Which command produced an outcome, so the right lifecycle event is published for it. */
    private enum Operation {
        SUBMIT,
        CANCEL,
        REPLACE
    }

    /**
     * Applies fills to both sides. The resting side's owner is not the account that sent the
     * aggressing command, and it is not waiting on any request, so its position and its remaining
     * reservation can only be updated from here.
     */
    private void applyExecutions(List<Execution> executions) {
        for (Execution execution : executions) {
            instruments.recordTradedPrice(execution.symbol(), execution.price());
            applyFill(
                    execution.executionId(),
                    execution.buyOrderId(),
                    execution.symbol(),
                    Side.BUY,
                    execution.quantity(),
                    execution.buyRemainingQuantity());
            applyFill(
                    execution.executionId(),
                    execution.sellOrderId(),
                    execution.symbol(),
                    Side.SELL,
                    execution.quantity(),
                    execution.sellRemainingQuantity());
        }
    }

    private void applyFill(
            long executionId,
            long orderId,
            String symbol,
            Side side,
            long quantity,
            long remainingQuantity) {
        OrderIdentity identity = registry.identify(orderId);
        if (identity == null) {
            LOG.warn("event=fill_for_unknown_order orderId={} symbol={}", orderId, symbol);
            return;
        }
        // Keyed on the execution id so that this call and the execution consumer's replay of the
        // same fill converge on one position rather than adding up to two.
        riskEngine.recordFill(identity.accountId(), executionId, symbol, side, quantity);
        riskEngine.recordRemainingQuantity(identity.accountId(), orderId, remainingQuantity);
    }

    /** Replaces the pre-trade estimate with what the engine says is actually still working. */
    private void settleReservation(String accountId, long orderId, OrderSnapshot order) {
        if (order == null) {
            riskEngine.release(accountId, orderId);
            return;
        }
        boolean stillWorking = order.status() == OrderStatus.NEW
                || order.status() == OrderStatus.PARTIALLY_FILLED;
        riskEngine.recordRemainingQuantity(
                accountId, orderId, stillWorking ? order.remainingQuantity() : 0);
    }

    private OrderOutcome awaitSubmission(OrderReservation reservation) {
        try {
            OrderOutcome outcome =
                    reservation.outcome().get(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return reservation.fresh() ? outcome : outcome.asDuplicate();
        } catch (TimeoutException timeout) {
            throw new MatchingTimeoutException(reservation.orderId());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new MatchingTimeoutException(reservation.orderId());
        } catch (ExecutionException failed) {
            throw asRuntimeException(failed.getCause(), "order submission");
        }
    }

    private <T> T awaitRouted(
            String operation, long orderId, CompletableFuture<RoutedResult<T>> future) {
        try {
            RoutedResult<T> routed = future.get(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
            logRouted(operation, orderId, routed);
            metrics.recordRouted(operation, routed);
            return routed.value();
        } catch (TimeoutException timeout) {
            throw new MatchingTimeoutException(orderId);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new MatchingTimeoutException(orderId);
        } catch (ExecutionException failed) {
            throw asRuntimeException(failed.getCause(), operation);
        }
    }

    private static RuntimeException asRuntimeException(Throwable cause, String operation) {
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        return new IllegalStateException("matching worker failed to complete " + operation, cause);
    }

    private static void logRouted(String operation, long orderId, RoutedResult<?> routed) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "event=command_routed operation={} orderId={} workerId={} routingSequence={} "
                            + "processingSequence={} queueWaitMicros={} processingMicros={}",
                    operation,
                    orderId,
                    routed.workerId(),
                    routed.routingSequence(),
                    routed.processingSequence(),
                    routed.queueWaitNanos() / 1_000,
                    routed.processingNanos() / 1_000);
        }
    }

    /**
     * The engine treats clientOrderId as a platform-wide identifier, while clients only guarantee
     * uniqueness within their own account. Namespacing by account reconciles the two without making
     * one client's identifier choices visible to another.
     */
    private static String engineClientOrderId(String accountId, String clientOrderId) {
        return accountId + ":" + clientOrderId;
    }
}
