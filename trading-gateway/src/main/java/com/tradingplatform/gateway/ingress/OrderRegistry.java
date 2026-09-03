package com.tradingplatform.gateway.ingress;

import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns order identity: it allocates order ids, decides whether an incoming submission is new or a
 * repeat, and records which account each order belongs to.
 *
 * <p>The idempotency key is {@code (accountId, clientOrderId)}. Scoping it to the account is what
 * lets two unrelated clients both use "order-1" without colliding, and it means one client cannot
 * probe another's clientOrderIds.
 *
 * <p>A registration is created before any risk or routing work happens, and it carries a future
 * that is completed once — by whichever thread produces the outcome. A concurrent duplicate
 * therefore waits for the first attempt rather than starting a second one. This is the whole
 * mechanism: without a single shared registration, two threads racing on the same clientOrderId
 * would both see "not seen before" and both submit.
 */
public final class OrderRegistry {
    private final AtomicLong orderIdSequence = new AtomicLong();
    private final Map<ClientOrderKey, Registration> registrations = new ConcurrentHashMap<>();
    private final Map<Long, OrderIdentity> identities = new ConcurrentHashMap<>();

    /**
     * Returns an existing registration for a repeated request, or creates one and allocates an
     * order id.
     *
     * @throws ClientOrderIdConflictException if the clientOrderId was used for different content
     */
    public OrderReservation reserve(String accountId, String clientOrderId, OrderContent content) {
        ClientOrderKey key = new ClientOrderKey(accountId, clientOrderId);
        // The id is allocated optimistically. A losing race discards it, which leaves a gap in the
        // sequence; ids only need to be unique and increasing, not contiguous.
        long candidateOrderId = orderIdSequence.incrementAndGet();
        Registration candidate = new Registration(candidateOrderId, content);
        Registration existing = registrations.putIfAbsent(key, candidate);
        if (existing == null) {
            identities.put(
                    candidateOrderId,
                    new OrderIdentity(
                            candidateOrderId,
                            accountId,
                            clientOrderId,
                            content.symbol(),
                            content.side(),
                            content.type(),
                            content.strategyId()));
            return new OrderReservation(candidateOrderId, true, candidate.outcome);
        }
        if (!existing.content.equals(content)) {
            throw new ClientOrderIdConflictException(clientOrderId, existing.orderId);
        }
        return new OrderReservation(existing.orderId, false, existing.outcome);
    }

    /** Publishes the final outcome to this request and to any concurrent duplicate waiting on it. */
    public void complete(String accountId, String clientOrderId, OrderOutcome outcome) {
        Registration registration = registrations.get(new ClientOrderKey(accountId, clientOrderId));
        if (registration != null) {
            registration.outcome.complete(outcome);
        }
    }

    /**
     * Withdraws a registration whose command provably never reached a matching worker, so the
     * client may retry the same clientOrderId. Anything already waiting on it is failed with the
     * same cause rather than being left to time out.
     */
    public void withdraw(String accountId, String clientOrderId, Throwable cause) {
        Registration registration =
                registrations.remove(new ClientOrderKey(accountId, clientOrderId));
        if (registration != null) {
            identities.remove(registration.orderId);
            registration.outcome.completeExceptionally(cause);
        }
    }

    /**
     * Fails a registration but keeps it, for the case where the command may already have reached a
     * matching worker. The client sees an error and a retry sees the same error, which is worse for
     * that client than a clean retry but avoids the far worse outcome of quietly duplicating a live
     * order.
     */
    public void fail(String accountId, String clientOrderId, Throwable cause) {
        Registration registration = registrations.get(new ClientOrderKey(accountId, clientOrderId));
        if (registration != null) {
            registration.outcome.completeExceptionally(cause);
        }
    }

    /**
     * @throws OrderNotFoundException when the order is unknown or belongs to a different account
     */
    public OrderIdentity requireOwned(String accountId, long orderId) {
        OrderIdentity identity = identities.get(orderId);
        if (identity == null || !identity.accountId().equals(accountId)) {
            throw new OrderNotFoundException(orderId);
        }
        return identity;
    }

    /** Returns null for orders this gateway did not issue. */
    public OrderIdentity identify(long orderId) {
        return identities.get(orderId);
    }

    public int registeredClientOrderIds() {
        return registrations.size();
    }

    /**
     * The order attributes that a retry must repeat to count as the same order.
     *
     * @param strategyId the book the order is traded for. Positions and P&L are reported per
     *     account, strategy and instrument, so the attribution has to be captured at the point the
     *     order is accepted; it cannot be reconstructed from a fill afterwards.
     */
    public record OrderContent(
            String symbol,
            Side side,
            OrderType type,
            long quantity,
            long price,
            String strategyId) {}

    /**
     * The attributes of an order that cannot change once it exists. Keeping them at the gateway
     * means a cancel or replace can be authorised and risk-checked without a round trip through the
     * matching worker just to look the order up.
     */
    public record OrderIdentity(
            long orderId,
            String accountId,
            String clientOrderId,
            String symbol,
            Side side,
            OrderType type,
            String strategyId) {}

    /**
     * @param fresh true if this caller created the registration and is responsible for producing
     *     the outcome; false if it must wait for the outcome of an earlier identical request
     */
    public record OrderReservation(
            long orderId, boolean fresh, CompletableFuture<OrderOutcome> outcome) {}

    private record ClientOrderKey(String accountId, String clientOrderId) {}

    private static final class Registration {
        private final long orderId;
        private final OrderContent content;
        private final CompletableFuture<OrderOutcome> outcome = new CompletableFuture<>();

        private Registration(long orderId, OrderContent content) {
            this.orderId = orderId;
            this.content = content;
        }
    }
}
