package com.tradingplatform.risk;

import com.tradingplatform.domain.Side;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mutable risk state for one account.
 *
 * <p>Every method is synchronized on the instance. The critical section covers "check limits and
 * reserve the order" as one step, which is the property that makes the checks meaningful: two
 * concurrent submissions from the same account must not both observe the pre-submission exposure
 * and both pass. Accounts are independent, so contention is per account rather than global.
 */
final class AccountRiskState {
    private final Map<Long, WorkingOrder> workingOrders = new HashMap<>();
    private final Map<String, Exposure> exposures = new HashMap<>();

    /**
     * Checks account-level limits against the worst-case outcome and, if they hold, records the
     * order as working. Passing an {@code orderId} that is already working replaces it, which is
     * how a replacement is risk-checked without briefly dropping its existing exposure.
     */
    synchronized RiskDecision reserve(WorkingOrder candidate, RiskLimits limits) {
        WorkingOrder replaced = workingOrders.get(candidate.orderId());
        if (replaced != null
                && (!replaced.symbol().equals(candidate.symbol())
                        || replaced.side() != candidate.side())) {
            throw new IllegalArgumentException(
                    "a working order cannot change symbol or side: " + candidate.orderId());
        }

        int projectedOpenOrders = workingOrders.size() + (replaced == null ? 1 : 0);
        if (projectedOpenOrders > limits.maxOpenOrders()) {
            return RiskDecision.reject(
                    RiskRejectionReason.MAX_OPEN_ORDERS_EXCEEDED,
                    "account would hold " + projectedOpenOrders + " open orders, limit is "
                            + limits.maxOpenOrders());
        }

        Exposure exposure = exposures.computeIfAbsent(candidate.symbol(), ignored -> new Exposure());
        long projectedBuy = exposure.workingBuyQuantity;
        long projectedSell = exposure.workingSellQuantity;
        if (replaced != null) {
            projectedBuy -= signedBuy(replaced);
            projectedSell -= signedSell(replaced);
        }
        projectedBuy += signedBuy(candidate);
        projectedSell += signedSell(candidate);

        long longExposure = exposure.netPosition + projectedBuy;
        long shortExposure = exposure.netPosition - projectedSell;
        long worstCase = Math.max(Math.abs(longExposure), Math.abs(shortExposure));
        if (worstCase > limits.maxPositionQuantity()) {
            return RiskDecision.reject(
                    RiskRejectionReason.POSITION_LIMIT_EXCEEDED,
                    "worst-case position in " + candidate.symbol() + " would be " + worstCase
                            + ", limit is " + limits.maxPositionQuantity());
        }

        exposure.workingBuyQuantity = projectedBuy;
        exposure.workingSellQuantity = projectedSell;
        workingOrders.put(candidate.orderId(), candidate);
        return RiskDecision.approve();
    }

    /** Drops a reservation whose order never reached the book. Unknown IDs are ignored. */
    synchronized void release(long orderId) {
        WorkingOrder removed = workingOrders.remove(orderId);
        if (removed != null) {
            Exposure exposure = exposures.get(removed.symbol());
            exposure.workingBuyQuantity -= signedBuy(removed);
            exposure.workingSellQuantity -= signedSell(removed);
        }
    }

    /**
     * Sets, rather than decrements, the reserved quantity of a working order. Absolute updates are
     * naturally idempotent, so the same matching outcome may be reported more than once without
     * corrupting the reservation. A non-positive remainder retires the order.
     */
    synchronized void recordRemainingQuantity(long orderId, long remainingQuantity) {
        WorkingOrder current = workingOrders.get(orderId);
        if (current == null) {
            return;
        }
        if (remainingQuantity <= 0) {
            release(orderId);
            return;
        }
        if (remainingQuantity == current.remainingQuantity()) {
            return;
        }
        long delta = remainingQuantity - current.remainingQuantity();
        Exposure exposure = exposures.get(current.symbol());
        if (current.side() == Side.BUY) {
            exposure.workingBuyQuantity += delta;
        } else {
            exposure.workingSellQuantity += delta;
        }
        workingOrders.put(orderId, current.withRemainingQuantity(remainingQuantity));
    }

    /**
     * Applies a fill at most once per execution and side.
     *
     * <p>Two independent paths report the same fill: the gateway settles it synchronously so that
     * admission decisions see it immediately, and the execution event stream replays it so that the
     * position can be rebuilt after a restart. Both must be able to run without the position being
     * counted twice, so the execution id is the deciding key rather than the call being the trigger.
     *
     * <p>Buys and sells are tracked separately because an account can be both sides of the same
     * execution, and a single set keyed on execution id would book only half of a self-trade.
     *
     * @return true if this call moved the position
     */
    synchronized boolean recordFill(long executionId, String symbol, Side side, long quantity) {
        Exposure exposure = exposures.computeIfAbsent(symbol, ignored -> new Exposure());
        Set<Long> applied =
                side == Side.BUY ? exposure.appliedBuyExecutions : exposure.appliedSellExecutions;
        if (!applied.add(executionId)) {
            return false;
        }
        exposure.netPosition += side == Side.BUY ? quantity : -quantity;
        return true;
    }

    synchronized AccountRiskSnapshot snapshot(String accountId) {
        List<AccountRiskSnapshot.SymbolExposure> symbols = new ArrayList<>(exposures.size());
        exposures.forEach((symbol, exposure) -> symbols.add(
                new AccountRiskSnapshot.SymbolExposure(
                        symbol,
                        exposure.netPosition,
                        exposure.workingBuyQuantity,
                        exposure.workingSellQuantity)));
        symbols.sort(Comparator.comparing(AccountRiskSnapshot.SymbolExposure::symbol));
        return new AccountRiskSnapshot(accountId, workingOrders.size(), symbols);
    }

    private static long signedBuy(WorkingOrder order) {
        return order.side() == Side.BUY ? order.remainingQuantity() : 0;
    }

    private static long signedSell(WorkingOrder order) {
        return order.side() == Side.SELL ? order.remainingQuantity() : 0;
    }

    private static final class Exposure {
        // Retained for the life of the process. Stage 7's snapshot-and-prune is what bounds this;
        // until then a long-running account accumulates one entry per fill.
        private final Set<Long> appliedBuyExecutions = new HashSet<>();
        private final Set<Long> appliedSellExecutions = new HashSet<>();
        private long netPosition;
        private long workingBuyQuantity;
        private long workingSellQuantity;
    }
}
