package com.tradingplatform.risk;

import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory pre-trade risk checks applied before an order is allowed to reach a matching worker.
 *
 * <p>Risk state lives entirely in memory because every accepted order pays for these checks and a
 * database round trip per rule would put storage latency on the ingress path. In-memory state is
 * authoritative while the process runs; rebuilding it from the durable execution history after a
 * restart is Stage 5 work and is not implemented here.
 *
 * <p>The engine deliberately reserves worst-case exposure at check time and settles it once the
 * matching outcome is known. Checking without reserving would let a burst of concurrent orders all
 * pass against the same pre-burst exposure.
 */
public final class PreTradeRiskEngine {
    private final InstrumentRegistry instruments;
    private final RiskLimits limits;
    private final Map<String, AccountRiskState> accounts = new ConcurrentHashMap<>();

    public PreTradeRiskEngine(InstrumentRegistry instruments, RiskLimits limits) {
        this.instruments = instruments;
        this.limits = limits;
    }

    /**
     * Runs every pre-trade rule and, on approval, reserves the order's worst-case exposure. The
     * caller must eventually call {@link #release} or {@link #recordRemainingQuantity} so the
     * reservation does not leak.
     */
    public RiskDecision reserve(String accountId, RiskCheckRequest request) {
        if (!instruments.isTradable(request.symbol())) {
            return RiskDecision.reject(
                    RiskRejectionReason.INSTRUMENT_NOT_TRADABLE,
                    request.symbol() + " is not an enabled instrument");
        }
        if (request.quantity() > limits.maxOrderQuantity()) {
            return RiskDecision.reject(
                    RiskRejectionReason.MAX_ORDER_QUANTITY_EXCEEDED,
                    "quantity " + request.quantity() + " exceeds limit "
                            + limits.maxOrderQuantity());
        }

        long effectivePrice = effectivePrice(request);
        if (effectivePrice <= 0) {
            return RiskDecision.reject(
                    RiskRejectionReason.REFERENCE_PRICE_UNAVAILABLE,
                    "no reference price for " + request.symbol()
                            + ", so a market order cannot be sized");
        }

        long notional;
        try {
            notional = Math.multiplyExact(request.quantity(), effectivePrice);
        } catch (ArithmeticException overflow) {
            // An order large enough to overflow a long notional is far past any sane limit.
            return RiskDecision.reject(
                    RiskRejectionReason.MAX_ORDER_NOTIONAL_EXCEEDED,
                    "notional overflows 64-bit arithmetic");
        }
        if (notional > limits.maxOrderNotional()) {
            return RiskDecision.reject(
                    RiskRejectionReason.MAX_ORDER_NOTIONAL_EXCEEDED,
                    "notional " + notional + " exceeds limit " + limits.maxOrderNotional());
        }

        WorkingOrder candidate = new WorkingOrder(
                request.orderId(),
                request.symbol(),
                request.side(),
                request.quantity(),
                effectivePrice);
        return state(accountId).reserve(candidate, limits);
    }

    public void release(String accountId, long orderId) {
        state(accountId).release(orderId);
    }

    /** Settles a reservation against the quantity the matching engine says is still working. */
    public void recordRemainingQuantity(String accountId, long orderId, long remainingQuantity) {
        state(accountId).recordRemainingQuantity(orderId, remainingQuantity);
    }

    /**
     * Applies a fill to an account's position, at most once per execution and side.
     *
     * @return true if this call moved the position, false if the execution was already applied
     */
    public boolean recordFill(
            String accountId, long executionId, String symbol, Side side, long quantity) {
        return state(accountId).recordFill(executionId, symbol, side, quantity);
    }

    public AccountRiskSnapshot snapshot(String accountId) {
        return state(accountId).snapshot(accountId);
    }

    public RiskLimits limits() {
        return limits;
    }

    public InstrumentRegistry instruments() {
        return instruments;
    }

    private long effectivePrice(RiskCheckRequest request) {
        return request.type() == OrderType.MARKET
                ? instruments.referencePrice(request.symbol())
                : request.price();
    }

    private AccountRiskState state(String accountId) {
        return accounts.computeIfAbsent(accountId, ignored -> new AccountRiskState());
    }
}
