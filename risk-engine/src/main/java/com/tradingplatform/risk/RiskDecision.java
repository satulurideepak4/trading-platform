package com.tradingplatform.risk;

import java.util.Optional;

/**
 * Result of a pre-trade check. An approved decision means the order's worst-case exposure is
 * already reserved against the account and must later be settled or released.
 */
public record RiskDecision(
        boolean approved, Optional<RiskRejectionReason> reason, String detail) {

    private static final RiskDecision APPROVED =
            new RiskDecision(true, Optional.empty(), "approved");

    public RiskDecision {
        if (approved == reason.isPresent()) {
            throw new IllegalArgumentException("approved decisions cannot carry a rejection reason");
        }
    }

    public static RiskDecision approve() {
        return APPROVED;
    }

    public static RiskDecision reject(RiskRejectionReason reason, String detail) {
        return new RiskDecision(false, Optional.of(reason), detail);
    }
}
