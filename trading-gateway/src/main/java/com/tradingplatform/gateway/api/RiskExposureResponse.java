package com.tradingplatform.gateway.api;

import com.tradingplatform.risk.AccountRiskSnapshot;
import java.util.List;

/**
 * The account's current pre-trade risk state: what {@link com.tradingplatform.risk.PreTradeRiskEngine}
 * would check the next order against.
 *
 * <p>This is the in-memory, synchronous view, not the durable one {@code /pnl} and {@code
 * /positions} read from Postgres. The two are expected to agree once both sides of a fill have
 * settled — see ADR-003 and ADR-010 — but this one exists specifically to answer "what would happen
 * if I sent an order right now", which only the live risk engine can answer.
 */
public record RiskExposureResponse(String accountId, int openOrders, List<SymbolExposure> exposures) {

    public static RiskExposureResponse from(AccountRiskSnapshot snapshot) {
        return new RiskExposureResponse(
                snapshot.accountId(),
                snapshot.openOrders(),
                snapshot.exposures().stream().map(SymbolExposure::from).toList());
    }

    /**
     * @param longExposure worst-case position if every working buy filled and no working sell did
     * @param shortExposure worst-case position if every working sell filled and no working buy did
     */
    public record SymbolExposure(
            String symbol,
            long netPosition,
            long workingBuyQuantity,
            long workingSellQuantity,
            long longExposure,
            long shortExposure) {

        static SymbolExposure from(AccountRiskSnapshot.SymbolExposure exposure) {
            return new SymbolExposure(
                    exposure.symbol(),
                    exposure.netPosition(),
                    exposure.workingBuyQuantity(),
                    exposure.workingSellQuantity(),
                    exposure.longExposure(),
                    exposure.shortExposure());
        }
    }
}
