package com.tradingplatform.risk;

import java.util.List;

public record AccountRiskSnapshot(
        String accountId, int openOrders, List<SymbolExposure> exposures) {

    public AccountRiskSnapshot {
        exposures = List.copyOf(exposures);
    }

    /**
     * Worst-case quantities for one instrument. {@code longExposure} is the position the account
     * would hold if every working buy filled and no working sell did; {@code shortExposure} is the
     * mirror case. Pre-trade position limits are checked against these, not against
     * {@code netPosition}, because a limit must hold even in the worst fill sequence.
     */
    public record SymbolExposure(
            String symbol,
            long netPosition,
            long workingBuyQuantity,
            long workingSellQuantity) {

        public long longExposure() {
            return netPosition + workingBuyQuantity;
        }

        public long shortExposure() {
            return netPosition - workingSellQuantity;
        }
    }
}
