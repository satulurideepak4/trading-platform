package com.tradingplatform.gateway.api;

import com.tradingplatform.portfolio.PortfolioReadModel.ProfitAndLoss;
import java.util.List;

public record ProfitAndLossResponse(
        String accountId,
        long realizedPnl,
        long unrealizedPnl,
        long totalPnl,
        List<PositionResponse> positions) {

    public static ProfitAndLossResponse from(ProfitAndLoss pnl) {
        return new ProfitAndLossResponse(
                pnl.accountId(),
                pnl.realizedPnl(),
                pnl.unrealizedPnl(),
                pnl.totalPnl(),
                pnl.positions().stream().map(PositionResponse::from).toList());
    }
}
