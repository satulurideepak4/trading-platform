package com.tradingplatform.gateway.api;

import com.tradingplatform.portfolio.PortfolioReadModel.PositionView;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * @param netQuantity positive is long, negative is short
 * @param averageEntryPrice absent when the position is flat; an average price of nothing is not a
 *     number
 * @param markPrice absent when the instrument has never traded, in which case unrealizedPnl is
 *     reported as zero rather than as a loss against a price of nothing
 */
public record PositionResponse(
        String strategyId,
        String symbol,
        long netQuantity,
        BigDecimal averageEntryPrice,
        Long markPrice,
        long realizedPnl,
        long unrealizedPnl,
        long totalPnl,
        long boughtQuantity,
        long soldQuantity,
        long executionCount,
        Instant updatedAt) {

    public static PositionResponse from(PositionView position) {
        return new PositionResponse(
                position.strategyId(),
                position.symbol(),
                position.netQuantity(),
                position.averageEntryPrice(),
                position.markPrice(),
                position.realizedPnl(),
                position.unrealizedPnl(),
                position.totalPnl(),
                position.boughtQuantity(),
                position.soldQuantity(),
                position.executionCount(),
                position.updatedAt());
    }
}
