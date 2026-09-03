package com.tradingplatform.gateway.api;

import com.tradingplatform.domain.Side;
import com.tradingplatform.portfolio.ExecutionRecord;
import java.time.Instant;

/**
 * One trade from the calling account's own point of view.
 *
 * <p>The counterparty's identity is deliberately absent. A real venue does not reveal who was on
 * the other side of a trade to the account that was not on that side, and there is no reason for
 * this one to behave differently.
 *
 * @param side this account's side of the trade, not the taker's
 * @param liquidity MAKER if this account's order was already resting, TAKER if it aggressed
 */
public record TradeResponse(
        long executionId,
        String symbol,
        Side side,
        long price,
        long quantity,
        String liquidity,
        Instant occurredAt) {

    public static TradeResponse forAccount(String accountId, ExecutionRecord execution) {
        boolean isBuyer = execution.buyAccountId().equals(accountId);
        long orderId = isBuyer ? execution.buyOrderId() : execution.sellOrderId();
        return new TradeResponse(
                execution.executionId(),
                execution.symbol(),
                isBuyer ? Side.BUY : Side.SELL,
                execution.price(),
                execution.quantity(),
                orderId == execution.makerOrderId() ? "MAKER" : "TAKER",
                execution.occurredAt());
    }
}
