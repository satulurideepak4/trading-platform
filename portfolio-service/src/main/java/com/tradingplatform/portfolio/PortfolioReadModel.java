package com.tradingplatform.portfolio;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Read side of the portfolio: positions and P&L as a caller wants to see them.
 *
 * <p>Unrealized P&L is computed on read from the current mark rather than stored, because it is a
 * function of a price that changes without any trade happening. Storing it would mean either
 * rewriting every position on every price tick, or serving a number that quietly went stale.
 */
public class PortfolioReadModel {
    private final PortfolioRepository repository;

    public PortfolioReadModel(PortfolioRepository repository) {
        this.repository = repository;
    }

    public List<PositionView> positionsFor(String accountId) {
        Map<String, Long> marks = repository.markPrices();
        return repository.findPositionsForAccount(accountId).stream()
                .map(position -> toView(position, marks.getOrDefault(position.key().symbol(), 0L)))
                .toList();
    }

    public Optional<PositionView> position(PositionKey key) {
        Map<String, Long> marks = repository.markPrices();
        return repository
                .findPosition(key)
                .map(position -> toView(position, marks.getOrDefault(key.symbol(), 0L)));
    }

    public List<ExecutionRecord> executionsFor(String accountId, int limit, int offset) {
        return repository.findExecutionsForAccount(accountId, limit, offset);
    }

    public Map<String, Long> markPrices() {
        return repository.markPrices();
    }

    public void recordMarkPrice(String symbol, long price, String source, Instant asOf) {
        repository.upsertMarkPrice(symbol, price, source, asOf);
    }

    /** Account-level totals, plus the per-position detail they were summed from. */
    public ProfitAndLoss profitAndLoss(String accountId) {
        List<PositionView> positions = positionsFor(accountId);
        long realized = positions.stream().mapToLong(PositionView::realizedPnl).sum();
        long unrealized = positions.stream().mapToLong(PositionView::unrealizedPnl).sum();
        return new ProfitAndLoss(accountId, realized, unrealized, realized + unrealized, positions);
    }

    private static PositionView toView(PositionRecord position, long markPrice) {
        PositionState state = position.state();
        return new PositionView(
                position.key().accountId(),
                position.key().strategyId(),
                position.key().symbol(),
                state.openQuantity(),
                state.averageEntryPrice().orElse(null),
                markPrice == 0 ? null : markPrice,
                state.realizedPnl(),
                state.unrealizedPnl(markPrice),
                state.totalPnl(markPrice),
                state.boughtQuantity(),
                state.soldQuantity(),
                state.executionCount(),
                position.updatedAt());
    }

    /**
     * @param markPrice null when the instrument has never traded and has no mark, in which case
     *     unrealized P&L is reported as zero rather than as a loss against a price of nothing
     */
    public record PositionView(
            String accountId,
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
            Instant updatedAt) {}

    public record ProfitAndLoss(
            String accountId,
            long realizedPnl,
            long unrealizedPnl,
            long totalPnl,
            List<PositionView> positions) {

        public ProfitAndLoss {
            positions = List.copyOf(positions);
        }
    }
}
