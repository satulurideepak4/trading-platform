package com.tradingplatform.portfolio;

import com.tradingplatform.domain.Side;
import com.tradingplatform.pipeline.events.ExecutionCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies one trade to the portfolio, exactly once.
 *
 * <h2>Why this is a single transaction</h2>
 *
 * Recording the execution and moving both positions happen together or not at all. That one
 * property answers most of the failure questions on its own:
 *
 * <ul>
 *   <li><b>The database write fails.</b> Nothing is committed, the listener throws, the offset is
 *       not committed, and the record is redelivered. No half-applied trade exists.
 *   <li><b>The write succeeds but the consumer dies before committing its offset.</b> The record is
 *       redelivered, the execution insert conflicts, and the whole apply is skipped. The commit
 *       that already happened is the proof it was applied.
 *   <li><b>A duplicate arrives at any time.</b> Same as above, and it costs one failed insert.
 * </ul>
 *
 * The alternative — a separate table of processed event ids — needs the same transaction anyway,
 * and adds a second place for the truth to live.
 *
 * <h2>Self-trades</h2>
 *
 * An account can be both sides of one execution. Both legs are still applied — a self-trade is
 * never skipped or short-circuited — using the same sequential read-then-write path as any other
 * execution: the second leg's read sees the first leg's write within the same transaction, so
 * neither leg is lost or silently overwritten by the other. Bought and sold quantities are each
 * recorded in full (turnover is real), and the account's net open quantity nets to zero only as a
 * consequence of applying both legs to the same position, not because either leg was skipped.
 */
public class PortfolioUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(PortfolioUpdater.class);
    private static final String LAST_TRADE = "LAST_TRADE";

    private final PortfolioRepository repository;

    public PortfolioUpdater(PortfolioRepository repository) {
        this.repository = repository;
    }

    /**
     * @return true if this call applied the trade, false if it had already been applied
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public boolean apply(ExecutionCreated execution) {
        if (!repository.recordExecutionIfAbsent(execution)) {
            LOG.debug("event=duplicate_execution_ignored executionId={}", execution.executionId());
            return false;
        }

        applyLeg(
                new PositionKey(
                        execution.buyAccountId(), execution.buyStrategyId(), execution.symbol()),
                Side.BUY,
                execution);
        applyLeg(
                new PositionKey(
                        execution.sellAccountId(), execution.sellStrategyId(), execution.symbol()),
                Side.SELL,
                execution);

        // Until Stage 6 brings a real feed, the last trade is the mark. It is written inside the
        // same transaction so an unrealized P&L reading can never be based on a price from a trade
        // that was rolled back.
        repository.upsertMarkPrice(
                execution.symbol(), execution.price(), LAST_TRADE, execution.occurredAt());
        return true;
    }

    private void applyLeg(PositionKey key, Side side, ExecutionCreated execution) {
        PositionState current = repository.lockAndRead(key);
        PositionState updated = current.applyFill(side, execution.quantity(), execution.price());
        repository.upsert(key, updated, execution.executionId(), execution.occurredAt());
    }
}
