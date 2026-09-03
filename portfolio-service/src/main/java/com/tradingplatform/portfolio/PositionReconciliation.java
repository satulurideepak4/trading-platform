package com.tradingplatform.portfolio;

import com.tradingplatform.domain.Side;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Rebuilds positions from the trade history and compares them with what is stored.
 *
 * <p>Positions are derived state: every one of them is a fold over the executions that produced it.
 * That is what makes this possible, and it is the reason the executions table is treated as the
 * record of truth while the positions table is treated as a cache that happens to be durable.
 *
 * <p>Reconciliation exists because "the code is idempotent" is a claim, not a guarantee. A bug in
 * the fold, a hand-edited row, a restore from an older backup or a trade dead-lettered during an
 * incident all leave positions that no longer match their history, and none of them announce
 * themselves. This recomputes the answer independently and reports the difference.
 */
public class PositionReconciliation {
    private static final Logger LOG = LoggerFactory.getLogger(PositionReconciliation.class);

    private final PortfolioRepository repository;

    public PositionReconciliation(PortfolioRepository repository) {
        this.repository = repository;
    }

    /** Recomputes from history and reports differences without changing anything. */
    @Transactional(readOnly = true)
    public ReconciliationReport check() {
        return compare(recomputeFromHistory());
    }

    /**
     * Recomputes from history and replaces the stored positions with the result.
     *
     * <p>Deliberately a separate call from {@link #check()}. Repairing derived state is an
     * operational decision with an audit trail attached, not something a health check should do on
     * its own.
     *
     * <p>Takes an exclusive table lock before recomputing anything, not just before the delete —
     * otherwise a trade a concurrent {@code PortfolioUpdater} commits after the recompute reads
     * history but before the delete+reinsert below would be deleted and then silently overwritten
     * with the stale value this method computed before that trade ever happened. See
     * {@link PortfolioRepository#lockPositionsTableExclusively()}.
     */
    @Transactional
    public ReconciliationReport repair() {
        repository.lockPositionsTableExclusively();
        Map<PositionKey, Rebuilt> rebuilt = recomputeFromHistory();
        ReconciliationReport report = compare(rebuilt);

        repository.deleteAllPositions();
        Instant now = Instant.now();
        rebuilt.forEach((key, value) ->
                repository.upsert(key, value.state(), value.lastExecutionId(), now));

        LOG.warn(
                "event=positions_repaired positionsRebuilt={} differencesBefore={}",
                rebuilt.size(),
                report.differences().size());
        return report;
    }

    /**
     * Folds the whole trade history in execution-id order.
     *
     * <p>Order matters: average cost depends on the sequence trades arrived in, so replaying them
     * in a different order produces a different, wrong answer. Execution ids come from the matching
     * engine's per-worker sequences, which is why the replay orders by them rather than by
     * timestamp — two trades can share a timestamp, but never an id.
     */
    private Map<PositionKey, Rebuilt> recomputeFromHistory() {
        Map<PositionKey, Rebuilt> rebuilt = new HashMap<>();
        repository.forEachExecutionInOrder(execution -> {
            applyLeg(
                    rebuilt,
                    new PositionKey(
                            execution.buyAccountId(), execution.buyStrategyId(), execution.symbol()),
                    Side.BUY,
                    execution);
            applyLeg(
                    rebuilt,
                    new PositionKey(
                            execution.sellAccountId(),
                            execution.sellStrategyId(),
                            execution.symbol()),
                    Side.SELL,
                    execution);
        });
        return rebuilt;
    }

    private static void applyLeg(
            Map<PositionKey, Rebuilt> rebuilt,
            PositionKey key,
            Side side,
            ExecutionRecord execution) {
        Rebuilt current = rebuilt.getOrDefault(key, Rebuilt.EMPTY);
        rebuilt.put(
                key,
                new Rebuilt(
                        current.state().applyFill(side, execution.quantity(), execution.price()),
                        execution.executionId()));
    }

    private ReconciliationReport compare(Map<PositionKey, Rebuilt> rebuilt) {
        Map<PositionKey, PositionState> stored = new HashMap<>();
        repository.findAllPositions().forEach(record -> stored.put(record.key(), record.state()));

        Set<PositionKey> allKeys = new LinkedHashSet<>(rebuilt.keySet());
        allKeys.addAll(stored.keySet());

        List<Difference> differences = new ArrayList<>();
        for (PositionKey key : allKeys) {
            PositionState expected =
                    rebuilt.getOrDefault(key, Rebuilt.EMPTY).state();
            PositionState actual = stored.getOrDefault(key, PositionState.FLAT);
            if (!expected.equals(actual)) {
                differences.add(new Difference(key, expected, actual));
            }
        }
        differences.sort(Comparator.comparing(difference -> difference.key().toString()));

        return new ReconciliationReport(
                repository.countExecutions(), rebuilt.size(), stored.size(), differences);
    }

    /**
     * @param differences empty means the stored positions are exactly what the history implies
     */
    public record ReconciliationReport(
            long executionsReplayed,
            int positionsRebuilt,
            int positionsStored,
            List<Difference> differences) {

        public ReconciliationReport {
            differences = List.copyOf(differences);
        }

        public boolean matches() {
            return differences.isEmpty();
        }
    }

    public record Difference(PositionKey key, PositionState expected, PositionState actual) {}

    private record Rebuilt(PositionState state, long lastExecutionId) {
        private static final Rebuilt EMPTY = new Rebuilt(PositionState.FLAT, 0);
    }
}
