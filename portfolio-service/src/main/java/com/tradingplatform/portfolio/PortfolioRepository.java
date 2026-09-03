package com.tradingplatform.portfolio;

import com.tradingplatform.pipeline.events.ExecutionCreated;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * All portfolio SQL, in one place so the statements that carry the correctness guarantees are
 * readable together rather than scattered behind an ORM.
 *
 * <p>Every method funnels connection-level failures into {@link TransientPersistenceException}, so
 * the consumer's error handler can tell "the database is down" from "this record is nonsense" and
 * treat them completely differently.
 */
public class PortfolioRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public PortfolioRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Records the trade, or reports that it was already recorded.
     *
     * <p>This is the idempotency mechanism for the whole portfolio. The insert and the position
     * updates it guards run in one transaction, so "the execution row exists" and "the positions
     * include it" are the same fact. There is no separate processed-ids table that could disagree
     * with the data it is meant to protect.
     *
     * @return true if this call recorded the trade, false if it was already known
     */
    public boolean recordExecutionIfAbsent(ExecutionCreated execution) {
        return withTransientFailureMapping("record execution " + execution.executionId(), () ->
                jdbc.update(
                        """
                        INSERT INTO executions (
                            execution_id, symbol, price, quantity,
                            buy_order_id, buy_account_id, buy_strategy_id,
                            sell_order_id, sell_account_id, sell_strategy_id,
                            maker_order_id, taker_order_id, occurred_at)
                        VALUES (
                            :executionId, :symbol, :price, :quantity,
                            :buyOrderId, :buyAccountId, :buyStrategyId,
                            :sellOrderId, :sellAccountId, :sellStrategyId,
                            :makerOrderId, :takerOrderId, :occurredAt)
                        ON CONFLICT (execution_id) DO NOTHING
                        """,
                        new MapSqlParameterSource()
                                .addValue("executionId", execution.executionId())
                                .addValue("symbol", execution.symbol())
                                .addValue("price", execution.price())
                                .addValue("quantity", execution.quantity())
                                .addValue("buyOrderId", execution.buyOrderId())
                                .addValue("buyAccountId", execution.buyAccountId())
                                .addValue("buyStrategyId", execution.buyStrategyId())
                                .addValue("sellOrderId", execution.sellOrderId())
                                .addValue("sellAccountId", execution.sellAccountId())
                                .addValue("sellStrategyId", execution.sellStrategyId())
                                .addValue("makerOrderId", execution.makerOrderId())
                                .addValue("takerOrderId", execution.takerOrderId())
                                .addValue("occurredAt", atUtc(execution.occurredAt())))
                        > 0);
    }

    /**
     * Reads a position and holds its row for the rest of the transaction.
     *
     * <p>The lock is not strictly needed for the consumer, whose partitioning already gives one
     * writer per position. It is needed because reconciliation is a second writer, and a rebuild
     * running while trades arrive would otherwise interleave two read-modify-write cycles.
     */
    public PositionState lockAndRead(PositionKey key) {
        return withTransientFailureMapping("read position " + key, () -> jdbc.query(
                        """
                        SELECT open_quantity, open_cost, realized_pnl,
                               bought_quantity, sold_quantity, execution_count
                        FROM positions
                        WHERE account_id = :accountId
                          AND strategy_id = :strategyId
                          AND symbol = :symbol
                        FOR UPDATE
                        """,
                        keyParameters(key),
                        (ResultSet rows) -> rows.next() ? readState(rows) : PositionState.FLAT));
    }

    public void upsert(PositionKey key, PositionState state, long lastExecutionId, Instant updatedAt) {
        withTransientFailureMapping("write position " + key, () -> jdbc.update(
                """
                INSERT INTO positions (
                    account_id, strategy_id, symbol,
                    open_quantity, open_cost, realized_pnl,
                    bought_quantity, sold_quantity, execution_count,
                    last_execution_id, updated_at)
                VALUES (
                    :accountId, :strategyId, :symbol,
                    :openQuantity, :openCost, :realizedPnl,
                    :boughtQuantity, :soldQuantity, :executionCount,
                    :lastExecutionId, :updatedAt)
                ON CONFLICT (account_id, strategy_id, symbol) DO UPDATE SET
                    open_quantity = EXCLUDED.open_quantity,
                    open_cost = EXCLUDED.open_cost,
                    realized_pnl = EXCLUDED.realized_pnl,
                    bought_quantity = EXCLUDED.bought_quantity,
                    sold_quantity = EXCLUDED.sold_quantity,
                    execution_count = EXCLUDED.execution_count,
                    last_execution_id = EXCLUDED.last_execution_id,
                    updated_at = EXCLUDED.updated_at
                """,
                keyParameters(key)
                        .addValue("openQuantity", state.openQuantity())
                        .addValue("openCost", state.openCost())
                        .addValue("realizedPnl", state.realizedPnl())
                        .addValue("boughtQuantity", state.boughtQuantity())
                        .addValue("soldQuantity", state.soldQuantity())
                        .addValue("executionCount", state.executionCount())
                        .addValue("lastExecutionId", lastExecutionId)
                        .addValue("updatedAt", atUtc(updatedAt))));
    }

    public Optional<PositionRecord> findPosition(PositionKey key) {
        List<PositionRecord> found = jdbc.query(
                """
                SELECT account_id, strategy_id, symbol,
                       open_quantity, open_cost, realized_pnl,
                       bought_quantity, sold_quantity, execution_count,
                       last_execution_id, updated_at
                FROM positions
                WHERE account_id = :accountId AND strategy_id = :strategyId AND symbol = :symbol
                """,
                keyParameters(key),
                POSITION_MAPPER);
        return found.stream().findFirst();
    }

    /** Uses the leftmost columns of the primary key, so no extra index is needed. */
    public List<PositionRecord> findPositionsForAccount(String accountId) {
        return jdbc.query(
                """
                SELECT account_id, strategy_id, symbol,
                       open_quantity, open_cost, realized_pnl,
                       bought_quantity, sold_quantity, execution_count,
                       last_execution_id, updated_at
                FROM positions
                WHERE account_id = :accountId
                ORDER BY strategy_id, symbol
                """,
                new MapSqlParameterSource("accountId", accountId),
                POSITION_MAPPER);
    }

    public List<PositionRecord> findAllPositions() {
        return jdbc.query(
                """
                SELECT account_id, strategy_id, symbol,
                       open_quantity, open_cost, realized_pnl,
                       bought_quantity, sold_quantity, execution_count,
                       last_execution_id, updated_at
                FROM positions
                ORDER BY account_id, strategy_id, symbol
                """,
                POSITION_MAPPER);
    }

    /**
     * An account is on one side or the other of a trade, so both indexed columns are searched.
     * Postgres combines the two index scans rather than falling back to a sequential scan.
     */
    public List<ExecutionRecord> findExecutionsForAccount(String accountId, int limit, int offset) {
        return jdbc.query(
                """
                SELECT execution_id, symbol, price, quantity,
                       buy_order_id, buy_account_id, buy_strategy_id,
                       sell_order_id, sell_account_id, sell_strategy_id,
                       maker_order_id, taker_order_id, occurred_at
                FROM executions
                WHERE buy_account_id = :accountId OR sell_account_id = :accountId
                ORDER BY occurred_at DESC, execution_id DESC
                LIMIT :limit OFFSET :offset
                """,
                new MapSqlParameterSource("accountId", accountId)
                        .addValue("limit", limit)
                        .addValue("offset", offset),
                EXECUTION_MAPPER);
    }

    /**
     * Streams the whole trade history in execution order for a rebuild. Row-by-row rather than
     * into a list, because reconciliation must not be bounded by how much of the history fits in
     * memory.
     */
    public void forEachExecutionInOrder(Consumer<ExecutionRecord> handler) {
        jdbc.getJdbcTemplate()
                .query(
                        """
                        SELECT execution_id, symbol, price, quantity,
                               buy_order_id, buy_account_id, buy_strategy_id,
                               sell_order_id, sell_account_id, sell_strategy_id,
                               maker_order_id, taker_order_id, occurred_at
                        FROM executions
                        ORDER BY execution_id
                        """,
                        (ResultSet rows) -> {
                            handler.accept(EXECUTION_MAPPER.mapRow(rows, rows.getRow()));
                        });
    }

    public long countExecutions() {
        Long count = jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM executions", Long.class);
        return count == null ? 0 : count;
    }

    public void upsertMarkPrice(String symbol, long price, String source, Instant asOf) {
        withTransientFailureMapping("write mark price for " + symbol, () -> jdbc.update(
                """
                INSERT INTO mark_prices (symbol, price, source, as_of, updated_at)
                VALUES (:symbol, :price, :source, :asOf, now())
                ON CONFLICT (symbol) DO UPDATE SET
                    price = EXCLUDED.price,
                    source = EXCLUDED.source,
                    as_of = EXCLUDED.as_of,
                    updated_at = now()
                -- A late-arriving older price must not overwrite a newer one.
                WHERE mark_prices.as_of <= EXCLUDED.as_of
                """,
                new MapSqlParameterSource("symbol", symbol)
                        .addValue("price", price)
                        .addValue("source", source)
                        .addValue("asOf", atUtc(asOf))));
    }

    public Map<String, Long> markPrices() {
        return jdbc.query("SELECT symbol, price FROM mark_prices", (ResultSet rows) -> {
            Map<String, Long> prices = new java.util.HashMap<>();
            while (rows.next()) {
                prices.put(rows.getString("symbol"), rows.getLong("price"));
            }
            return Map.copyOf(prices);
        });
    }

    /** Used by reconciliation to rebuild from scratch. Never called on the trading path. */
    public void deleteAllPositions() {
        jdbc.getJdbcTemplate().update("DELETE FROM positions");
    }

    /**
     * Blocks every concurrent writer of {@code positions} — {@link #upsert}, including the one
     * {@link #lockAndRead}'s own per-row {@code FOR UPDATE} normally guards against — until this
     * transaction commits or rolls back.
     *
     * <p>{@code lockAndRead}'s row lock protects one key at a time, which is exactly wrong for a
     * bulk delete-and-replace: reconciliation's {@code repair()} recomputes every position from
     * history, deletes the whole table, and reinserts the result in one transaction, and a trade
     * committed by a concurrent {@code PortfolioUpdater} anywhere in that window — after the
     * recompute read but before the delete — would otherwise be deleted and then silently
     * overwritten with the stale, pre-trade value {@code repair()} computed before it ever
     * happened. {@code EXCLUSIVE} mode blocks exactly that: it conflicts with the {@code ROW
     * EXCLUSIVE} lock every write takes, while still permitting concurrent plain reads.
     */
    public void lockPositionsTableExclusively() {
        jdbc.getJdbcTemplate().update("LOCK TABLE positions IN EXCLUSIVE MODE");
    }

    private static final RowMapper<PositionRecord> POSITION_MAPPER = (rows, rowNumber) ->
            new PositionRecord(
                    new PositionKey(
                            rows.getString("account_id"),
                            rows.getString("strategy_id"),
                            rows.getString("symbol")),
                    readState(rows),
                    rows.getObject("last_execution_id", Long.class),
                    rows.getObject("updated_at", OffsetDateTime.class).toInstant());

    private static final RowMapper<ExecutionRecord> EXECUTION_MAPPER = (rows, rowNumber) ->
            new ExecutionRecord(
                    rows.getLong("execution_id"),
                    rows.getString("symbol"),
                    rows.getLong("price"),
                    rows.getLong("quantity"),
                    rows.getLong("buy_order_id"),
                    rows.getString("buy_account_id"),
                    rows.getString("buy_strategy_id"),
                    rows.getLong("sell_order_id"),
                    rows.getString("sell_account_id"),
                    rows.getString("sell_strategy_id"),
                    rows.getLong("maker_order_id"),
                    rows.getLong("taker_order_id"),
                    rows.getObject("occurred_at", OffsetDateTime.class).toInstant());

    private static PositionState readState(ResultSet rows) throws SQLException {
        return new PositionState(
                rows.getLong("open_quantity"),
                rows.getLong("open_cost"),
                rows.getLong("realized_pnl"),
                rows.getLong("bought_quantity"),
                rows.getLong("sold_quantity"),
                rows.getLong("execution_count"));
    }

    private static MapSqlParameterSource keyParameters(PositionKey key) {
        return new MapSqlParameterSource("accountId", key.accountId())
                .addValue("strategyId", key.strategyId())
                .addValue("symbol", key.symbol());
    }

    private static OffsetDateTime atUtc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static <T> T withTransientFailureMapping(String what, java.util.function.Supplier<T> work) {
        try {
            return work.get();
        } catch (TransientDataAccessException | CannotGetJdbcConnectionException transientFailure) {
            throw new TransientPersistenceException("could not " + what, transientFailure);
        } catch (DataAccessException failure) {
            // A connection dropped mid-statement surfaces as a non-transient type, but is exactly
            // the case that must be retried rather than dead-lettered.
            if (isConnectionFailure(failure)) {
                throw new TransientPersistenceException("could not " + what, failure);
            }
            throw failure;
        }
    }

    private static void withTransientFailureMapping(String what, Runnable work) {
        withTransientFailureMapping(what, () -> {
            work.run();
            return null;
        });
    }

    private static boolean isConnectionFailure(DataAccessException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof java.sql.SQLNonTransientConnectionException
                    || cause instanceof java.sql.SQLTransientConnectionException
                    || cause instanceof java.net.SocketException) {
                return true;
            }
        }
        return false;
    }
}
