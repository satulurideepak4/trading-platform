-- Portfolio schema: the durable record of what traded and what each book now holds.
--
-- Money is stored as BIGINT throughout, never floating point. Prices are integer ticks and
-- quantities are whole units, so every monetary value here is an exact product of the two —
-- "tick-quantity" units. Average entry price is the only value that needs a division, and it is
-- derived on read rather than stored, so no rounding is ever written down.

CREATE TABLE executions (
    execution_id           BIGINT       PRIMARY KEY,
    symbol                 TEXT         NOT NULL,
    price                  BIGINT       NOT NULL CHECK (price > 0),
    quantity               BIGINT       NOT NULL CHECK (quantity > 0),
    buy_order_id           BIGINT       NOT NULL,
    buy_account_id         TEXT         NOT NULL,
    buy_strategy_id        TEXT         NOT NULL,
    sell_order_id          BIGINT       NOT NULL,
    sell_account_id        TEXT         NOT NULL,
    sell_strategy_id       TEXT         NOT NULL,
    maker_order_id         BIGINT       NOT NULL,
    taker_order_id         BIGINT       NOT NULL,
    occurred_at            TIMESTAMPTZ  NOT NULL,
    recorded_at            TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE executions IS
    'Durable trade record. The primary key is also the consumer idempotency key: an insert that '
    'conflicts proves the trade was already applied to positions in the same transaction.';

-- The executions endpoint answers "what did this account trade, most recent first". An account
-- appears on either side of a trade, so one index per side; Postgres combines them with a BitmapOr
-- for the OR predicate rather than falling back to a sequential scan.
CREATE INDEX idx_executions_buy_account ON executions (buy_account_id, occurred_at DESC);
CREATE INDEX idx_executions_sell_account ON executions (sell_account_id, occurred_at DESC);

-- Reconciliation replays every trade for one instrument in execution order. Without this it is a
-- full scan of the whole trade history to rebuild a single symbol.
CREATE INDEX idx_executions_symbol_id ON executions (symbol, execution_id);

CREATE TABLE positions (
    account_id        TEXT         NOT NULL,
    strategy_id       TEXT         NOT NULL,
    symbol            TEXT         NOT NULL,
    -- Signed: positive is long, negative is short.
    open_quantity     BIGINT       NOT NULL,
    -- Cost of the currently open position in tick-quantity units, signed to match open_quantity.
    -- Unrealized P&L is (open_quantity * mark) - open_cost, which needs no division at all.
    open_cost         BIGINT       NOT NULL,
    realized_pnl      BIGINT       NOT NULL,
    bought_quantity   BIGINT       NOT NULL,
    sold_quantity     BIGINT       NOT NULL,
    execution_count   BIGINT       NOT NULL,
    last_execution_id BIGINT,
    updated_at        TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (account_id, strategy_id, symbol)
);

COMMENT ON TABLE positions IS
    'Derived state. Rebuildable in full from the executions table, which is what makes recovery '
    'from a corrupt or lost position row possible.';

-- No separate index on account_id: it is the leftmost column of the primary key, so "every
-- position for this account" and "every position for this account and strategy" are already
-- served. Adding one would cost write throughput and buy nothing.

-- "Who is exposed to this instrument" for risk reporting. Partial, because a position that has
-- gone flat is not exposure and there are far more closed positions than open ones over time.
CREATE INDEX idx_positions_open_by_symbol ON positions (symbol) WHERE open_quantity <> 0;

CREATE TABLE mark_prices (
    symbol      TEXT         PRIMARY KEY,
    price       BIGINT       NOT NULL CHECK (price > 0),
    source      TEXT         NOT NULL,
    as_of       TIMESTAMPTZ  NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE mark_prices IS
    'The price unrealized P&L is measured against. Fed by last traded price today; Stage 6 replaces '
    'that with a real market-data feed.';
