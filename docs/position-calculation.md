# Position and P&L calculation

Positions are tracked per **account, strategy and instrument**. A `strategyId` travels with every
order (defaulting to `DEFAULT` if the client does not separate books) precisely so that one account
running several strategies against the same instrument gets independent positions rather than one
number that mixes them together.

## Representation: no floating point, ever

Prices are integer ticks and quantities are whole units, so every monetary value in this system is
an exact product of the two — call it a "tick-quantity" unit. A position is stored as two signed
integers:

* `openQuantity` — positive is long, negative is short.
* `openCost` — the cost of the currently open position, in tick-quantity units, signed to match
  `openQuantity`. A long has paid out, so its cost is positive; a short has received proceeds, so
  its cost is negative.

Unrealized P&L falls straight out of that representation with **no division anywhere**:

```text
unrealizedPnl = openQuantity * markPrice - openCost
```

The only place a division ever happens is computing the **average entry price** for display
(`openCost / openQuantity`), and that value is never written back or fed into another calculation —
it is derived fresh on every read, so a rounded number can never contaminate the ledger.

`PositionState` (in `portfolio-service`) is the whole of this arithmetic, and it is a pure function
with no I/O — see `PositionStateTest` for the property-based coverage, including a 200-run randomized
round-trip test asserting that closing a position out always realizes exactly proceeds minus cost.

## Accounting method: weighted average cost

Adding to a position adds to its cost. Reducing one realizes the difference between the trade price
and the *average* cost of the part closed — not FIFO, not LIFO. A trade large enough to flip the
sign (a sell of 15 against a long of 10) closes the old position completely and opens a new one at
the trade price; anything else would attribute the wrong average to either side of that flip.

Reducing part of a position needs a *proportional share* of its cost, and that is the one place
integer division appears mid-calculation:

```text
costOfClosing = openCost * closingQuantity / |openQuantity|
```

A **full** close skips this division entirely and takes the whole remaining cost — that is what
makes a complete round trip exact regardless of how many partial closes preceded it. A partial close
can be off by at most one tick-quantity unit until the position is fully closed, at which point that
last unit is recovered by the full-close path rather than silently discarded. `PositionStateTest`
exercises this directly (`partialClosesDoNotLoseValueToRounding`).

The intermediate multiplication (`openCost * closingQuantity`) is done in `BigInteger`, because a
large position multiplied by a large partial close can overflow a `long` well inside plausible
trading sizes — and an overflow here would silently produce a wrong number rather than fail loudly.

## Self-trades

An account can legitimately be both the buyer and the seller of one execution (crossing its own
resting order). Both legs are applied — buy quantity and sell quantity both increase, and they net
against `openQuantity` correctly — rather than being coalesced into one no-op. This needed a
specific fix during development: an earlier version deduplicated fills by execution id in one shared
set, which silently dropped the second leg of a self-trade. See
`PortfolioUpdaterTest.aSelfTradeBooksBothLegsInsteadOfNettingToNothing` and the equivalent test in
`PositionStore` (Stage 4) / `AccountRiskState` (risk engine) — the same bug class was fixed in all
three places a fill is applied.

## Where the calculation runs, and why it runs twice

The **same** `PositionState.applyFill` logic effectively runs in two places for every trade:

1. **`PreTradeRiskEngine`**, synchronously, on the gateway request thread, the moment a fill is
   known — because a pre-trade limit check must see the fill that just happened, not the one that
   will arrive over Kafka a few milliseconds later.
2. **`PortfolioUpdater`**, asynchronously, when the `ExecutionCreated` event is consumed from Kafka
   and written to Postgres inside one transaction with the execution record itself.

These are not in tension. The risk engine's copy is the fast, non-durable answer to "would this next
order be allowed"; the portfolio's copy is the durable answer to "what does this account actually
hold". Both are keyed on execution id so a redelivery of the same fill is a no-op in either place —
see [ADR-008](architecture-decisions/ADR-008-consumer-idempotency.md) — and
`ExecutionPipelineIntegrationTest.theRiskProjectionAgreesWithTheDurablePortfolio` asserts the two
views agree.

## Idempotency: one transaction, one primary key

`executions.execution_id` is the primary key, and `PortfolioUpdater.apply` inserts the execution row
and moves both positions inside **one transaction**:

```sql
INSERT INTO executions (execution_id, ...) VALUES (...)
ON CONFLICT (execution_id) DO NOTHING;
```

If the insert reports zero rows affected, the trade was already applied and nothing else in the
method runs. This is deliberately not a separate "have I seen this id" table: the execution row
*is* the durable record of "this trade happened", so checking it and recording it are the same
statement, and there is no second source of truth that could drift from the first. See
[ADR-010](architecture-decisions/ADR-010-persistence-architecture.md).

## What is not modeled yet

* **Fees and commissions** are not tracked. `realizedPnl` is gross.
* **Multi-currency** is not modeled; every price is assumed to be in one implicit currency.
* **Mark prices** come from three sources, all writing the same `mark_prices` row: the last traded
  price (updated in the same transaction as the trade that produced it, source `LAST_TRADE`), the
  manual `PUT /instruments/{symbol}/mark-price` endpoint, and — as of Stage 6 — the live market-data
  feed, polled on a schedule by `MarkPriceUpdater` (source `MARKET_DATA`). Whichever has the most
  recent `as_of` wins; see [market-data.md](market-data.md#closing-the-stage-5-gap-mark-prices).
