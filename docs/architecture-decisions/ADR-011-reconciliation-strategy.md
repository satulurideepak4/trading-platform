# ADR-011: Reconciliation by full replay, not incremental checking

* Status: Accepted
* Date: 2026-08-19

## Context

ADR-010 makes the normal path for positions correct: one transaction, one primary key, idempotent
by construction. That is a claim about the code, not a guarantee that holds under every condition —
a hand-edited row, a restore from an older backup, a bug in `PositionState.applyFill` that shipped
and ran for a while before being caught, or a trade that reached Postgres through some path other
than `PortfolioUpdater`, would all leave positions that no longer match the history they were
derived from, and none of them announce themselves. Something has to be able to answer "do the
stored positions still agree with what actually traded" independently of the code path that
produced them.

## Decision

**Positions are treated as a derived, rebuildable cache; `executions` is treated as the record of
truth.** Every position is a fold over a subset of the executions table, so reconciliation does not
need a parallel bookkeeping mechanism — it needs to run the same fold again and compare.

**Full replay, not incremental checking.** `PositionReconciliation` reads every row in `executions`,
in `execution_id` order, and reduces it into positions using the exact same `PositionState`
arithmetic the live path uses. This is deliberately simple: there is no separate "since last
checkpoint" reconciliation state to keep correct, and a full replay is precisely as trustworthy as
the thing it is trying to verify — a partial or incremental check would only be as good as its own
bookkeeping, which is the same category of problem reconciliation exists to catch elsewhere.

**Replay order is `execution_id`, never `occurred_at`.** Weighted-average-cost accounting is
path-dependent: replaying two trades in the wrong relative order produces a different average entry
price and a different realized P&L split, even though both trades are the same two trades. Two
trades can share a timestamp; they can never share an execution id, because ids come from the
matching engine's disjoint per-worker sequences. Ordering by anything else would make the rebuild
itself an unreliable source of truth.

**`check()` and `repair()` are two different operations, not one with a flag.** `check()` never
writes. `repair()` recomputes, then deletes every stored position and reinserts the rebuild's
answer, in one transaction — and it still returns the "before" differences, so a repair action
always carries a record of what it changed. Splitting them is a deliberate friction point:
overwriting derived state is an operational decision that should be visible as its own call, not a
side effect of a routine health check.

**No incremental delta application.** `repair()` does not try to compute and apply the *difference*
between stored and rebuilt state; it replaces stored state with rebuilt state wholesale. Computing a
correct delta requires trusting the stored state enough to diff against it meaningfully, which is
exactly the thing that is in question when reconciliation is being run at all.

## Consequences

* Reconciliation is trivially correct to reason about: its output is a function of `executions`
  alone, nothing else.
* Cost scales with the size of the `executions` table, streamed row by row
  (`forEachExecutionInOrder`) rather than materialized, so it does not have a memory ceiling tied to
  history size — but it does have a time cost that grows with the platform's lifetime trade volume.
  A full nightly reconciliation is affordable at moderate scale; reconciling continuously, or at very
  large trade volumes, needs a checkpointed/incremental design this ADR explicitly does not attempt.
* `repair()` is safe to run while trades are landing, because both paths are transactional against
  the same primary key; whether a trade committed one instant before or after a `repair()` began is
  the only thing that can vary, and either outcome is a complete, consistent answer at the transaction
  boundary — never a partially-applied one.
* `repair()` cannot fix damage to `executions` itself. A deleted or corrupted execution row is a
  genuine, permanent loss of trade history; reconciliation only checks that positions agree with
  whatever history currently exists, not that the history is complete. Protecting `executions` — 
  backups, retention — is an operational concern this ADR does not cover.
* There is currently no scheduled trigger and no HTTP surface for `check()`/`repair()`; both are
  called programmatically today (see tests). Wiring one in — a scheduled job with alerting on
  drift, an authenticated operator endpoint for `repair()` — is explicitly future work, noted in
  docs/recovery-model.md.

## Alternatives considered

* **Incremental/checkpointed reconciliation** (verify only executions since the last check): scales
  better at high volume, but needs its own durable checkpoint and its own correctness argument for
  "nothing was missed between checkpoints" — complexity this stage does not yet need to pay for.
* **Continuous reconciliation on every write:** would catch drift immediately, but doubles the cost
  of every trade for a property that, once ADR-010's transactional guarantee holds, should not
  actually drift in normal operation. Reconciliation is a check against the *unusual* case, not
  something that belongs on the hot path.
* **Trusting `positions` as authoritative and never re-deriving it:** simplest, and exactly the
  position this ADR rejects — it would leave a hand-edited row or a shipped bug in the fold
  permanently undetected.
