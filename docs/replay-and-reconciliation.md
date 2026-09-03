# Replay and reconciliation

This system now rebuilds three different kinds of state from three different sources of truth. They
were built in three different stages, for three different reasons, and it is worth being explicit
about why there are three rather than one, and how they relate when more than one restart happens at
once.

| Mechanism | Rebuilds | Source of truth | Trigger | Stage |
| --- | --- | --- | --- | --- |
| Matching-engine journal replay | One worker's order book | A local append-only file | Every `OrderRouter` construction, when journaling is enabled | 7 |
| Executions-topic replay | The risk engine's synchronous position view | Kafka's `trading.executions.v1`, from the beginning | Every `risk-state-updater` consumer startup | 4 |
| Position reconciliation | Postgres's `positions` table | Postgres's own `executions` table | Manual: `PositionReconciliation.check()` / `.repair()` | 5 |

## Why three, not one

Each rebuilds state that lives in a different place, from the source that actually has authority over
it:

* The order book is **local, in-process state** with no reason to leave the process — nothing outside
  the worker that owns a symbol ever needs to read `MatchingEngine`'s internals directly. Its source
  of truth is therefore local too: a file next to the process, not a network call away.
* The risk engine's positions are a **synchronous cache of a fact Kafka already durably owns** — every
  execution that produced them was published there first. Rebuilding from Kafka is rebuilding from
  the original, not a copy.
* Postgres's positions are **derived, durable state** — Stage 5's own design note calls them "a cache
  that happens to be durable," folded from the `executions` table, which is Postgres's own copy of
  the same facts. Reconciling from that table catches drift Postgres introduced on its own (a bad
  migration, a manually edited row, a restore from an older backup) that Kafka would have no way to
  detect, because Kafka was never wrong.

A single unified "replay everything from Kafka" mechanism was considered and is not what this system
does, on purpose: it would make the order book's recovery time depend on Kafka being reachable at
all, for state that has never needed Kafka to exist in the first place (the matching engine has no
Kafka dependency — a Stage 1 invariant, preserved through Stage 7's own durability work).

## The key property: replay never republishes

All three mechanisms share one rule, worth stating once rather than once per mechanism: **rebuilding
state must never re-emit an event a downstream consumer already saw.**

* Journal replay calls `MatchingEngine.submit/cancel/replace` directly from `OrderRouter`'s
  constructor — never through `OrderIngressService` or `TradingEventEmitter`, the only components
  that call the Kafka publisher. Rebuilding the book cannot accidentally re-publish an execution that
  already reached Kafka before the crash.
* Executions-topic replay is *reading* Kafka, not writing to it — there is nothing to accidentally
  republish.
* Position reconciliation reads and writes Postgres only; it never touches Kafka.

This is also why journal replay is safe to leave enabled by default with no special-casing: because
matching is deterministic (Stage 1), replaying the same command sequence produces the same execution
ids as the first run. Even in a hypothetical future where replay *did* feed something downstream, that
downstream already treats every event id as at-most-once (ADR-008) — replay would look like ordinary
redelivery, not a new fact. Nothing about this system's idempotency story depends on that hypothetical
being avoided; it depends on event ids being derived from what happened, not generated fresh each
time, which was already true before Stage 7.

## What ties them together after a real incident

A single crash rarely damages only one of these. After, say, a gateway process crash during a broker
blip:

1. **Matching engine** recovers first and fastest — it's local, synchronous, and does not wait on
   anything external. The book is correct before the process finishes starting.
2. **Risk positions** recover next — the gateway reports itself not-ready (`ReplayReadiness`) until
   the executions-topic replay catches up, so no order is admitted against a stale risk view in the
   meantime.
3. **Postgres positions** are not automatically re-checked. If the crash happened to coincide with a
   partial database write, `PositionReconciliation.check()` is how that would be found — it is not
   run on a schedule today (see Known limitations, and the same point in
   `docs/recovery-model.md`).

None of the three needs to wait for another to finish; they read independent sources. What they do
share is that all three are *replays of a record that already existed* — the local journal, the Kafka
topic, the executions table — which is the same property that makes each of them safe to run as often
as needed without fear of duplicating anything.

## Known limitations

* Position reconciliation has no scheduled trigger. It is correct and tested
  (`PositionReconciliationTest`), but nobody calls it automatically after an incident — an operator
  has to know to run it. Exposing it as a scheduled job or an HTTP endpoint is future work (see
  `docs/recovery-model.md`).
* Matching-engine journal replay has no equivalent of `check()` — there is no independent way to
  verify a replayed book matches what would have existed absent the crash, other than the fact that
  matching is deterministic. `OrderRouterRecoveryTest` proves this once, in a test; it is not
  something the running system re-verifies for itself in production.
* If the journal is lost (not just the process), matching-engine state cannot be rebuilt at all — see
  `docs/disaster-recovery.md`.
