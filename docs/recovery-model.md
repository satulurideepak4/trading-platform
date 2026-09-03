# Recovery model: positions and the portfolio consumer

This document reasons explicitly through the failure scenarios that matter for the durable
portfolio, and how reconciliation rebuilds derived state from the execution history.

## The property everything else rests on

`PortfolioUpdater.apply` inserts the execution row and moves both positions in **one Postgres
transaction**. That single property is what makes every scenario below tractable: "the execution
is recorded" and "the positions include it" are not two facts that can disagree, because they are
the same commit.

## Scenario: Kafka processing succeeds but the DB write fails

The transaction never commits (Postgres rejected it, or the connection dropped mid-statement). The
`@KafkaListener` method throws. Nothing was written — not the execution row, not either position.

`DownstreamUnavailableException` (thrown when the failure looks like connectivity rather than bad
data — see `PortfolioRepository.withTransientFailureMapping`) is classified as retryable-forever by
`KafkaPipelineConfiguration`'s error handler: a fixed backoff with no attempt limit, rather than the
bounded retry-then-dead-letter path every other consumer failure takes. The offset is not committed,
so the same record is redelivered until the write succeeds. See
[ADR-009](architecture-decisions/ADR-009-retry-and-dead-letter-strategy.md) for why an *ordinary*
failure is bounded and this one deliberately is not: dead-lettering a trade because the database
was briefly unreachable would turn a recoverable outage into a permanently missing position, and
nothing can reconstruct a discarded trade later.

Tested directly: `DatabaseOutageIntegrationTest` pauses the real Postgres container, publishes an
execution, confirms it is **not** on the dead-letter topic, then unpauses and confirms the position
appears once the database returns.

## Scenario: DB succeeds but the consumer crashes before the offset commit

The transaction committed — the execution row and both positions exist in Postgres. The process
dies before `AckMode.RECORD` commits the Kafka offset. On restart (or on the next consumer in the
group after a rebalance), the same record is redelivered.

`recordExecutionIfAbsent`'s `ON CONFLICT (execution_id) DO NOTHING` makes the redelivery a no-op:
zero rows affected, and `apply` returns `false` without touching either position. This is the same
mechanism as the previous scenario's retry, just observed from the other side of the crash — see
[ADR-008](architecture-decisions/ADR-008-consumer-idempotency.md). Tested by
`PipelineFailureIntegrationTest.aRestartedConsumerResumesFromItsCommittedOffsetWithoutDoubleCounting`,
which stops the listener container mid-stream, publishes more records while it is down, and
confirms nothing is lost and nothing is double-counted once it restarts.

## Scenario: a duplicate execution arrives

Indistinguishable, by design, from the crash-redelivery case above: the same primary-key conflict
makes it a no-op. Kafka's at-least-once delivery, a producer retry, and an operator replaying a
segment of the topic all produce the same shape of event — a record with an `execution_id` already
present — and all three are handled by the same one `ON CONFLICT` clause. Tested directly by
`ExecutionPipelineIntegrationTest.redeliveringTheSameExecutionDoesNotMoveThePositionTwice`, which
publishes the identical execution three times and asserts the position moved once.

## Scenario: events arrive after a restart

Consumers start from the **earliest** offset (`ConsumerConfig.AUTO_OFFSET_RESET_CONFIG = earliest`)
and commit is per-record. A restarted consumer either resumes from its last committed offset (the
normal case) or, for a brand-new consumer group, replays the entire topic from the start. Either
way, every execution is eventually applied exactly once, because the idempotency guarantee does not
depend on where in the topic the consumer happens to be — only on whether that execution id already
has a row.

This is also how the **risk engine's** positions survive a gateway restart, closing the gap Stage 3
left open: `RiskStateUpdater` replays `trading.executions.v1` from the beginning and calls the same
`recordFill(accountId, executionId, ...)` the gateway calls synchronously, which is idempotent per
execution and side (see `docs/position-calculation.md`). `ReplayReadiness` holds the gateway's
health check `OUT_OF_SERVICE` until that replay has caught up, so a restarted instance cannot admit
an order against a half-rebuilt risk position.

## Scenario: the portfolio service is temporarily unavailable

If "portfolio service" means the database: covered above. If it means the consumer process itself
is down (deployment, crash, planned maintenance): records simply queue up in Kafka. Nothing is lost
— the topic is the durable log — and consumer lag rises for exactly as long as the consumer is down.
`PipelineFailureIntegrationTest.aBacklogIsWorkedThroughRatherThanDropped` publishes a burst of 300
records with no consumer actively draining them and confirms the backlog is fully applied once
processing resumes; the contract is that the consumer catches up, not that it never falls behind.

## Reconciliation: rebuilding derived state from history

Positions are **derived** state — every one of them is a fold over the executions that produced it
— and `executions` is treated as the record of truth while `positions` is treated as a durable
cache. `PositionReconciliation` makes that relationship checkable and repairable:

* **`check()`** replays the entire `executions` table in `execution_id` order (never by timestamp —
  two trades can share a timestamp but never an id), rebuilds every position from scratch in memory,
  and diffs the result against what is stored. It changes nothing; it only reports.
* **`repair()`** does the same rebuild, then deletes every stored position and reinserts the
  recomputed ones, inside one transaction. It is a deliberately separate operation from `check()` —
  repairing derived state is an operational decision with a "before" report attached, not something
  a health check should do on its own.

Execution order matters for the rebuild: weighted-average-cost accounting is path-dependent, so
replaying trades out of order produces a different, wrong average. Execution ids come from the
matching engine's per-worker disjoint sequences (see ADR-007 in the Stage 4 docs), which is why they
are the replay order rather than `occurred_at`.

Reconciliation exists for the case where "the code is idempotent" is a claim, not a guarantee: a
hand-edited row, a restore from an older backup, a bug in the fold, or a trade that reached
Postgres through some path other than `PortfolioUpdater`. `PositionReconciliationTest` covers a
position that drifted to a wrong value, a position missing entirely, and the case where nothing has
drifted at all.

### What reconciliation does not solve

It rebuilds from `executions`, so it cannot repair damage to `executions` itself — a row deleted or
corrupted there is a real, permanent loss of trade history, not something a replay can recover.
Guarding the durability of `executions` (backups, retention, replication) is an operational concern
outside this module's scope.

## Known limitations

* Reconciliation is invoked programmatically today (`PositionReconciliation.check()` /
  `.repair()`), not exposed over HTTP or run on a schedule. Wiring it into an operational surface —
  a scheduled check with alerting on drift, an authenticated admin endpoint for `repair()` — is
  future work.
* `repair()` locks nothing beyond the transaction it runs in. Running it while trades are actively
  being applied by `PortfolioUpdater` is safe (both are transactional and idempotent), but a
  `repair()` and a live trade landing in the same instant means the trade could be counted in the
  rebuild or not, depending on commit order — either outcome is correct, but the report's numbers
  are a snapshot, not a guarantee about what happens a moment later.
* Order books now survive a restart too, via the matching-engine journal —
  see [ADR-015](architecture-decisions/ADR-015-matching-engine-durability.md) and
  [Replay and reconciliation](replay-and-reconciliation.md). Gateway-side idempotency registrations
  and rate-limit buckets still do not; see the gateway API docs' known-limitations section.
