# ADR-010: One transaction per execution, keyed on its primary key

* Status: Accepted
* Date: 2026-08-19

## Context

Stage 4 gave every trade a durable event on Kafka. Stage 4 also gave every consumer at-least-once
delivery: a producer retry, a consumer crash before its offset commits, or an operator replay can
all present the same execution more than once. Applying a fill is not naturally idempotent — it is
an addition to a running total — so *something* has to make "apply this execution" safe to call an
arbitrary number of times with the same argument.

The database write must also stay off the matching path. The matching engine has no Kafka
dependency and no database dependency; nothing about Stage 5 changes that.

## Decision

**JDBC over an ORM.** The write path is a handful of deliberate SQL statements whose conflict
behaviour *is* the correctness mechanism. Hiding that behind an ORM's generated SQL would make the
thing that matters most about this code the least visible part of it. `PortfolioRepository` is
every statement, in one place, readable together.

**One transaction per execution: insert the trade, move both positions.**

```java
@Transactional
public boolean apply(ExecutionCreated execution) {
    if (!repository.recordExecutionIfAbsent(execution)) {
        return false;                 // already applied — nothing else runs
    }
    applyLeg(buyKey, BUY, execution);
    applyLeg(sellKey, SELL, execution);
    repository.upsertMarkPrice(...);
    return true;
}
```

`executions.execution_id` is the primary key, and the insert is `ON CONFLICT (execution_id) DO
NOTHING`. A zero-row result *is* the duplicate check — there is no separate "have I seen this id"
table that could drift from the data it exists to protect. Whether the redelivery is a producer
retry, a post-crash re-consume before an offset commit, or an operator replaying part of the topic,
it is the identical code path: the insert reports nothing changed, and the method returns without
touching either position.

**Positions are read with `SELECT ... FOR UPDATE` inside the same transaction.** This is not needed
for ordinary consumption — Kafka's symbol-keyed partitioning already gives one writer per position
under normal operation, the same guarantee the matching engine relies on (see the Stage 4 ADR on
partitioning). It is needed because reconciliation's `repair()` is a *second* writer, and the row
lock is what stops a live trade and a rebuild from interleaving two read-modify-write cycles into a
wrong number.

**Self-trades apply both legs.** An account can be both the buyer and the seller of one execution.
The two legs are applied sequentially in the transaction, so the second read observes the first
write, and the position nets correctly rather than the second leg looking like a duplicate of the
first. This needed a real fix during development — the naive version deduplicated by execution id
alone and silently dropped one leg — recorded in
[docs/position-calculation.md](../position-calculation.md).

**Only the database write is transactional and durable; risk stays synchronous and in-memory.**
`PreTradeRiskEngine` still applies fills on the gateway request thread, unchanged from Stage 3. A
pre-trade decision has to see the fill that just happened, not the one that will arrive after a
Kafka round trip, and putting the database in front of that decision was ruled out in ADR-003 for
exactly that reason. The two paths share the idempotency key (`accountId, executionId, side`) so
they converge rather than double-count; see docs/position-calculation.md for how that is proven.

**Failures are classified before they are handled.** A write that fails because the record is
impossible (already ruled out upstream, so this should not happen, but defensively: negative
quantity, missing account) is a bug, not an outage, and is handled by the ordinary bounded-retry
path from ADR-009. A write that fails because Postgres itself could not be reached is wrapped as
`DownstreamUnavailableException` and retried without limit — see
[docs/recovery-model.md](../recovery-model.md) for the full reasoning and the test that proves it.

## Consequences

* A trade and its position effects can never disagree: they commit together or not at all.
* Redelivery of any kind — crash, retry, or deliberate replay — is one `ON CONFLICT` clause away
  from being a no-op, proven by `PortfolioUpdaterTest`'s concurrent-application test and
  `ExecutionPipelineIntegrationTest`'s redelivery test.
* The row lock on `positions` means two writers to the *same* position key serialize. Under normal
  operation there is only ever one (the partition-owning consumer), so this costs nothing in
  practice; it only matters when reconciliation's `repair()` runs concurrently with live trading.
* A database outage stalls the portfolio consumer for its partitions until the database returns.
  That is an accepted, deliberate trade-off: a stalled consumer is visible in lag and recoverable by
  fixing the dependency, which is strictly better than the alternative of a permanently missing
  trade.
* Hand-writing SQL means every new query is a deliberate choice, including its index. Every index
  in `V1__portfolio_schema.sql` has a comment explaining the query it exists for.

## Alternatives considered

* **A separate `processed_execution_ids` table:** the same idempotency property, but as a second
  fact that has to be kept in lockstep with the data it protects, updated in the same transaction
  either way — no simpler, and one more thing that can drift.
* **Exactly-once Kafka-to-Kafka semantics (transactions):** solves duplicate delivery between Kafka
  topics, not between Kafka and Postgres, and adds coordinator overhead for a guarantee the primary
  key already provides here.
* **An ORM (JPA/Hibernate) with optimistic locking:** more familiar to some teams, but optimistic
  locking turns concurrent writers into retried exceptions rather than a lock that simply waits, and
  the generated SQL would obscure exactly the conflict behaviour this ADR is about.
* **Writing positions from the gateway's synchronous risk path instead of from Kafka:** rejected in
  ADR-003 for the matching-path-latency reason restated above, and it would remove the durability
  property (a database write on the request path is not free to fail without failing the request).
