# ADR-008: Idempotency through derived event ids and idempotent state

* Status: Accepted
* Date: 2026-08-18

## Context

Kafka redelivers. A consumer that crashes after applying a record but before committing its offset
sees that record again. A rebalance can hand a partition to another instance mid-batch. A producer
retry can append the same fact twice. None of these are faults; they are the normal operation of an
at-least-once system, and every consumer has to be correct under them.

Two of the four consumers accumulate rather than overwrite. A position is a running total, so
applying one execution twice is not a duplicate write — it is a wrong number.

## Decision

**Event ids are derived from the fact, not generated.** `exe-<executionId>`, `acc-<orderId>`,
`can-<orderId>`, `fil-<orderId>`, `rep-<orderId>-<prioritySequence>`. Each is a pure function of
identifiers the matching engine already guarantees unique. A random UUID would make a redelivery
indistinguishable from a second, genuinely new fact — the consumer would have no way to tell them
apart, and no amount of deduplication logic could recover the difference.

**State is idempotent where it can be, deduplicated where it cannot.**

* `ExecutionStore` is keyed by execution id, so recording a redelivery is a `putIfAbsent` that does
  nothing. There is no separate "seen" table to drift out of step with the data.
* `PositionStore` and the risk engine accumulate, so each position remembers the execution ids it
  has absorbed, and the check happens *inside the same lock as the update*. Checking first and
  updating afterwards leaves a window in which two concurrent redeliveries both pass the check.
* Buys and sells are tracked in separate sets. An account can be both sides of one execution, and a
  single set keyed on execution id alone would treat the second leg as a duplicate and book half a
  self-trade.

**Duplicates are counted, not ignored.** `trading.consumer.duplicates` rises when redelivery
happens. Correctness is unaffected, but a consumer that suddenly starts seeing duplicates is usually
one that is rebalancing repeatedly or failing to commit, and that is worth an alert.

**Offsets are committed per record** (`AckMode.RECORD`) with auto-commit off. Processing is
idempotent, so at-least-once is safe either way; per-record commits simply make the redelivery
window after a crash as small as it can be.

**Consumers start from the earliest offset.** A projection that began at the tail would hold a
confidently wrong position. Replaying from the start is also what rebuilds risk positions after a
restart, which is why `ReplayReadiness` keeps the gateway out of service until the risk consumer has
caught up.

**The synchronous path and the replay path share the same idempotency key.** The gateway applies a
fill to the risk engine immediately, because an admission decision cannot wait for a broker round
trip, and the `RiskStateUpdater` applies the same fill from the stream. Both call
`recordFill(accountId, executionId, ...)`, which applies at most once per execution and side, so the
two converge instead of doubling. In steady state the consumer is a no-op that reports duplicates;
after a restart it is the thing that rebuilds the state.

## Consequences

* Redelivery is provably harmless: an integration test publishes the same execution three times and
  asserts the position does not move.
* The gateway can be restarted without losing positions, which was the largest gap left by Stage 3.
* **Deduplication sets grow without bound.** Every position holds every execution id it has ever
  absorbed. This is the main cost of the approach and is not solved here — Stage 5 moves the check
  to a database primary key, and Stage 7's snapshot-and-prune bounds what has to be remembered.
* The audit log's deduplication only holds inside its bounded window; a redelivery arriving after
  its entry was evicted is appended again. Acceptable because it is a debugging window, not the
  audit record of truth — Kafka is.
* Idempotency is per process. Two gateway instances would each maintain their own sets and their own
  projections, which is another reason Stage 4 remains single-instance.

## Alternatives considered

* **Exactly-once semantics with Kafka transactions:** removes duplicates at the broker, but only for
  Kafka-to-Kafka flows, and does nothing for the synchronous risk path or a database write. It also
  adds coordinator overhead on the trade path for a property that idempotent state already provides.
* **A shared table of processed event ids:** one mechanism for every consumer, but a side table can
  drift from the state it is supposed to protect, and it needs its own transaction with the update
  to be correct.
* **Trusting offsets:** simplest, and wrong. Offsets are committed after processing, so a crash in
  between guarantees reprocessing.
* **Generated UUID event ids:** conventional, but destroys the ability to recognise a republished
  fact, which is the property the whole design rests on.
