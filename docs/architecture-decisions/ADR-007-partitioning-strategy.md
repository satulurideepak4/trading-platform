# ADR-007: Partition by symbol

* Status: Accepted
* Date: 2026-08-18

## Context

The partition key decides the only ordering guarantee Kafka offers. Records with the same key go to
the same partition and are consumed in the order they were produced; records with different keys
have no relative order at all. Choosing the key is therefore choosing which sequence downstream
consumers are allowed to rely on.

Three candidates were plausible: the symbol, the order id, and the account id.

## Decision

**Every event is keyed by its symbol.**

Three reasons, in order of how decisive they are.

**1. It is the only key that is well defined for every event.** An execution has two accounts — a
buyer and a seller — and no single account id can be its key without arbitrarily privileging one
side. Keying trades by the buyer and lifecycle events by the owner would put the two sides of one
trade on different partitions and give them no relative order. Every event, including an execution,
belongs to exactly one instrument.

**2. It matches the ordering the matching engine already guarantees.** Stage 2 gives each order book
a single writer: one worker owns a symbol and produces its events in a total order. Keying by symbol
maps that existing sequence onto one partition, so the order a symbol's events are appended to the
log is the order the engine produced them. Any other key would take an ordering the system already
has and discard it.

**3. It is the sequence consumers actually need.** Position updates are additions per account and
instrument, and additions commute — the arrival order of two fills for one account in different
instruments does not matter. What does matter is the fill sequence within an instrument, which is
exactly what this key preserves.

Order id was rejected because it gives per-order ordering, which nothing needs beyond what symbol
already provides, while multiplying the key cardinality by the order count and losing any notion of
book-level sequence. Account id was rejected on point 1 alone.

## Consequences

* End-to-end ordering per instrument: engine → partition → consumer, with no reordering anywhere in
  between. Combined with an idempotent producer, which preserves per-partition order even with
  several requests in flight, this holds without limiting the producer to one in-flight request.
* **A hot symbol makes a hot partition.** Consumption of one instrument cannot be parallelised, and
  a single very active instrument caps downstream throughput. This is the same skew the matching
  workers have, for the same reason, and that is deliberate: the bottleneck appears in one place
  rather than in two unrelated ones.
* Consumer parallelism is bounded by partition count, not by instrument count. Instruments beyond
  the partition count share partitions and therefore share ordering they do not need — harmless, but
  it means adding instruments does not add parallelism.
* Partition count cannot be reduced, and increasing it re-hashes keys to different partitions.
  Ordering is broken for any key that moves, so a partition increase needs a quiet period or a
  deliberate cutover, not a live config change.
* Consumers must not assume order between two instruments. Nothing does today, and a consumer that
  needed it — a cross-instrument strategy, say — would need its own sequencing rather than relying
  on Kafka.

## Alternatives considered

* **Order id:** finer-grained parallelism, but an execution names two orders, so it has the same
  "which one is the key" problem as the account, and per-order ordering is strictly weaker than what
  symbol already gives.
* **Account id:** matches how positions are stored, but is undefined for an execution and would
  split the two legs of one trade across partitions.
* **No key (round robin):** perfectly even load and no hot partition, but no ordering whatsoever,
  which would make every consumer responsible for reordering fills before applying them.
* **Composite `symbol:account`:** finer distribution, but two events for one instrument could then
  be consumed out of order, which is the guarantee being bought here.
