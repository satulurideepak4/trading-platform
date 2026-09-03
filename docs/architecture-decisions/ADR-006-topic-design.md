# ADR-006: Two topics, split by what consumers need rather than by event type

* Status: Accepted
* Date: 2026-08-18

## Context

Stage 4 introduces six events: `OrderAccepted`, `OrderRejected`, `OrderCancelled`, `OrderReplaced`,
`ExecutionCreated` and `OrderFilled`. They have to be organised into topics before anything is
published, and topic layout is close to irreversible once producers and consumers exist.

The choice interacts with ordering. Kafka guarantees order within a partition, and nothing across
topics. Splitting two events that a consumer must see in order into separate topics silently removes
a guarantee that no amount of consumer code can restore.

## Decision

Two topics:

```text
trading.orders.v1        OrderAccepted, OrderRejected, OrderCancelled, OrderReplaced, OrderFilled
trading.executions.v1    ExecutionCreated
```

plus a dead-letter topic for each (`.dlq`), with the same partition count as its source.

**Executions are separated because they are what downstream state is derived from.** Positions, P&L
and risk are functions of trades, not of order lifecycle. Keeping the trade stream on its own topic
means those consumers read only what they need, and can be scaled, replayed, retained and audited
independently of the far chattier lifecycle stream — every order produces an accept or a reject,
while only matches produce trades.

**Lifecycle events stay together because they order against each other.** `OrderAccepted` then
`OrderCancelled` for the same order must be observed in that order, and putting them in one topic
with the same key is what makes that true.

**`OrderFilled` lives with the lifecycle events, not with the executions**, because it is an order
state transition rather than a trade. The money moved in the executions; this is the announcement
that an order is done.

**Nothing needs cross-topic ordering.** No consumer reads both and depends on their relative order:
the execution-derived consumers read only executions, and the audit consumer records both without
inferring anything from their interleaving. That is what makes the split safe, and it is the
constraint to check before adding a consumer.

**Versioned names.** Adding a field is backwards compatible and stays on `v1`. A change a consumer
cannot ignore requires `v2`, which old and new consumers can straddle during a migration; renaming
in place would need every consumer to deploy simultaneously.

## Consequences

* A consumer that needs a total order over everything that happened to one order cannot get it from
  one topic. Nothing needs that today; a consumer that did would have to read both topics and merge
  on `occurredAt`, which is a real cost this design accepts.
* Two topics mean two retention and two partition-count decisions. Executions likely want longer
  retention than lifecycle events, since they are what state is rebuilt from — which is an argument
  for the split rather than against it.
* The DLQ mirrors the partition count so the recoverer can preserve the partition index and keep a
  key's failures together with the topic they came from.
* Partition count cannot be lowered later, and raising it changes which partition a key lands on,
  breaking ordering for keys that move. Twelve is the default here: enough headroom to add consumer
  instances, small enough to stay readable.

## Alternatives considered

* **One topic for everything:** gives a total order per key across all event types and is simplest
  to reason about, but forces every consumer to deserialize and skip the majority of traffic, and
  couples the retention of trades to the retention of order chatter.
* **One topic per event type:** maximum consumer selectivity, but destroys ordering between events
  that describe the same order, which is the guarantee hardest to get back.
* **A topic per instrument:** natural partitioning, but thousands of topics is a metadata and
  rebalance problem, and the same effect is available through partitions on one topic.
