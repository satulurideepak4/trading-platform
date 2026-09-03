# Kafka event pipeline

Kafka is the asynchronous backbone for everything that happens *after* a trade. It is deliberately
not involved in producing one: the matching engine has no Kafka dependency, and the gateway's
admission decision does not wait for a broker.

## Flow

```text
HTTP request
     │
     ▼  gateway: auth, rate limit, idempotency, pre-trade risk
     ▼  matching worker  ── owns the book, never touches Kafka
     │
     ├─► synchronous: risk state settled, HTTP response returned
     │
     └─► events offered to a bounded in-process queue (non-blocking)
              │
              ▼  single dispatcher thread
              ▼  idempotent producer, acks=all
     ┌────────┴─────────┐
     ▼                  ▼
trading.orders.v1   trading.executions.v1
     │                  │
     │        ┌─────────┼──────────┬──────────────┐
     ▼        ▼         ▼          ▼              ▼
  audit    audit    execution   position    risk-state
processor processor processor  processor     updater
                        │           │             │
                   ExecutionStore  PositionStore  PreTradeRiskEngine
```

Each processor is its own consumer group, so they progress independently and one falling behind
does not hold up the others.

## Why the producer sits behind a queue

`KafkaTemplate.send` is asynchronous in the normal case, but blocks for up to `max.block.ms` when
the producer's accumulator is full or metadata is unavailable — exactly the conditions a broker
problem creates. A matching worker must not be the thread that discovers that, because it owns
several order books and blocking it stops all of them.

So events cross a bounded `ArrayBlockingQueue` and one dispatcher thread owns every call into the
producer. The dispatcher is single-threaded on purpose: events reach the queue in the order the
workers produced them, and one dispatcher preserves that order into the producer.

This is measured, not assumed. `KafkaOutageIntegrationTest` pauses the broker container for three
seconds, submits orders, and asserts they are still accepted promptly — then that the buffered
events arrive once the broker returns.

**Overflow loses events.** A full queue means the broker has been unreachable long enough to exhaust
both the producer's buffer and this one. Blocking instead would stall matching, so the publisher
drops and counts (`trading.events.dropped`). There is no local durable record to recover from this
specific buffer. Stage 7 built a durable journal, but for a different piece of state — the matching
engine's order book (ADR-015) — not this publish queue; that is still the honest limitation of this
design today. See [Failure matrix](failure-matrix.md), row 1.

## Topics and keys

| Topic | Contents | Key |
| --- | --- | --- |
| `trading.orders.v1` | OrderAccepted, OrderRejected, OrderCancelled, OrderReplaced, OrderFilled | symbol |
| `trading.executions.v1` | ExecutionCreated | symbol |
| `trading.orders.v1.dlq` | records given up on | original key |
| `trading.executions.v1.dlq` | records given up on | original key |

The symbol is the key for every event. An execution has two accounts and one instrument, so the
symbol is the only key well defined for it; it also maps the matching engine's existing per-symbol
ordering straight onto one partition. Full reasoning in [ADR-007](architecture-decisions/ADR-007-partitioning-strategy.md);
topic split in [ADR-006](architecture-decisions/ADR-006-topic-design.md).

## Events

All six are immutable records with an `eventType` discriminator written into the payload, so a
record is self-describing to a consumer that is not this application. Spring's type headers are
switched off for the same reason.

Event ids are **derived from the fact**, never generated:

| Event | Id | Unique because |
| --- | --- | --- |
| OrderAccepted | `acc-<orderId>` | an order is accepted at most once |
| OrderRejected | `rej-<orderId>` | the gateway allocates a fresh order id per rejection |
| OrderCancelled | `can-<orderId>` | an order reaches CANCELLED at most once |
| OrderReplaced | `rep-<orderId>-<prioritySequence>` | the engine's priority sequence increases per replacement |
| ExecutionCreated | `exe-<executionId>` | per-worker disjoint sequences, unique platform-wide |
| OrderFilled | `fil-<orderId>` | an order reaches FILLED at most once |

A random id would make a redelivery indistinguishable from a new fact. See
[ADR-008](architecture-decisions/ADR-008-consumer-idempotency.md).

## Consumers

| Consumer group | Reads | Maintains | Idempotent by |
| --- | --- | --- | --- |
| `execution-processor` | executions | `ExecutionStore` | execution id is the primary key |
| `position-processor` | executions | Postgres, via `PortfolioProcessor`/`PortfolioRepository` | execution id is the Postgres primary key, `ON CONFLICT (execution_id) DO NOTHING` |
| `audit-processor` | both topics | `AuditLog` | event id, within the bounded window |
| `risk-state-updater` | executions | `PreTradeRiskEngine` positions | execution id and side, per account |

`position-processor` and `risk-state-updater` both derive positions and are still separate: one
reports what an account holds, the other decides what it may send next. Different availability
requirements, and in Stage 5 different storage.

### Why the risk updater is a reconciliation path

The gateway applies a fill to the risk engine synchronously, because an admission decision has to
see the fill that just happened rather than the one arriving over a broker in a few milliseconds.
Making Kafka the only writer would put broker latency and broker availability in front of pre-trade
risk.

The consumer exists for the property the synchronous path cannot provide: **positions that survive a
restart.** Both paths call `recordFill(accountId, executionId, ...)`, which applies at most once, so
they converge rather than double-count. In steady state the consumer is a no-op reporting
duplicates; after a restart, replaying the executions topic from the beginning rebuilds what the
gateway lost.

`ReplayReadiness` holds the gateway out of service until that replay has caught up, so a restarted
instance cannot check limits against a half-rebuilt position.

## Failure handling

Permanent failures (`DeserializationException`, `InvalidEventException`) are dead-lettered on the
first attempt. Everything else gets four attempts with exponential backoff from 200ms, capped at 5s,
then goes to the DLQ. Finite on purpose: an unbounded retry is a stopped partition with no alarm.
See [ADR-009](architecture-decisions/ADR-009-retry-and-dead-letter-strategy.md).

## Metrics

Exposed at `/actuator/metrics`.

| Metric | Answers |
| --- | --- |
| `trading.events.published` / `.publish.failed` | is the pipeline keeping up with the engine? |
| `trading.events.dropped` | have we lost events to buffer overflow? |
| `trading.events.queue.depth` | is the publisher backing up? |
| `trading.events.publish.latency` | p50/p95/p99/p99.9 to broker ack |
| `trading.consumer.processing{consumer}` | per-consumer processing percentiles |
| `trading.consumer.applied` / `.duplicates` | how much traffic is redelivery? |
| `trading.consumer.invalid` / `.dead.lettered` | what are we refusing and giving up on? |
| `kafka.consumer.fetch.manager.records.lag.max` | consumer lag, from the Kafka client |

## Tested failure scenarios

All against a real broker via Testcontainers.

| Scenario | Test |
| --- | --- |
| Duplicate delivery | `redeliveringTheSameExecutionDoesNotMoveThePositionTwice` |
| Account on both sides of a trade | `anAccountOnBothSidesOfATradeBooksBothLegs` |
| Sync and replay paths agree | `theRiskProjectionAgreesWithTheGatewaysOwnView` |
| Unparseable record | `anUnparseableRecordIsDeadLetteredAndDoesNotBlockThePartition` |
| Semantically invalid record | `aStructurallyValidButImpossibleRecordIsDeadLettered` |
| Consumer restart | `aRestartedConsumerResumesFromItsCommittedOffsetWithoutDoubleCounting` |
| Consumer group rebalance | `aRebalanceNeitherLosesNorDuplicatesExecutions` |
| Slow consumer / burst | `aBacklogIsWorkedThroughRatherThanDropped` |
| Temporary broker outage | `tradingContinuesWhileTheBrokerIsDownAndTheBacklogDrainsAfterwards` |

## Known limitations

* Publisher buffer overflow loses events, with no local durable record to replay from. Still open —
  Stage 7's journal covers matching-engine state (ADR-015), a different gap than this one.
* Deduplication sets grow without bound in the risk engine's own bookkeeping and the audit log.
  Positions no longer have this problem — Stage 5 moved that check to a Postgres primary key — but
  the other two structures were not migrated and remain open; not the gap Stage 7 closed either.
* Nothing consumes the DLQ, still — records sit there until someone looks. Stage 8 added the alert
  (`EventsDeadLettered`, on `trading_consumer_dead_lettered_total`) so a give-up is no longer
  silent, but nothing automatically reprocesses or even inspects what landed there.
* A dead-lettered execution leaves downstream state permanently short that trade. Reconciliation
  from execution history is Stage 5.
* Consumers run in the gateway process. That is required for the risk projection, which is read on
  the admission path, but it means the pipeline cannot be scaled separately from ingress.
* Replay readiness is inferred from a consumer going idle, which a genuinely quiet topic also looks
  like. It errs safe — a backlog keeps the gateway un-ready — but it is a signal, not a proof.
