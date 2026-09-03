# Consistency model

This is a synthesis, not a new mechanism: every guarantee below is already implemented and
documented elsewhere (`concurrency-model.md`, `kafka-design.md`, `recovery-model.md`,
`position-calculation.md`, and the ADRs each of those cites). What's missing before this document
is one place that states, in one pass, what ordering holds, what idempotency holds, and which
pieces of state are strongly vs. eventually consistent — the question a new reader actually asks
first, that no single per-topic doc answers by itself.

## Ordering guarantees

**Per symbol: total order, by design.** `OrderRouter` hashes each symbol to exactly one worker for
the router's lifetime, and that worker processes its queue FIFO. Every command for one symbol —
across every client, every account — is therefore totally ordered. There is deliberately **no
global order across independent instruments**: two different symbols owned by two different
workers may complete in either order, because nothing in the domain needs them to agree on one.
Each worker also uses a disjoint execution-id sequence (`worker i` emits `i+1`, `i+1+N`, ...), so
ids never collide across workers without a shared counter. See
[Concurrency model](concurrency-model.md).

**Per Kafka partition: preserved, by the same key everywhere.** Every event's Kafka key is its
symbol (see [ADR-007](architecture-decisions/ADR-007-partitioning-strategy.md)), so one symbol's
events always land on one partition, in the order `BufferedKafkaEventPublisher`'s single dispatcher
thread handed them to the producer — the same order the matching engine produced them, combined
with an idempotent producer that preserves per-partition order even with several requests in
flight. A consumer reading one partition therefore sees one symbol's history in the order it
actually happened. See [Kafka design](kafka-design.md).

## Idempotency guarantees

Every event id is **derived from the fact it describes**, never generated — the entire scheme
rests on this one property (redelivery reproduces the same id; a random id would make a
redelivery indistinguishable from a new fact):

| Event | Id | Unique because |
| --- | --- | --- |
| OrderAccepted | `acc-<orderId>` | an order is accepted at most once |
| OrderRejected | `rej-<orderId>` | the gateway allocates a fresh order id per rejection |
| OrderCancelled | `can-<orderId>` | an order reaches CANCELLED at most once |
| OrderReplaced | `rep-<orderId>-<prioritySequence>` | the engine's priority sequence increases per replacement |
| ExecutionCreated | `exe-<executionId>` | per-worker disjoint sequences, unique platform-wide |
| OrderFilled | `fil-<orderId>` | an order reaches FILLED at most once |

Every consumer turns that id into its own at-most-once apply, each against the state it alone owns:

| Consumer group | Idempotent by |
| --- | --- |
| `execution-processor` | execution id is the primary key of `ExecutionStore` |
| `position-processor` (`PortfolioProcessor`) | execution id is the Postgres primary key, `ON CONFLICT DO NOTHING` |
| `audit-processor` | event id, within the bounded audit window |
| `risk-state-updater` | execution id and side, per account, in `PreTradeRiskEngine.recordFill` |

The gateway itself applies the same fill to risk **synchronously**, using the same
`recordFill(executionId, ...)` call the Kafka-driven `risk-state-updater` uses — both paths call it
with the same execution id, so the two converge instead of double-counting, whichever happens
first. That's also why `trading.consumer.duplicates` is expected to be nonzero in normal
operation: the Kafka path replaying a fill the gateway already applied synchronously is not a bug,
it's the mechanism working. See [ADR-004](architecture-decisions/ADR-004-idempotency-strategy.md),
[ADR-008](architecture-decisions/ADR-008-consumer-idempotency.md).

## Transactional guarantees

`PortfolioUpdater.apply` inserts the execution row and moves both sides' positions in **one
Postgres transaction** — "the execution is recorded" and "the positions include it" cannot
disagree, because they're the same commit. Nothing else needs its own transaction: the matching
engine's journal append happens before the caller's result is returned (not inside a database
transaction at all — it's a local file, not Postgres), and the risk engine's synchronous apply is
an in-memory call with no I/O to fail mid-way. See [Recovery model](recovery-model.md),
[ADR-010](architecture-decisions/ADR-010-persistence-architecture.md).

## What consistency model applies where

| State | Consistency | Bound |
| --- | --- | --- |
| Matching engine's own book (in-process) | Strong — the single worker thread is the only writer | Immediate; no propagation delay because nothing is copied |
| Matching engine's journal (local disk) | Strong for any command the caller saw succeed | `FlushPolicy.EVERY_RECORD` fsyncs before the caller's future completes |
| Risk engine's synchronous view (gateway process) | Strong for the fill that just happened, eventually consistent with Kafka's copy | Immediate for the synchronous apply; the Kafka-driven `risk-state-updater` converges on redelivery, at-most-once, usually within milliseconds |
| Postgres positions/executions (`portfolio-service`) | Eventually consistent with the matching engine's outcome | Bounded by Kafka consumer lag plus one transaction; every test in this codebase that waits for it uses a 20-second Awaitility window as an empirical upper bound for a healthy system, not a guarantee the system makes |
| Market-data feed state | Eventually consistent, independently of the order/execution path entirely | Bounded by the market-data processor's own sequencing, not discussed further here — see [Market data](market-data.md) |

The 20-second figure above is a test-suite convention, not a documented SLA — `benchmark`'s
`LoadTestRunner` and `docs/performance-engineering.md` are where an actual measured latency number
would live, and none of Stage 9's numbers claim an end-to-end HTTP-to-Postgres bound.

## Known limitations

Pulled from the same disclosures `README.md`'s "Known limitations carried into later stages"
section already makes, restated here only where they bear on consistency specifically:

* The risk engine's and the audit log's execution-id dedup sets grow without bound in memory —
  idempotency itself is correct, but the bookkeeping that makes it correct is not durable or
  bounded.
* Gateway idempotency registrations and rate-limit buckets do not survive a restart; a retry
  racing that exact window can still duplicate an order despite every guarantee above holding once
  a command is admitted.
* Replay readiness (`ReplayReadiness.isCaughtUp`) is inferred from a consumer going idle, which a
  genuinely quiet topic also looks like — it errs safe, but it is a signal, not a proof.
* If `BufferedKafkaEventPublisher`'s buffer overflows during a long broker outage, the ordering and
  idempotency guarantees above are moot for the events that were dropped — there is no local
  durable record of what was lost. See `failure-matrix.md` row 1.
