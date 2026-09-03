# Architecture

This is the one-page view: what the platform is made of, how a request actually moves through it,
and why each technology choice is what it is. Every per-topic doc it cross-links owns the detail —
this page synthesizes and points outward rather than repeating any of them, the same way
`failure-matrix.md` synthesizes failure scenarios instead of re-deriving them.

## The whole platform

```text
                         STRATEGY / CLIENT
                                 │
                 ┌───────────────┴────────────────┐
                 ▼ HTTP/JSON, HTTP/Protobuf         ▼ TCP + binary
        ╔═══════════════════════════════════════════════╗
        ║                trading-gateway                 ║
        ║  auth → rate limit → validate → idempotency     ║
        ║  → pre-trade risk (risk-engine) → route          ║
        ╚═══════════════════════╤═════════════════════════╝
                                 ▼
                      matching-engine.OrderRouter
                      hash(symbol) → single-writer worker
                 ┌───────────────┼────────────────┐
                 ▼                ▼                ▼
            Worker 1          Worker 2          Worker N
         MatchingEngine    MatchingEngine    MatchingEngine
         + local journal   + local journal   + local journal
                 │                │                │
                 └───────────────┴────────────────┘
                                 ▼
                    execution-pipeline (async, buffered)
                                 ▼
                   ┌─────────────┴─────────────┐
                   ▼                             ▼
         trading.orders.v1              trading.executions.v1
                   │                             │
                   ▼                             ▼
          AuditProcessor            ┌────────────┼────────────┐
        (both topics; the           ▼            ▼            ▼
         only consumer of    RiskStateUpdater  ExecutionProcessor  PortfolioProcessor
         both)               (risk-engine's    (execution-store,   (Postgres: executions
                              durable-restart   in-memory dedup)     + positions, one
                              reconciliation)                        transaction each)
                                                                            ▼
                                                                       PostgreSQL

  exchange-simulator ──► market-data (sequencing, gap detection, normalization)
                                 │
                                 ▼
                    WebSocket hub, one queue per subscriber
                                 │
                                 ▼
                          STRATEGY / CLIENT
```

Two independent flows share the diagram: the order/execution path down the left-and-center (a
client's own trades), and the market-data feed at the bottom (a read-only, unauthenticated view of
the whole market). They meet only at `MarkPriceUpdater`, which polls market-data's latest state on
a schedule and writes changed reference prices into `mark_prices` — the mechanism unrealized P&L
uses to move between trades. See [Market data](market-data.md).

## Implemented modules

* **`trading-domain`** — commands, order states, snapshots, rejection reasons, immutable
  executions. Records and enums only, no branching logic; every other module depends on it.
* **`matching-engine`** — synchronous, deterministic price-time-priority matching
  (`MatchingEngine`), single-writer-per-book routing (`OrderRouter`), and an append-only per-worker
  journal (`FileCommandJournal`) that recovers a worker's book after a restart. See
  [Matching algorithm](matching-algorithm.md), [Concurrency model](concurrency-model.md).
* **`risk-engine`** — in-memory pre-trade risk checks with reserve-and-settle exposure accounting,
  rebuilt after a restart by replaying `trading.executions.v1` rather than its own durable store.
* **`market-data`** — a sequenced, gap-aware market-data processor and a non-blocking
  per-subscriber fan-out hub, independent of Spring, Kafka and the OMS domain. See
  [Market data](market-data.md).
* **`execution-pipeline`** — immutable trading events (`TradingEvent` and its six record
  implementations), a buffered Kafka producer (`BufferedKafkaEventPublisher`), and the downstream
  consumers with idempotency, retry and dead-lettering. See [Kafka design](kafka-design.md).
* **`portfolio-service`** — exact-arithmetic position/P&L state, a transactional Kafka consumer
  (`PortfolioProcessor`) that writes it to Postgres, and reconciliation that rebuilds it from trade
  history. See [Position calculation](position-calculation.md).
* **`trading-gateway`** — Spring Boot order ingress (HTTP/JSON, HTTP/Protobuf, persistent TCP),
  authentication, rate limiting, idempotency, routing, the portfolio read API, the market-data
  WebSocket/REST endpoints, and the Prometheus metrics bridge. See
  [Gateway API](gateway-api.md), [Observability](observability.md).
* **`exchange-simulator`** — deterministic and concurrent generated order-flow smoke/stress
  runners, plus a market-data feed generator, all seeded for reproducibility.
* **`benchmark`** — JMH microbenchmarks and `LoadTestRunner`, a concurrent load generator driving
  named traffic-shape workloads against the real router or a running gateway. See
  [Benchmark methodology](benchmark-methodology.md).

The Master-Prompt's originally proposed layout also named `persistence/`, `observability/` and
`integration-tests/` as top-level modules. None exist as separate Maven modules today, and that is
an intentional, considered deviation rather than an unfinished one: persistence lives inside
`portfolio-service` (the module that owns the durable state it persists), observability's wiring
lives in `trading-gateway` and `docker/` (the process that actually reports metrics and the compose
stack that scrapes them), and integration tests live alongside the module whose integration they
prove (`trading-gateway/src/test`, `portfolio-service/src/test`) rather than in a module of their
own. Splitting these out would not have changed what the system does, only how many `pom.xml`
files describe it.

## Request and execution flow

The gateway's own request pipeline — auth, rate limit, validation, idempotency, risk, routing — is
[Gateway API](gateway-api.md)'s diagram, not repeated here. Once a command reaches
`OrderRouter`, [Concurrency model](concurrency-model.md) owns per-symbol ordering and worker
ownership, and [Order lifecycle](order-lifecycle.md) owns the state machine a single order moves
through. What happens after a match — execution events, Kafka, downstream consumers — is
[Kafka design](kafka-design.md)'s.

## Why these technology choices

* **Kafka as the event backbone, not the matching path** — durable, ordered, replayable
  distribution to independent consumers that must each see every fact exactly once; never in the
  latency-sensitive submit/match/acknowledge loop. See [ADR-006](architecture-decisions/ADR-006-topic-design.md),
  [ADR-007](architecture-decisions/ADR-007-partitioning-strategy.md).
* **PostgreSQL for durable positions, off the matching path** — transactional, indexed, queryable
  state that survives a restart, written from the async consumer side only. See
  [ADR-010](architecture-decisions/ADR-010-persistence-architecture.md).
* **Single-writer-per-book over locking** — one thread owns a symbol's book for its lifetime, so
  price-time priority needs no lock at all on the hot path. See
  [ADR-002](architecture-decisions/ADR-002-single-writer-order-books.md).
* **Event ids derived from the fact, not generated** — the entire idempotency strategy rests on
  redelivery producing the same id twice rather than a new one. See
  [ADR-008](architecture-decisions/ADR-008-consumer-idempotency.md),
  [ADR-004](architecture-decisions/ADR-004-idempotency-strategy.md).
* **A local append-only journal for matching-engine durability** — the order book is
  process-local state with no reason to leave the process, so its source of truth is a file next
  to it, not a network call away. See [ADR-015](architecture-decisions/ADR-015-matching-engine-durability.md).

## Sequence diagrams

Each diagram below is grounded in a specific test or doc — check the citation under each rather
than treating the picture as aspirational.

### 1. Order submission

```text
Client          Gateway              RiskEngine      OrderRouter      MatchingEngine
  │  POST /orders   │                     │                │                │
  ├────────────────►│                     │                │                │
  │                 │ authenticate,       │                │                │
  │                 │ rate limit,         │                │                │
  │                 │ validate,           │                │                │
  │                 │ OrderRegistry.reserve                │                │
  │                 ├────────────────────►│                │                │
  │                 │ reserve exposure    │                │                │
  │                 │◄────────────────────┤                │                │
  │                 ├──────────────────────────────────────►│                │
  │                 │              route (hash by symbol)   │                │
  │                 │                     │                ├───────────────►│
  │                 │                     │                │  submit()      │
  │                 │                     │                │◄───────────────┤
  │                 │                     │                │  CommandResult │
  │                 │◄──────────────────────────────────────┤                │
  │                 │ settle risk state, publish event      │                │
  │◄────────────────┤                     │                │                │
  │  201 Created     │                     │                │                │
```

Grounded in [Gateway API](gateway-api.md)'s own request-pipeline diagram and
`ExecutionPipelineIntegrationTest.aTradeReachesEveryDownstreamProjectionIncludingPostgres`
(trading-gateway), which drives this exact path over real HTTP.

### 2. Successful execution

```text
MatchingEngine        execution-pipeline         Kafka                    Consumers
  │ two orders cross      │                        │                          │
  │  produces Execution   │                        │                          │
  ├───────────────────────►│                        │                          │
  │                        │ ExecutionCreated,      │                          │
  │                        │ eventId = "exe-<id>"   │                          │
  │                        ├───────────────────────►│ trading.executions.v1    │
  │                        │                        │  key = symbol            │
  │                        │                        ├─────────────────────────►│ RiskStateUpdater
  │                        │                        ├─────────────────────────►│ ExecutionProcessor
  │                        │                        ├─────────────────────────►│ PortfolioProcessor
  │                        │                        │                          │  1 Postgres txn:
  │                        │                        │                          │  execution + both
  │                        │                        │                          │  legs' positions
```

Grounded in `MatchingEngineTest.fullyFillsCrossingLimitOrdersAtRestingPrice` (the match itself) and
the same `ExecutionPipelineIntegrationTest`, which asserts every consumer converges. See
[Kafka design](kafka-design.md).

### 3. Duplicate request

```text
Producer                   ExecutionStore (execution-processor)
  │  ExecutionCreated,          │
  │  eventId = "exe-42"         │
  ├─────────────────────────────►│ putIfAbsent(42, execution) → absent, recorded
  │                              │
  │  same record, redelivered   │
  ├─────────────────────────────►│ putIfAbsent(42, execution) → already present, no-op
```

The same shape repeats at every consumer keyed by the execution id (`RiskStateUpdater` via
`PreTradeRiskEngine.recordFill`'s at-most-once apply, `PortfolioProcessor` via Postgres's own
primary key). Grounded in
`ExecutionPipelineIntegrationTest.redeliveringTheSameExecutionDoesNotMoveThePositionTwice` and
`ExecutionsDedupTest` (execution-pipeline, unit-level). See
[ADR-004](architecture-decisions/ADR-004-idempotency-strategy.md),
[ADR-008](architecture-decisions/ADR-008-consumer-idempotency.md).

### 4. Kafka failure

```text
MatchingEngine        BufferedKafkaEventPublisher              Kafka
  │  publish(event)         │                                    │
  ├─────────────────────────►│ offer to bounded in-process queue │
  │                          │  (never blocks the caller)        │
  │                          │                                    ✕ unreachable
  │                          │  dispatcher thread retries send()  │
  │                          │  queue keeps filling...            │
  │                          │  queue full → drop + count         │
  │                          │  trading.events.dropped++          │
  │                          │  (KafkaEventsDropped alert fires)  │
```

This diagram deliberately matches a disclosed limitation, not a hoped-for retry path: once the
buffer overflows, events are dropped with no local durable record to replay from — Stage 7's
matching-engine journal covers the order book, not this buffer. See
`failure-matrix.md` row 1, [Observability](observability.md) (`KafkaEventsDropped`,
`EventsPublishFailing`), and `KafkaOutageIntegrationTest`, which proves the *matching* side of this
picture (orders keep being accepted through a 3-second broker pause) without asserting the buffer
never overflows on a longer one.

### 5. Service restart / replay

```text
Process restarts
  │
  ▼  OrderRouter constructor (before accepting any traffic)
     replay each worker's journal ──► book, order statuses, execution-id
                                       sequence identical to pre-crash
  ▼  RiskStateUpdater consumer starts
     replay trading.executions.v1 from the beginning ──► risk positions rebuilt
     gateway reports NOT ready (ReplayReadiness) until this catches up
  ▼  Postgres positions
     already durable — no replay needed, they were committed transactionally
     before the crash
  ▼  gateway's OrderRegistry order-id allocator
     restarts from 1, unconditionally ──► first order that collides with a
     recovered resting order's id fails DUPLICATE_ORDER_ID (reproduced, not
     hypothetical)
```

Grounded in `OrderRouterRecoveryTest`, `MatchingRestartIntegrationTest`,
`ExecutionPipelineIntegrationTest.consumersReportThemselvesCaughtUpSoTheGatewayCanBecomeReady`, and
[Disaster recovery](disaster-recovery.md), which states the `DUPLICATE_ORDER_ID` gap in the same
terms. See [ADR-015](architecture-decisions/ADR-015-matching-engine-durability.md).
