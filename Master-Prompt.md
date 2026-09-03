# MASTER CONTEXT PROMPT

You are helping me build a production-grade **Real-Time Electronic Trading Platform** in Java.

This is not a tutorial, CRUD application, toy stock-market application or resume keyword project. Build it as an engineering system that demonstrates the kinds of problems encountered in hedge funds, electronic trading firms, market makers and trading infrastructure teams.

The project should be relevant to engineering roles at firms such as Millennium, IMC, Tower Research, Squarepoint, Qube Research & Technologies, WorldQuant, AlphaGrep, Virtu, Optiver, D. E. Shaw, Point72 and similar companies.

Do not imitate any proprietary system belonging to these companies. Instead implement common electronic-trading infrastructure concepts using sound production engineering.

The core engineering themes of the project are:

* deterministic order processing
* order lifecycle management
* price-time-priority matching
* market-data processing
* pre-trade risk
* execution events
* positions and P&L
* event-driven architecture
* Kafka
* concurrency
* partitioning
* ordering guarantees
* idempotency
* backpressure
* persistence and recovery
* failure handling
* JVM performance
* network performance
* serialization
* observability
* benchmarking
* profiling
* latency percentiles
* graceful degradation
* production-style testing

Use **Java 21**, Maven and Spring Boot where Spring Boot makes sense.

However, do not force Spring Boot into latency-sensitive components simply because it is convenient.

The architecture should distinguish between:

1. latency-sensitive synchronous processing;
2. asynchronous event processing;
3. control-plane operations;
4. persistence;
5. observability.

Prefer a modular monorepo initially instead of creating unnecessary microservices.

The final repository should approximately contain:

```text
trading-platform/
│
├── trading-domain/
├── trading-gateway/
├── matching-engine/
├── market-data/
├── execution-pipeline/
├── risk-engine/
├── portfolio-service/
├── persistence/
├── exchange-simulator/
├── benchmark/
├── observability/
├── integration-tests/
├── docker/
└── docs/
```

The final logical architecture should evolve toward:

```text
                     STRATEGY / CLIENT
                            │
                            ▼
                  ┌───────────────────┐
                  │  Trading Gateway  │
                  │                   │
                  │ Authentication    │
                  │ Validation        │
                  │ Rate limiting     │
                  │ Idempotency       │
                  │ Pre-trade risk    │
                  └─────────┬─────────┘
                            │
                            ▼
                    Order Router
                            │
                   partition by symbol
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
       Engine-1         Engine-2         Engine-N
            │
      In-memory books
            │
      Matching Engine
            │
         Executions
            │
            ▼
       Durable Event Path
            │
            ▼
          Kafka
      ┌─────┼─────────┬──────────┐
      ▼     ▼         ▼          ▼
 Position  Risk      P&L        Audit
      │
      ▼
 PostgreSQL


Exchange / Market Simulator
            │
            ▼
 Market Data Processor
            │
       normalized feed
            │
       ┌────┴────┐
       ▼         ▼
 Strategies    WebSocket
```

An important design goal is that PostgreSQL and Redis must **not automatically be placed in the critical matching path**.

Every architectural choice must have an engineering reason.

When creating code:

* use clear names;
* avoid huge classes;
* avoid fake abstractions;
* avoid unnecessary design patterns;
* write meaningful tests;
* use records where appropriate;
* prefer immutable domain events;
* separate domain logic from infrastructure;
* add structured logging;
* handle errors explicitly;
* never silently swallow exceptions;
* document important concurrency assumptions.

Whenever a stage introduces a design decision, create or update:

```text
docs/architecture-decisions/
```

using short Architecture Decision Records.

Examples:

```text
ADR-001-single-writer-orderbook.md
ADR-002-kafka-partitioning.md
ADR-003-event-idempotency.md
```

Do not fabricate benchmark numbers.

Performance numbers may appear in documentation only after a benchmark has actually generated them.

Do not prematurely optimize the first implementation. We intentionally want to create a baseline and later use profiling to discover bottlenecks.

When I send each stage:

1. inspect the current repository;
2. preserve working functionality;
3. implement only the requested stage;
4. add tests;
5. update documentation;
6. tell me exactly how to run it;
7. explain the important engineering decisions;
8. mention weaknesses that we intentionally leave for later stages.

Do not start future stages unless I explicitly provide the next prompt.

---

# STAGE 1 — Trading Domain + Matching Engine

Build the core trading domain and an in-memory matching engine.

Do not introduce Kafka, PostgreSQL, Redis, REST APIs or Spring Boot application infrastructure yet. This stage should focus entirely on correct deterministic trading logic.

Support instruments identified by symbol and implement:

* BUY and SELL sides;
* LIMIT orders;
* MARKET orders;
* NEW, PARTIALLY_FILLED, FILLED, CANCELLED and REJECTED states;
* quantity;
* remaining quantity;
* price;
* timestamps;
* unique order IDs;
* client order IDs.

Implement an order book with **price-time priority**.

For BUY orders, higher prices have priority.

For SELL orders, lower prices have priority.

For orders at the same price, the earlier order must execute first.

Support:

```text
submit
cancel
replace/modify
```

A match should generate immutable execution events containing sufficient information to reconstruct what happened.

Design the matching logic to be deterministic: processing the same ordered input sequence should generate the same resulting order-book state and execution sequence.

Create comprehensive tests covering:

* full fills;
* partial fills;
* multiple price levels;
* time priority;
* market orders;
* cancellations;
* modifications;
* insufficient liquidity;
* invalid quantities;
* invalid prices;
* duplicate IDs.

Create a simple command-line simulator capable of feeding thousands of generated orders into the engine.

Keep the matching engine independent from Spring and infrastructure libraries.

Document:

```text
docs/order-lifecycle.md
docs/matching-algorithm.md
ADR explaining the selected order-book data structure
```

At the end, show me the important classes, how the order book is represented, algorithmic complexity for submission/cancellation/matching, and how to run all tests.

Do not implement concurrency yet.

---

# STAGE 2 — Concurrent Order Routing and Deterministic Processing

The matching engine is currently correct but essentially single-threaded.

Now introduce concurrency without destroying deterministic order-book semantics.

Do NOT simply place `synchronized` around the entire matching engine.

Implement a **single-writer-per-order-book architecture**.

Incoming orders should be routed according to symbol or instrument so that a particular order book has one logical owner at a time.

Conceptually:

```text
Incoming orders
      │
      ▼
Order Router
      │
hash(symbol)
      │
 ┌────┼────┐
 ▼    ▼    ▼
W1    W2    W3
│     │     │
Books Books Books
```

Each worker may own multiple symbols, but two workers must not concurrently mutate the same order book.

Use bounded queues between producers and matching workers so that overload can be detected rather than causing unlimited memory growth.

Define what happens when the queue is full.

Measure queue depth and processing time.

Preserve ordering for commands belonging to the same instrument.

Add a monotonically increasing sequence number where useful so processing order can be reasoned about.

Add tests for:

* simultaneous submissions;
* ordering;
* concurrent symbols;
* cancellation while traffic exists;
* queue saturation;
* deterministic replay.

Create a stress-test runner that can generate configurable numbers of:

```text
symbols
producer threads
orders
orders/second
```

Create an ADR explaining why we use single-writer order books instead of coarse-grained locking.

Do not optimize queues or introduce exotic lock-free libraries yet. We need a measurable baseline first.

---

# STAGE 3 — Trading Gateway + Pre-Trade Risk + Idempotency

Now introduce a proper order-ingress layer.

Build a trading gateway that accepts trading commands and performs all necessary checks before they reach matching workers.

Use Spring Boot for the gateway/control-plane APIs.

Support:

```text
POST /orders
DELETE /orders/{id}
PUT /orders/{id}
GET /orders/{id}
GET /orderbook/{symbol}
```

The gateway should perform:

```text
request validation
client authentication abstraction
clientOrderId idempotency
rate limiting
instrument validation
pre-trade risk validation
routing
```

Implement realistic but simplified pre-trade checks including:

```text
maximum order quantity
maximum order notional
allowed instruments
maximum open orders
position limit
duplicate clientOrderId
```

Do not put PostgreSQL into the hot request path merely to check every risk rule.

Design an in-memory risk-state representation and explain how authoritative state and persistence could coexist.

Make idempotency correct: retrying the same client order should not accidentally create another order.

Return meaningful rejection reasons.

Add correlation IDs and structured logs so an order can be traced from gateway to matching engine.

Create tests for duplicate requests, risk rejection, concurrent requests and rate limiting.

Create ADRs for:

```text
pre-trade risk architecture
idempotency strategy
rate limiting strategy
```

---

# STAGE 4 — Execution Event Pipeline with Kafka

The matching engine now generates executions. Introduce Kafka as the asynchronous backbone for downstream trade processing.

Do NOT use Kafka to synchronously coordinate every step inside the matching engine.

Define immutable events such as:

```text
OrderAccepted
OrderRejected
OrderCancelled
OrderReplaced
ExecutionCreated
OrderFilled
```

Use sensible topic organization.

Choose Kafka message keys deliberately so ordering guarantees match trading requirements.

Explain why the selected partition key is used.

Implement producers and consumers.

Downstream consumers should include:

```text
execution processor
position processor
audit processor
risk-state updater
```

Consumers must be **idempotent**.

Assume Kafka can redeliver messages.

Implement event IDs and duplicate detection.

Handle poison messages through an appropriate retry/dead-letter strategy without creating infinite retry loops.

Expose Kafka consumer lag and processing metrics.

Add integration tests using Testcontainers.

Test scenarios including:

```text
duplicate Kafka delivery
consumer restart
consumer group rebalance
invalid message
slow consumer
temporary Kafka outage
```

Create ADRs covering:

```text
topic design
partitioning strategy
consumer idempotency
retry/DLQ strategy
```

---

# STAGE 5 — Positions, P&L, Risk and Persistence

Implement the downstream portfolio side of the trading platform.

From executions, calculate positions per:

```text
account
strategy
instrument
```

Track:

```text
net position
average entry price
realized P&L
unrealized P&L
```

Introduce market prices so unrealized P&L can be recalculated.

Persist appropriate state in PostgreSQL.

Use Flyway migrations.

Do not synchronously insert into PostgreSQL from the latency-sensitive matching loop.

Explicitly reason about:

```text
Kafka processing succeeds but DB write fails
DB succeeds but consumer crashes before offset commit
duplicate execution arrives
events arrive after restart
portfolio service is temporarily unavailable
```

Design consumers so recovery does not corrupt positions.

Implement reconciliation logic capable of rebuilding derived position state from execution history.

Expose endpoints for:

```text
positions
executions
P&L
risk exposure
```

Add proper indexes and explain each important index.

Add tests for crash/restart and duplicate-event scenarios.

Create:

```text
docs/position-calculation.md
docs/recovery-model.md
```

---

# STAGE 6 — Market Data Engineering

Introduce a realistic market-data subsystem.

Create an exchange/market simulator producing:

```text
bid updates
ask updates
trades
last traded price
```

Generate configurable data rates across many symbols.

Build a market-data processor that:

```text
ingests raw events
validates them
assigns/validates sequence numbers
normalizes them
detects gaps
maintains latest state
publishes normalized updates
```

Expose processed market data through WebSocket subscribers.

Use market prices to update unrealized P&L.

Handle:

```text
duplicate market-data events
out-of-order events
sequence gaps
slow subscribers
subscriber disconnect/reconnect
bursty traffic
```

Do not allow a slow WebSocket client to block the market-data ingestion path.

Document backpressure strategy.

Where appropriate introduce a binary representation or Protobuf and make serialization pluggable so we can benchmark JSON versus binary encoding later.

Create ADRs for:

```text
market-data sequencing
slow-consumer handling
serialization
```

---

# STAGE 7 — Reliability, Recovery and Failure Engineering

Treat the entire system as production infrastructure.

Introduce explicit failure testing.

Implement or document appropriate handling for:

```text
Kafka unavailable
PostgreSQL unavailable
gateway restart
matching-worker restart
portfolio consumer crash
network interruption
duplicate execution
duplicate order submission
out-of-order messages
queue saturation
slow consumer
poison Kafka event
partial downstream outage
traffic burst
```

For the most important scenarios, create automated integration or chaos-style tests.

Design durable recovery for orders/executions.

Evaluate whether a write-ahead log, append-only journal or another durable mechanism is necessary for recovering matching-engine state.

If introducing a local journal, ensure the design supports:

```text
append
flush policy
replay
sequence verification
snapshot + replay
```

Do not pretend that an in-memory matching engine can survive process loss without addressing state recovery.

Implement graceful shutdown so queues can be drained safely and offsets/state handled correctly.

Document recovery-point and recovery-time trade-offs.

Create:

```text
docs/failure-matrix.md
docs/disaster-recovery.md
docs/replay-and-reconciliation.md
```

The failure matrix should have columns:

```text
Failure
Impact
Detection
Immediate behavior
Recovery
Data-loss possibility
Operational alert
```

---

# STAGE 8 — Observability and Production Operations

Add production-grade observability.

Integrate:

```text
Micrometer
Prometheus
Grafana
structured logging
distributed correlation IDs
health checks
```

Expose metrics including:

```text
orders received/sec
orders matched/sec
executions/sec
rejections/sec
matching latency
gateway latency
queue depth
queue saturation
Kafka producer latency
Kafka consumer lag
market-data events/sec
position processing latency
JVM heap
GC pause time
thread counts
CPU
error rates
```

Latency metrics must include percentiles where technically appropriate:

```text
p50
p95
p99
p99.9
```

Create a Docker Compose environment that can run the required infrastructure locally.

Provide Grafana dashboards aimed at answering:

```text
Is the system healthy?
Where is latency increasing?
Are queues backing up?
Is Kafka falling behind?
Is GC affecting latency?
Are orders being rejected unusually?
Is one symbol/partition overloaded?
```

Add alerting-rule examples.

Do not add meaningless dashboards just for appearance.

---

# STAGE 9 — Performance Engineering and Benchmarking

Now treat performance as an engineering exercise rather than guessing.

Create a dedicated benchmark/load-testing module.

Establish a reproducible baseline before optimizing anything.

Measure:

```text
throughput
p50
p95
p99
p99.9
maximum latency
CPU usage
memory usage
allocation rate
GC pause
queue depth
Kafka lag
```

Test multiple workloads:

```text
single hot symbol
many evenly distributed symbols
burst traffic
steady traffic
high cancellation rate
high match rate
low match rate
slow downstream consumer
```

Use appropriate tools such as:

```text
JMH
Java Flight Recorder
jcmd
jstat
async-profiler where available
```

Generate benchmark reports automatically into:

```text
benchmarks/results/
```

Do not optimize until profiling identifies measurable bottlenecks.

After the baseline, investigate and experiment with several engineering changes such as:

```text
coarse locking vs single-writer
different queue implementations
JSON vs Protobuf
REST vs persistent TCP for order ingress
allocation-heavy vs reduced-allocation paths
G1GC vs ZGC where sensible
Kafka batch configurations
different worker/partition counts
```

For every optimization document:

```text
Hypothesis
Measurement before
Profiler evidence
Change
Measurement after
Trade-offs
Conclusion
```

If an optimization makes performance worse, keep that result in the engineering report. Negative results are useful.

Never fabricate impressive throughput.

---

# STAGE 10 — Low-Latency Networking Experiment

Keep the REST API for usability, but create an additional experimental low-latency order-entry path.

Implement a persistent TCP protocol for:

```text
submit order
cancel order
replace order
acknowledgment
execution response
```

Use a compact message schema.

Compare:

```text
HTTP + JSON
HTTP + Protobuf if implemented
persistent TCP + binary encoding
```

Measure actual latency and throughput.

Explain:

```text
connection establishment cost
serialization overhead
kernel networking overhead
buffering
backpressure
partial reads/writes
message framing
```

Do not claim TCP is always faster without measuring.

Keep this as an engineering experiment rather than rebuilding the entire platform around custom networking.

Document the protocol and benchmark results.

---

# STAGE 11 — System-Level Correctness and Testing

Build a serious test strategy.

Add:

```text
unit tests
property-based tests where useful
integration tests
Testcontainers tests
concurrency tests
replay tests
failure tests
load tests
```

Important invariants should include:

```text
executed quantity can never exceed order quantity
order remaining quantity cannot become negative
cancelled orders cannot execute afterward
filled orders cannot return to active state
each execution affects positions exactly once
bid/ask ordering remains valid
replaying identical journal/events recreates identical state
```

Add deterministic seeded test workloads so failures can be reproduced.

Create an end-to-end test:

```text
Generate market
    ↓
submit orders
    ↓
risk
    ↓
matching
    ↓
executions
    ↓
Kafka
    ↓
positions/P&L
    ↓
persistence
    ↓
verify resulting state
```

Create `docs/testing-strategy.md`.

---

# STAGE 12 — Final Production-Grade Repository Polish

Now prepare the repository as something a senior engineer or trading-technology hiring manager could inspect.

Do not change architecture merely to make the repository look complicated.

Create a professional root README containing:

1. what problem the platform solves;
2. architecture diagram;
3. request/execution flow;
4. concurrency model;
5. matching algorithm;
6. Kafka architecture;
7. consistency model;
8. failure/recovery model;
9. market-data architecture;
10. observability;
11. benchmark methodology;
12. actual benchmark results;
13. discovered bottlenecks;
14. optimizations attempted;
15. engineering trade-offs;
16. known limitations;
17. how to run locally;
18. how to run benchmarks;
19. how to reproduce failure tests.

Create:

```text
docs/
├── architecture.md
├── order-lifecycle.md
├── matching-algorithm.md
├── concurrency-model.md
├── kafka-design.md
├── market-data.md
├── consistency-model.md
├── failure-matrix.md
├── replay-and-reconciliation.md
├── performance-engineering.md
├── benchmark-methodology.md
├── testing-strategy.md
└── architecture-decisions/
```

Create one architecture diagram showing the whole platform and smaller sequence diagrams for:

```text
order submission
successful execution
duplicate request
Kafka failure
service restart/replay
```

Inspect the entire repository for:

```text
dead code
AI-generated comments that add no value
unnecessary abstractions
inconsistent naming
poor exception handling
missing tests
hardcoded configuration
security mistakes
thread-safety issues
resource leaks
incorrect Kafka offset handling
transaction problems
```

Run all tests and benchmarks.

Do not hide unresolved problems. Put genuine limitations under:

```text
Known Limitations / Future Engineering Work
```

---

# FINAL REVIEW PROMPT

The system is now feature complete.

Act as a senior engineer interviewing a candidate for a trading-infrastructure position at a top hedge fund or electronic trading company.

Review this repository deeply.

Do not praise it generically.

Challenge the architecture.

Identify anything that would make an experienced trading-systems engineer think this is merely a portfolio project.

Specifically investigate:

* matching-engine correctness;
* concurrency;
* ordering;
* determinism;
* state ownership;
* Kafka semantics;
* idempotency;
* durability;
* recovery;
* consistency;
* backpressure;
* network design;
* JVM behavior;
* GC;
* memory allocation;
* latency methodology;
* benchmark validity;
* market-data sequencing;
* P&L correctness;
* failure scenarios;
* observability;
* operational readiness.

For every weakness, classify it:

```text
CRITICAL
IMPORTANT
OPTIONAL
```

Then propose the smallest engineering change necessary to address it.

Finally generate **30 interview questions specifically based on this repository** that a Millennium, IMC, Tower, Squarepoint, Qube, WorldQuant, AlphaGrep, Virtu, Optiver or similar trading-infrastructure interviewer might ask.

For each question, reference the exact component in my project that I should use when explaining my answer.

Do not generate generic interview questions unrelated to the implementation.
