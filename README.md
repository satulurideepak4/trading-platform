# Real-Time Electronic Trading Platform

This repository is a modular Java 21 implementation of production-oriented electronic trading
infrastructure. Stages 1 to 10 provide deterministic trading-domain types, an in-memory price-time
priority matching engine with durable recovery across a restart, bounded concurrent routing,
in-memory pre-trade risk, an authenticated HTTP order gateway, a Kafka execution-event pipeline
with idempotent downstream consumers, a durable Postgres-backed portfolio with exact position and
P&L accounting, a simulated market-data feed that keeps unrealized P&L moving between trades, a
containerised Prometheus/Grafana observability stack over metrics the matching layer wasn't
publishing before this stage, and a benchmark suite with real, measured performance numbers —
nothing in this repository's history claimed a throughput or latency figure until Stage 9 actually
ran one. Stage 11 generalizes correctness from fixed example tests to randomized invariant tests
seeded for reproducibility, closes execution-pipeline's test coverage gap, and extends the
platform's end-to-end test with a seeded workload verified against an independently computed
reference answer — see [Testing strategy](docs/testing-strategy.md). Stage 12 is this repository's
own audit pass plus the synthesis documents that tie the rest together — see
[Architecture](docs/architecture.md) and [Consistency model](docs/consistency-model.md).

## Implemented modules

* `trading-domain`: commands, order states, snapshots, rejection reasons, and immutable executions.
* `matching-engine`: synchronous matching, single-writer-per-book routing workers, and an
  append-only per-worker journal that recovers a worker's book after a restart.
* `risk-engine`: in-memory pre-trade risk checks with reserve-and-settle exposure accounting.
* `market-data`: a sequenced, gap-aware market-data processor, a non-blocking per-subscriber fan-out
  hub, and pluggable JSON/binary serialization — independent of Spring, Kafka and the OMS domain.
* `execution-pipeline`: immutable trading events, a buffered Kafka producer, and the downstream
  consumers with idempotency, retry and dead-lettering.
* `portfolio-service`: exact-arithmetic position/P&L state, a transactional Kafka consumer that
  writes it to Postgres, and reconciliation that rebuilds it from trade history.
* `trading-gateway`: Spring Boot order ingress with authentication, rate limiting, idempotency,
  routing, the portfolio read API, the market-data WebSocket/REST endpoints, and the Prometheus
  metrics bridge for everything the matching layer computes but doesn't itself depend on Micrometer
  to report.
* `exchange-simulator`: deterministic and concurrent generated order-flow smoke/stress runners, plus
  a market-data feed smoke test (`MarketDataFeedSimulator`).
* `benchmark`: JMH microbenchmarks of `MatchingEngine.submit()` in isolation, and `LoadTestRunner` —
  a concurrent load generator that drives `OrderRouter` (or a benchmark-local comparison harness)
  through named traffic-shape workloads and reports real throughput/latency/queue-depth numbers.

Prices are positive integer ticks; this avoids floating-point ambiguity and leaves
instrument-specific tick-size validation to a later stage. Positions and P&L are likewise exact
integer/`BigInteger` arithmetic — no floating point anywhere in the money path; see
[Position calculation](docs/position-calculation.md). Matching, routing and risk remain plain Java
with no Spring, Kafka or database dependency; the broker and the database are only ever reached
from the asynchronous path behind the matching engine.

Stage 2 maps each symbol to one matching worker. Each worker owns its engine state and consumes a
bounded queue, so different workers can process concurrently while commands for one symbol retain
FIFO admission order. Queue saturation rejects admission immediately with
`OrderRoutingRejectedException`; a rejected command was not processed and may be retried by the
caller. See [Concurrency model](docs/concurrency-model.md).

Stage 3 puts a gateway in front of that router. Each request is authenticated to an account, charged
against a per-account token bucket, validated, resolved against a `(account, clientOrderId)`
idempotency registration, and risk checked before any command is routed. Risk exposure is reserved
before routing and settled from the matching outcome, including for the resting side of a fill. See
[Gateway API](docs/gateway-api.md).

Stage 4 publishes what happened. Matching outcomes become immutable events on two Kafka topics,
keyed by symbol so the engine's per-instrument ordering is preserved end to end. Events cross a
bounded in-process queue and a single dispatcher thread, so a broker problem can never block a
matching worker — an integration test pauses the broker for three seconds and asserts orders are
still accepted promptly. Event ids are derived from the facts they describe rather than generated,
which is what lets every consumer deduplicate a redelivery. Replaying the executions topic rebuilds
risk positions after a restart, and the gateway reports itself out of service until that replay has
caught up. See [Kafka design](docs/kafka-design.md).

Stage 5 gives executions and positions a durable home. `PortfolioProcessor` consumes the same
executions topic Stage 4 introduced and, in one Postgres transaction per trade, inserts the
execution and moves both accounts' positions — the execution's primary key is the idempotency
check, so redelivery of any kind is one `ON CONFLICT` clause away from a no-op. Position arithmetic
is exact (integer cost basis, no floating point, no division on the hot path) and uses weighted
average cost with correct partial-close and self-trade handling. `PositionReconciliation` can
rebuild every position from the trade history and report or repair drift. New endpoints —
`/positions`, `/executions`, `/pnl`, `/risk/exposure` — expose it, all scoped to the authenticated
caller. See [Position calculation](docs/position-calculation.md) and
[Recovery model](docs/recovery-model.md).

Stage 6 adds a simulated market-data feed, deliberately imperfect — `MarketSimulator` injects
duplicates, out-of-order records and sequence gaps on purpose — so `MarketDataProcessor`'s
classification is exercised by every run rather than assumed to work. Normalized updates fan out
over WebSocket through a hub that gives every subscriber its own bounded queue and drain thread, so
one slow client can never block another or the feed itself. The same processor state closes a gap
Stage 5 left open: `MarkPriceUpdater` polls it on a schedule and writes changed reference prices into
`mark_prices`, so unrealized P&L now moves between trades instead of sitting frozen at the last
traded price. See [Market data](docs/market-data.md).

Stage 7 stops pretending the in-memory matching engine can survive process loss. Each worker now
writes an append-only journal of the commands it processes, on its own thread, immediately before
applying each one — by the time a caller ever sees a command succeed, it is already fsynced to disk.
A restarted `OrderRouter` replays every worker's journal before accepting new traffic, reconstructing
the exact same book, order statuses and execution-id sequence, without re-publishing a single event
downstream consumers already saw the first time. Most of the rest of Stage 7's failure list — Kafka
and Postgres outages, duplicate/out-of-order events, poison messages, slow consumers, queue
saturation, traffic bursts — was already handled by Stages 2-6; this stage's other job was writing
that coverage down in one place rather than leaving it implicit. See
[Failure matrix](docs/failure-matrix.md), [Disaster recovery](docs/disaster-recovery.md) and
[Replay and reconciliation](docs/replay-and-reconciliation.md).

Stage 8 makes the matching layer's own throughput, latency and queue depth actually visible —
before this stage they were computed (`RouterMetricsSnapshot`) but never reached Micrometer, so
nothing could answer "orders received/matched/rejected per second" or "matching latency p50/p95/p99"
or "is one worker's queue backing up." `GatewayMetrics` and `RouterMetricsBinder` bridge those
numbers in from `trading-gateway`, the same layered approach Stage 7's journal used to keep
`matching-engine` itself free of Spring/Micrometer. A `micrometer-registry-prometheus` dependency and
`/actuator/prometheus` turn every meter in the process — these new ones and the ones earlier stages
already published — into something scrapable, and the gateway is now containerised
(`trading-gateway/Dockerfile`) so Prometheus, running in the same Docker network, has something to
reach. Three Grafana dashboards and a handful of real alert rules round it out. See
[Observability](docs/observability.md).

Stage 9 finally measures the system every prior stage described but never benchmarked. JMH
microbenchmarks isolate `MatchingEngine.submit()`'s own cost (insertion and matching separately);
a purpose-built `LoadTestRunner` drives `OrderRouter` under seven named concurrent traffic shapes
(steady, burst, single-hot-symbol, many-symbols, high-cancel-rate, high/low-match-rate), plus an
eighth run for real against the containerised gateway to observe Kafka consumer lag under load.
Four optimization experiments follow the same rigor — worker/partition count, ADR-002's
single-writer-per-book queue against a coarse-lock alternative (with JFR evidence explaining the
result), queue implementation, and G1 vs. ZGC — each written up as hypothesis, before, profiler
evidence, after, trade-offs and conclusion, including the one (G1 vs. ZGC) whose result runs
against the more commonly expected outcome. Every number is real, measured on this repository's own
disclosed hardware, never estimated; see [Benchmark methodology](docs/benchmark-methodology.md) for
the disclosure and reproduction steps, and [Performance engineering](docs/performance-engineering.md)
for the numbers and experiments themselves.

Stage 10 answers two questions Stage 9's own docs deliberately left open by name: JSON vs. Protobuf,
and REST vs. a persistent connection. `/protobuf/orders` puts a generated Protobuf schema in front
of the same `OrderIngressService` the JSON path already uses; a new experimental TCP order-entry
protocol (`docs/tcp-protocol.md`) goes further — one authenticated, length-prefixed binary
connection a client keeps open for its whole session, pipelining several in-flight
submit/cancel/replace commands rather than paying a new connection's setup cost per order. Neither
touches `OrderIngressService`, `OrderRegistry`, `OrderController` or `TradingEventEmitter` — both
are new transport/encoding layers in front of the same ingress pipeline, same risk checks, same
idempotency registry, same matching engine every REST request already goes through. All three paths
(HTTP+JSON, HTTP+Protobuf, TCP+binary) were measured back to back, real sockets against a real
running gateway, on this repository's own disclosed hardware — including where the fastest path
turned out not to be the hand-rolled TCP codec. See
[Networking comparison](docs/networking-comparison.md) for the numbers,
[TCP protocol](docs/tcp-protocol.md) for the wire spec, and
[ADR-016](docs/architecture-decisions/ADR-016-low-latency-tcp-protocol.md) for the design decisions
behind both.

## Prerequisites

* JDK 21
* Maven 3.9 or later

Ensure both `java` and Maven use JDK 21:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21) # macOS
export PATH="$JAVA_HOME/bin:$PATH"
java -version
mvn -version
```

## Build and test

```bash
mvn clean verify
```

## Run the simulator

Build the executable jar and process 10,000 generated orders:

```bash
mvn -pl exchange-simulator -am package
java -jar exchange-simulator/target/exchange-simulator-1.0.0-SNAPSHOT.jar 10000
```

An optional second argument supplies the random seed:

```bash
java -jar exchange-simulator/target/exchange-simulator-1.0.0-SNAPSHOT.jar 10000 7921
```

The simulator reports actual wall-clock time for that smoke-test run only. It is not a JMH
benchmark and its output must not be treated as controlled performance evidence.

## Run the concurrent stress workload

Build the shaded jar, then select the Stage 2 runner from its classpath:

```bash
mvn -pl exchange-simulator -am package
java -cp exchange-simulator/target/exchange-simulator-1.0.0-SNAPSHOT.jar \
  com.tradingplatform.simulator.ConcurrentOrderFlowSimulator \
  --symbols=16 --producers=4 --orders=100000 --orders-per-second=0 \
  --workers=4 --queue-capacity=8192 --seed=7921
```

`--orders-per-second=0` means unthrottled. The runner reports observed queue and processing metrics,
but it is intentionally not presented as a controlled benchmark.

## Run the market-data feed smoke test

Generates market data straight into a processor, with duplicates, out-of-order records and sequence
gaps injected on purpose, and reports how many of each the processor caught:

```bash
mvn -pl exchange-simulator -am install -DskipTests
mvn -pl exchange-simulator exec:java@marketdata-feed
```

## Run the trading gateway

```bash
cp .env.example .env
set -a && source .env && set +a

# Kafka + Postgres. The gateway also runs without them: set PIPELINE_ENABLED=false to trade with
# no downstream — positions and the portfolio endpoints only exist when it is on.
docker compose up -d

mvn -pl trading-gateway -am package
java -jar trading-gateway/target/trading-gateway-1.0.0-SNAPSHOT.jar
```

## Run the trading workstation UI

The browser workstation is a React control-plane client. It reaches the gateway only through its
REST and market-data WebSocket APIs; it never connects to Kafka, PostgreSQL, matching workers or
the experimental TCP port.

```bash
cp .env.example .env
docker compose up --build
```

Open [http://localhost:8081](http://localhost:8081). The local Compose proxy makes the UI and the
gateway a same-origin application. The default local API key is already prefilled for the sample
environment; it stays in browser session storage only and is never baked into the UI image.

The UI shows gateway connectivity explicitly. A disconnected gateway never produces simulated
order acknowledgements: order submission is disabled until the authenticated gateway data loads.
Market-data stream updates are labelled by their backend sequencing outcome, so a `GAP` or stale
feed is visible to the operator rather than silently presented as current market state.

Flyway migrates the schema automatically on startup; there is nothing to run by hand. If port 5432
is already taken by something else on your machine, set `DB_HOST_PORT` in `.env` to an alternate
host port and update `DB_PORT` to match.

`GATEWAY_API_KEYS` has no default; the gateway refuses to start without it rather than running with
an accidental identity. Everything else falls back to the values in `.env.example`.

```bash
curl -s localhost:8080/actuator/health

curl -s -X POST localhost:8080/orders \
  -H 'X-Api-Key: local-dev-key' \
  -H 'X-Correlation-Id: demo-1' \
  -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"demo-0001","symbol":"AAPL","side":"SELL","type":"LIMIT","quantity":100,"price":19050}'

# The same request again returns the same order with "duplicate": true, not a second order.
curl -s -X POST localhost:8080/orders \
  -H 'X-Api-Key: local-dev-key' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"demo-0001","symbol":"AAPL","side":"SELL","type":"LIMIT","quantity":100,"price":19050}'

curl -s 'localhost:8080/orderbook/AAPL?depth=5' -H 'X-Api-Key: local-dev-key'
```

Full endpoint, status-code and rejection-reason reference: [Gateway API](docs/gateway-api.md).

### The Protobuf and TCP order-entry paths (Stage 10)

Same gateway process, two more ways in alongside `/orders`:

```bash
# HTTP+Protobuf: same ingress pipeline as /orders, a generated schema instead of JSON.
# order_entry.proto -> SubmitOrderProto/OrderAckProto; see docs/tcp-protocol.md's sibling spec
# in trading-gateway/src/main/proto/order_entry.proto for the exact message shapes.
curl -s -X POST localhost:8080/protobuf/orders \
  -H 'X-Api-Key: local-dev-key' -H 'Content-Type: application/x-protobuf' \
  --data-binary @order.bin -o ack.bin  # a SubmitOrderProto in, an OrderAckProto out

# TCP+binary: one persistent, authenticated, length-prefixed connection (default port 9090,
# mapped to host 9091 by docker-compose to avoid Prometheus's own 9090). No curl equivalent -
# see benchmark/.../load/TcpBinarySubmissionTarget.java for a complete client, or
# docs/tcp-protocol.md for the wire format to build your own.
```

Wire format, session lifecycle and configuration: [TCP protocol](docs/tcp-protocol.md). Measured
comparison against REST/JSON: [Networking comparison](docs/networking-comparison.md).

## Kill and restart the gateway

Proves Stage 7's headline claim directly, against the same process the rest of this README uses —
the resting order from the previous section is still there after an ungraceful kill:

```bash
# Find and kill the gateway process (not docker compose — Kafka/Postgres can stay up).
pkill -9 -f trading-gateway-1.0.0-SNAPSHOT.jar

java -jar trading-gateway/target/trading-gateway-1.0.0-SNAPSHOT.jar &

sleep 5
curl -s 'localhost:8080/orderbook/AAPL?depth=5' -H 'X-Api-Key: local-dev-key'
# The resting 100 @ 19050 ask is still there — recovered from
# ./data/matching-journal/worker-*.journal before this request was ever accepted.
```

`MATCHING_JOURNAL_DIR` in `.env` controls where the journal lives; set `MATCHING_JOURNAL_ENABLED=false`
to confirm the difference — the book comes back empty without it. See
[ADR-015](docs/architecture-decisions/ADR-015-matching-engine-durability.md).

Do not submit a *new* order immediately after this restart and expect it to succeed: the gateway's
own order-id counter is not durable and restarts from 1, so it collides with the order id the
matching engine just recovered. That's a real, disclosed gap, not a flaw in this demo — see
[Disaster recovery](docs/disaster-recovery.md).

## Watch the event pipeline

```bash
# Cross the spread from the second account, then read what the platform published.
curl -s -X POST localhost:8080/orders \
  -H 'X-Api-Key: local-dev-key-2' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"demo-0002","symbol":"AAPL","side":"BUY","type":"LIMIT","quantity":40,"price":19050}'

docker exec trading-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic trading.executions.v1 --from-beginning --timeout-ms 5000

docker exec trading-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic trading.orders.v1 --from-beginning --timeout-ms 5000
```

Pipeline health and throughput:

```bash
curl -s localhost:8080/actuator/health
curl -s localhost:8080/actuator/metrics/trading.consumer.duplicates
curl -s localhost:8080/actuator/metrics/kafka.consumer.fetch.manager.records.lag.max
```

`trading.consumer.duplicates` is expected to be non-zero in normal operation: the gateway applies a
fill to risk synchronously and the execution consumer replays the same fill, and the two are
deduplicated on execution id rather than double-counted.

## Check positions, trades and P&L

Continuing from the trade above, once the pipeline has caught up (a second or two):

```bash
curl -s localhost:8080/positions -H 'X-Api-Key: local-dev-key'
curl -s localhost:8080/executions -H 'X-Api-Key: local-dev-key'
curl -s localhost:8080/pnl -H 'X-Api-Key: local-dev-key'

# The synchronous risk view — no wait needed, it was correct the instant the order was accepted.
curl -s localhost:8080/risk/exposure -H 'X-Api-Key: local-dev-key'

# Mark an instrument that has gone quiet, and unrealized P&L follows immediately — no trade needed.
curl -s -X PUT localhost:8080/instruments/AAPL/mark-price \
  -H 'X-Api-Key: local-dev-key' -H 'Content-Type: application/json' -d '{"price":19200}'
curl -s localhost:8080/positions -H 'X-Api-Key: local-dev-key'
```

Inspect the durable state directly:

```bash
docker exec trading-postgres psql -U trading -d trading \
  -c "select account_id, strategy_id, symbol, open_quantity, open_cost, realized_pnl from positions;"
docker exec trading-postgres psql -U trading -d trading \
  -c "select execution_id, symbol, price, quantity, buy_account_id, sell_account_id from executions;"
```

## Watch the market-data feed

The feed starts with the gateway (`MARKETDATA_ENABLED=true` by default) and needs no API key —
market data carries no per-account state. A REST snapshot needs nothing but curl:

```bash
curl -s localhost:8080/marketdata/AAPL
```

For the live WebSocket stream, any WebSocket client works; for example with
[`websocat`](https://github.com/vi/websocat):

```bash
websocat ws://localhost:8080/marketdata
# then send:
{"type":"subscribe","symbols":["AAPL","MSFT"]}
```

The connection immediately receives each subscribed symbol's current state (tagged
`"outcome":"SNAPSHOT"`), then live ticks as they happen.

To see the feed close the Stage 5 gap — unrealized P&L moving with no second trade — open a position
and watch it change on its own:

```bash
curl -s -X POST localhost:8080/orders \
  -H 'X-Api-Key: local-dev-key' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"demo-0003","symbol":"AAPL","side":"SELL","type":"LIMIT","quantity":10,"price":19050}'
curl -s -X POST localhost:8080/orders \
  -H 'X-Api-Key: local-dev-key-2' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"demo-0004","symbol":"AAPL","side":"BUY","type":"LIMIT","quantity":10,"price":19050}'

# Re-run this a few seconds apart with no further trades; markPrice moves on its own.
curl -s localhost:8080/positions -H 'X-Api-Key: local-dev-key'
```

Full protocol, sequencing/backpressure design, and the mark-price bridge:
[Market data](docs/market-data.md).

## Run the observability stack

```bash
cp .env.example .env
docker compose up -d --build
```

Brings up Postgres, Kafka, the gateway itself (now containerised —
[`trading-gateway/Dockerfile`](trading-gateway/Dockerfile)), Prometheus and Grafana together.
Submit a few orders (see above), then:

* Prometheus: [localhost:9090](http://localhost:9090) — try the `trading_matching_latency_seconds`
  or `trading_orders_accepted_total` series, or check `/alerts` for the rule states.
* Grafana: [localhost:3000](http://localhost:3000), `admin`/`admin` — three dashboards are
  provisioned automatically under **Trading Platform**: Overview, Trading & Matching, and Kafka &
  Pipeline. No manual setup needed to reproduce them.

Prefer running the gateway from the JVM instead (the path every other section of this README uses)?
`docker compose up -d postgres kafka prometheus grafana` and point
`docker/prometheus/prometheus.yml`'s target at `host.docker.internal:8080`.

Full metrics catalog, what each dashboard answers, and the alert rules:
[Observability](docs/observability.md).

## Run the benchmarks

```bash
# JMH: MatchingEngine.submit() in isolation (insertion cost and matching cost separately), plus
# Stage 10's pure JSON/Protobuf/binary-codec serialization comparison.
mvn -pl benchmark -am package -DskipTests
java -jar benchmark/target/benchmarks.jar -rf json -rff benchmarks/results/jmh-results.json

# LoadTestRunner: OrderRouter (or a comparison harness) under concurrent load.
java -cp benchmark/target/classes:matching-engine/target/classes:trading-domain/target/classes \
  com.tradingplatform.benchmark.load.LoadTestRunner --workload=steady --target=router
```

`--workload` accepts `steady`, `burst`, `single-hot-symbol`, `many-symbols`, `high-cancel-rate`,
`high-match-rate`, `low-match-rate`; `--target` accepts `router` (the real production
`OrderRouter`), `coarse-lock`, `queue-array`, `queue-linked` (benchmark-local comparison
harnesses), or Stage 10's three network targets — `rest-json`, `rest-protobuf`, `tcp-binary` —
which drive a real running gateway over real sockets instead of an in-process `OrderRouter`; see
[Benchmark methodology](docs/benchmark-methodology.md#stage-10-network-targets-httpjson-httpprotobuf-tcpbinary)
for how to run those three fairly. Every run writes a timestamped report into
`benchmarks/results/`. Full disclosure, methodology, and the four optimization experiments'
results: [Benchmark methodology](docs/benchmark-methodology.md) and
[Performance engineering](docs/performance-engineering.md); Stage 10's own numbers:
[Networking comparison](docs/networking-comparison.md).

## Design documentation

* [Architecture](docs/architecture.md)
* [Order lifecycle](docs/order-lifecycle.md)
* [Matching algorithm](docs/matching-algorithm.md)
* [Concurrency model](docs/concurrency-model.md)
* [Consistency model](docs/consistency-model.md)
* [Gateway API](docs/gateway-api.md)
* [Kafka design](docs/kafka-design.md)
* [Position calculation](docs/position-calculation.md)
* [Recovery model](docs/recovery-model.md)
* [Market data](docs/market-data.md)
* [Failure matrix](docs/failure-matrix.md)
* [Disaster recovery](docs/disaster-recovery.md)
* [Replay and reconciliation](docs/replay-and-reconciliation.md)
* [Observability](docs/observability.md)
* [Benchmark methodology](docs/benchmark-methodology.md)
* [Performance engineering](docs/performance-engineering.md)
* [Testing strategy](docs/testing-strategy.md)
* [TCP protocol](docs/tcp-protocol.md)
* [Networking comparison](docs/networking-comparison.md)
* [ADR-001: order-book data structure](docs/architecture-decisions/ADR-001-order-book-data-structure.md)
* [ADR-002: single-writer order books](docs/architecture-decisions/ADR-002-single-writer-order-books.md)
* [ADR-003: pre-trade risk architecture](docs/architecture-decisions/ADR-003-pre-trade-risk-architecture.md)
* [ADR-004: idempotency strategy](docs/architecture-decisions/ADR-004-idempotency-strategy.md)
* [ADR-005: rate limiting strategy](docs/architecture-decisions/ADR-005-rate-limiting-strategy.md)
* [ADR-006: topic design](docs/architecture-decisions/ADR-006-topic-design.md)
* [ADR-007: partitioning strategy](docs/architecture-decisions/ADR-007-partitioning-strategy.md)
* [ADR-008: consumer idempotency](docs/architecture-decisions/ADR-008-consumer-idempotency.md)
* [ADR-009: retry and dead-letter strategy](docs/architecture-decisions/ADR-009-retry-and-dead-letter-strategy.md)
* [ADR-010: persistence architecture](docs/architecture-decisions/ADR-010-persistence-architecture.md)
* [ADR-011: reconciliation strategy](docs/architecture-decisions/ADR-011-reconciliation-strategy.md)
* [ADR-012: market-data sequencing](docs/architecture-decisions/ADR-012-market-data-sequencing.md)
* [ADR-013: slow-consumer handling](docs/architecture-decisions/ADR-013-slow-consumer-handling.md)
* [ADR-014: market-data serialization](docs/architecture-decisions/ADR-014-market-data-serialization.md)
* [ADR-015: matching-engine durability](docs/architecture-decisions/ADR-015-matching-engine-durability.md)
* [ADR-016: low-latency TCP protocol](docs/architecture-decisions/ADR-016-low-latency-tcp-protocol.md)

## Known limitations carried into later stages

* **Positions and order books** now both survive a restart, durably: the risk engine's synchronous
  copy (rebuilt by replaying the executions topic, Stage 4) and the portfolio's durable copy
  (Postgres, Stage 5) as before, and as of Stage 7 the matching engine's own book, via the per-worker
  journal (ADR-015). Gateway idempotency registrations and rate-limit buckets still do not survive a
  restart — a retry racing that exact window can still duplicate an order. Sharper and unconditional:
  the gateway's order-id allocator also resets to 1 on restart, so recovering the matching engine's
  book actually introduced a new, guaranteed failure mode — the first order submitted after a restart
  with any recovered resting orders collides (`DUPLICATE_ORDER_ID`). Reproduced directly against a
  running gateway, not hypothesized; see [Disaster recovery](docs/disaster-recovery.md).
* The matching-engine journal grows without bound — nothing truncates or compacts it, on purpose;
  ADR-015 evaluates snapshotting and defers it with a stated trigger (measured replay time becoming
  significant) rather than building it ahead of any evidence it's needed. Corruption detection is
  sequence-only, not checksummed: a record silently altered in place without breaking the sequence
  would replay wrong with nothing to catch it. See [Disaster recovery](docs/disaster-recovery.md).
* If the publisher's buffer overflows during a long broker outage, events are dropped and counted
  with no local durable record to replay from. Stage 7's journal covers matching-engine state, not
  this buffer — they are different durability gaps with different fixes, and this one is still open.
* A distinct, narrower gap from the one above: if one event's own `send()` call fails outright —
  the broker itself reachable and healthy, unlike the buffer-overflow case — the event is dropped
  permanently with no retry or dead-letter path, unconditionally, not just past some outage-length
  threshold. `trading.events.publish.failed` counts it and `EventsPublishFailing` alerts on it (see
  [Observability](docs/observability.md)), found and disclosed during a Stage 9 verification pass
  rather than fixed outright: a real fix needs a retry design that provably preserves per-symbol
  Kafka ordering (the reason the dispatcher is single-threaded at all), which is a bigger, riskier
  change than a verification pass should make blind. See
  [Failure matrix](docs/failure-matrix.md), row 15.
* The risk engine's and the audit log's execution-id dedup sets still grow without bound in memory.
  Positions no longer have this problem — Postgres's primary key replaced the in-memory set for
  them — but the risk engine's own bookkeeping (`AccountRiskState`) and the audit log have not been
  migrated, and remain open. (Not the same gap Stage 7 closed: that was the matching engine's order
  book, via ADR-015's journal, which explicitly defers snapshotting rather than adding it.)
* Nothing consumes the dead-letter topics, still, and a dead-lettered execution leaves the portfolio
  permanently short that trade. Reconciliation can now detect the resulting drift
  (`PositionReconciliation.check()`), but nothing runs it on a schedule yet, and it cannot recover a
  trade that was never durably recorded. Stage 8 added the alert (`EventsDeadLettered`, on
  `trading_consumer_dead_lettered_total`) so a give-up is no longer silent — it did not add
  consumption or automatic reprocessing.
* Replay readiness is inferred from a consumer going idle, which a genuinely quiet topic also looks
  like. It errs safe, but it is a signal rather than a proof.
* Reconciliation has no scheduled trigger and no HTTP surface; `check()`/`repair()` are called
  programmatically today. See [Recovery model](docs/recovery-model.md).
* Fees, commissions and multi-currency accounting are not modeled; realized P&L is gross, in one
  implicit currency.
* `MarketSimulator` is one thread per process — a smoke-test generator, still not a benchmarked
  producer; Stage 9 benchmarked order matching and routing, not the market-data feed itself, and
  that gap remains open. A dropped market-data update is gone: there is no replay beyond the
  current-state snapshot a new WebSocket subscription receives, and the binary codec has no
  schema-evolution story. See [Market data](docs/market-data.md#known-limitations).
* The gateway is single-instance by design. A second instance would have its own risk state,
  idempotency map, rate-limit buckets and deduplication sets — though both instances would still
  agree on positions, since those live in the shared Postgres rather than in either process.
* The router's admission critical section is one global lock. Stage 9 measured its real cost — a
  benchmark-local coarse-lock harness outperformed the real `OrderRouter` by roughly 2x at 1.5M
  orders, and JFR profiling attributed the difference to `CompletableFuture`/`ArrayBlockingQueue`
  cross-thread handoff overhead rather than the admission lock itself, which turned out to be cheap
  at the producer thread counts tested. See
  [Performance engineering § Experiment 2](docs/performance-engineering.md#experiment-2-coarse-lock-vs-single-writer-per-book-adr-002)
  and the [ADR-002 addendum](docs/architecture-decisions/ADR-002-single-writer-order-books.md#stage-9-addendum-measured-coarse-lock-comparison).
  The measurement is real; no production code changed as a result of it.
* Stage 9's numbers are real but scoped: measured on one developer laptop (Apple M1 Pro, 8 cores,
  16GB), not a dedicated benchmark rig, and the four optimization experiments' findings were not
  applied back into `OrderRouter`/`MatchingEngine` — they are evidence for a future change, not a
  change themselves. See [Benchmark methodology](docs/benchmark-methodology.md) for what "real" means
  here and its limits.
* No Alertmanager is wired up — `docker/prometheus/alert-rules.yml`'s rules evaluate and are visible
  at `localhost:9090/alerts`, but nothing routes a firing alert to a notification of any kind.
  Grafana's own login is a hardcoded local-only default; this compose stack is not meant to be
  exposed beyond a developer machine as configured. See [Observability](docs/observability.md).
* `OrderRegistry`'s idempotency map (`registrations`/`identities`) only ever shrinks along the
  command-never-reached-a-worker path; every successfully submitted or matching-engine-rejected
  order's entry is kept for the life of the process, with no eviction. Found during Stage 12's
  audit pass — the same shape as the already-disclosed unbounded growth in the risk engine's and
  audit log's dedup sets above, just not previously named for the gateway's own registry.
* API-key authentication (`ApiKeyAuthenticator`) uses a plain map lookup with no constant-time
  comparison, no rotation and no expiry. This is disclosed in the class's own Javadoc as
  deliberately "the weakest realistic mechanism... the piece a real deployment would replace
  first" — restated here because Stage 12's audit specifically checked it and it hadn't previously
  been surfaced outside the source file itself.
* Stage 11 turned the matching engine's required invariants (executed quantity never exceeds order
  quantity, remaining quantity never negative, cancelled/filled orders never reactivate, bid/ask
  ordering stays valid, replay is deterministic) from fixed hand-picked examples into randomized,
  seeded properties checked over hundreds of generated command sequences, and gave
  `execution-pipeline` a real unit-test suite where none existed before. It did not add a
  property-based testing library, and `trading-domain` remains deliberately untested (zero
  branching logic). See [Testing strategy](docs/testing-strategy.md#known-limitations) for what
  Stage 11 does and does not cover.
