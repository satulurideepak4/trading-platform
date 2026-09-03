# Benchmark methodology

Every number in `docs/performance-engineering.md` and `benchmarks/results/*.md` was actually
measured in this environment, on the run whose report file it appears in. Nothing here is
estimated, extrapolated from unrelated hardware, or written before the command that produced it
was run. This document is the disclosure `docs/performance-engineering.md` points back to for
every claim it makes, and the reproduction instructions for reproducing any of them yourself.

## Hardware and JDK disclosure

* **Host**: Apple M1 Pro (Apple Silicon, arm64), 8 cores, 16GB RAM, macOS 15.1.1.
* **JDK**: Zulu 21.0.7 (arm64) for the JMH suite and `LoadTestRunner`, run directly on the host.
* **Container runs** (the slow-downstream-consumer workload only): the same host, one layer of
  Docker Desktop virtualization, `eclipse-temurin:21-jre-alpine` (Temurin 21.0.11) inside the
  `trading-gateway` image.

This is a developer laptop shared with an ordinary developer's other running processes and
containers (see `docs/performance-engineering.md`'s environment note on each run), not a
dedicated, isolated benchmark rig with pinned cores, disabled turbo/frequency scaling, or a
controlled thermal environment. Absolute numbers are specific to this machine, this JDK build, and
this moment; they are not vendor-comparable throughput claims and should not be read as such. What
*is* trustworthy is the *relative* comparison within a single experiment run back-to-back on the
same machine in the same session — worker count against itself, one queue implementation against
another, one collector against another — because whatever noise the shared environment
contributes affects both sides of that comparison similarly.

## Why two tools, not one

* **JMH** (`benchmark/src/main/java/com/tradingplatform/benchmark/jmh/`) measures
  `MatchingEngine.submit()` in isolation: pure insertion cost (orders that never cross) and pure
  matching cost (orders that always cross one resting counter-order). JMH exists for exactly this —
  warmup, forking, dead-code-elimination-safe blackholes, and `Mode.SampleTime` gives real
  p50/p95/p99/p99.9 percentiles natively.
* **`LoadTestRunner`** (`benchmark/src/main/java/com/tradingplatform/benchmark/load/`) measures
  `OrderRouter` (or a benchmark-local variant of it) under realistic concurrent multi-producer load:
  queueing, backpressure, admission rejection, and producer-observed wall-clock latency. This is a
  systems question — queue depth, saturation, cross-thread handoff cost — that a microbenchmark
  harness isn't built to represent.

Real matching-engine benchmarking is usually split this same way in practice: a hot-path
microbenchmark for the algorithm, a load generator for the system it runs inside.

## Workloads

Seven of the eight workloads the master prompt asks for are one `Workload` record each
(`benchmark/.../load/Workload.java`), every one differing from `steady()` by exactly one field —
enforced by `WorkloadTest`, not just asserted in a comment:

| Workload | Parameter varied |
| --- | --- |
| `steady` | baseline: 16 symbols, 50,000 orders/s paced, 500,000 orders, 4 workers |
| `burst` | `ordersPerSecond=0` (unthrottled — as fast as the target will admit) |
| `single-hot-symbol` | `symbolCount=1` |
| `many-symbols` | `symbolCount=64` |
| `high-cancel-rate` | `cancelRatio=0.4` |
| `high-match-rate` | `matchBandTicks=2` (narrow band around a shared reference price → prices usually overlap) |
| `low-match-rate` | `matchBandTicks=2000` (wide band → prices rarely overlap) |

Match rate is *engineered*, not asserted: `LoadTestRunner` draws every limit order's price from
`referencePrice ± matchBandTicks`, and the realized match rate (`executions / accepted`) is
whatever the run actually measured, reported in each result, not a claimed target.

The eighth — **slow downstream consumer** — could not be produced by `LoadTestRunner`, because
there is no downstream consumer to slow down inside an in-process `OrderRouter`; the position/
execution/audit/risk consumers only exist in the running `trading-gateway`. It is run instead
against the real `docker compose` stack, reading the same `kafka_consumer_fetch_manager_records_lag_max`
Prometheus metric `docs/observability.md` already documents. See
`benchmarks/results/slow-downstream-consumer-docker-compose-*.md` for exactly what was done and
why it was adapted from "pause the consumer" (not reachable in a single-process architecture) to
"send enough real burst traffic to see whether a consumer falls behind on its own."

## Profiling toolset

**JFR, not async-profiler.** `async-profiler` is not installed in this environment (checked: not
on `PATH`, no Homebrew formula present). JFR is built into JDK 21, needs no extra installation, and
is explicitly named in the master prompt's own suggested toolset. Runs are started with
`-XX:StartFlightRecording=filename=<file>,settings=profile` and read back with the JDK's own
`jfr summary` and `jfr print --events jdk.ExecutionSample` — no third-party tooling, no
interpretation layer between the raw recording and what's quoted in the write-up.

**`jstat -gc` / `-Xlog:gc`** supplements for GC-focused comparisons (the G1 vs. ZGC experiment):
real pause counts, pause durations, and (where they occurred) allocation-stall events, read
straight from the JVM's own GC log, not summarized from memory.

## Reproducing a run

```bash
# JMH suite (2 forks, 5x1s warmup, 5x1s measurement per benchmark - see the class Javadoc, not
# 1-iteration theater):
mvn -pl benchmark -am package -DskipTests
java -jar benchmark/target/benchmarks.jar -rf json -rff benchmarks/results/jmh-results.json

# One macro workload against the real OrderRouter:
java -cp benchmark/target/classes:matching-engine/target/classes:trading-domain/target/classes \
  com.tradingplatform.benchmark.load.LoadTestRunner --workload=steady --target=router

# Available --target values: router (the real production OrderRouter), coarse-lock
# (CoarseLockedMatchingHarness), queue-array / queue-linked (QueueComparisonHarness), plus Stage
# 10's three network targets below. Available --workload values: steady, burst, single-hot-symbol,
# many-symbols, high-cancel-rate, high-match-rate, low-match-rate. --orders/--workers/--seed
# override the named workload's defaults; --producers controls the load generator's own thread
# count (default: min(4, cores)).

# Slow-downstream-consumer workload (see benchmarks/results/slow-downstream-consumer-*.md for the
# exact commands used):
docker compose up -d --build
# ... send real order traffic at the running gateway, read /actuator/prometheus ...
docker compose down
```

## Stage 10: network targets (HTTP+JSON, HTTP+Protobuf, TCP+binary)

Unlike `router`/`coarse-lock`/`queue-array`/`queue-linked`, which drive an in-process `OrderRouter`
with no network involved, `rest-json`, `rest-protobuf` and `tcp-binary` are real clients making
real socket calls against a running `trading-gateway`. They need the stack up first, they need a
Java build that has resolved `trading-gateway` into the local Maven repository (`exec:java` cannot
see a sibling module's classes through `-am` the way `package -am` can, because it does not run the
reactor's `package` phase), and — because the gateway's own risk limits and idempotency registry are
real and shared across every request an account makes — the fairest comparison of the three targets
requires **the same, fresh gateway state** for each one; running them back to back against a
long-lived gateway lets an earlier target's resting orders and open-order count bleed into the
next one's results.

```bash
# One-time (or after any source change): install every module, including trading-gateway's plain
# jar (not its Spring Boot -exec.jar), into the local repo so exec:java can resolve it as a
# dependency.
mvn -q install -DskipTests

# Before EACH target run, for a clean, non-confounded comparison:
docker compose down -v && docker compose up -d --build
# wait for `docker inspect --format='{{.State.Health.Status}}' trading-gateway` == healthy

mvn -pl benchmark exec:java -Dexec.mainClass=com.tradingplatform.benchmark.load.LoadTestRunner \
  -Dexec.args="--target=tcp-binary --orders=150 --producers=4 --host=localhost --tcp-port=9091 --api-key=local-dev-key --workload=steady"

mvn -pl benchmark exec:java -Dexec.mainClass=com.tradingplatform.benchmark.load.LoadTestRunner \
  -Dexec.args="--target=rest-json --orders=150 --producers=4 --host=localhost --http-port=8081 --api-key=local-dev-key --workload=steady"

mvn -pl benchmark exec:java -Dexec.mainClass=com.tradingplatform.benchmark.load.LoadTestRunner \
  -Dexec.args="--target=rest-protobuf --orders=150 --producers=4 --host=localhost --http-port=8081 --api-key=local-dev-key --workload=steady"

# JMH pure serialization comparison (2 forks, 5x1s warmup, 5x1s measurement - same discipline as
# every other JMH suite in this repo):
java -jar benchmark/target/benchmarks.jar SerializationBenchmark -rf json -rff benchmarks/results/jmh-serialization-results.json
```

`--host`/`--http-port`/`--tcp-port`/`--api-key` are new `Arguments` fields specific to the three
network targets; every in-process target ignores them. `local-dev-key` (account `ACC-DEV`) is the
default docker-compose credential — see `.env.example`'s `GATEWAY_API_KEYS`.

`LoadTestRunner` namespaces every clientOrderId it generates with a per-invocation `runId`
(`System.nanoTime()` in base 36) specifically so that two separate invocations against the same
long-lived gateway — the ordinary case for a network target, unlike an in-process target which gets
a fresh engine every run — never collide on `"load-1"`..`"load-N"` and produce a spurious
`ClientOrderIdConflictException` instead of a clean admission. See the comment on its declaration in
`LoadTestRunner.main()`.

Two order-count/producer-count regimes are used deliberately, not one, because they measure
different things — see `docs/networking-comparison.md`'s backpressure section for what the
difference between them revealed:

* **`--orders=150 --producers=4`** ("typical"): comfortably under the gateway's fixed 2-second
  `trading.matching.command-timeout`, giving a clean, mutually comparable latency/throughput number
  for each transport with no requests timing out on any of the three.
* **`--orders=400 --producers=8`** ("burst"): a deliberate concurrency burst — 8 producer threads
  firing as fast as the 50,000/s `steady` pacing allows, which is effectively unthrottled at this
  scale — sized to expose how each transport's own admission model behaves once the shared,
  unchanged matching-engine pipeline is the bottleneck, not the network.

Every run writes its own timestamped Markdown report into `benchmarks/results/`, generated by
`ReportWriter`, never hand-edited — that directory is the raw evidence
`docs/performance-engineering.md` and `docs/networking-comparison.md` summarize and interpret.
