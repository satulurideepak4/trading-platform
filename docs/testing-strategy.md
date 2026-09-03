# Testing strategy

This platform has 282 automated tests across seven modules (`trading-domain` and
`exchange-simulator` have none, deliberately — see Known limitations). This document says what
kind of test lives where, which of the Master-Prompt's required invariants each one proves, and
how to reproduce a failure. It does not restate what individual docs already own: failure
scenarios are `failure-matrix.md`'s table, replay mechanisms are
`replay-and-reconciliation.md`'s, and load/throughput numbers are `benchmark-methodology.md`'s.

## Test pyramid by module

| Module | Test types present | `@Test` count | Stage 11 additions |
| --- | --- | --- | --- |
| `trading-domain` | none | 0 | none — see Known limitations |
| `matching-engine` | unit, concurrency, replay, journal-corruption, randomized invariant | 40 | `MatchingEngineInvariantTest` (+2) |
| `risk-engine` | unit | 16 | none |
| `market-data` | unit | 31 | none |
| `execution-pipeline` | unit, concurrency | 29 | all 5 files (+29, from 0) |
| `portfolio-service` | unit, randomized invariant, Testcontainers | 23 | none |
| `trading-gateway` | unit, Testcontainers integration, full-stack randomized invariant | 115 | `SeededWorkloadIntegrationTest` (+2) |
| `benchmark` | unit (of the load-generation harness itself) | 28 | none |
| **Total** | | **282** | |

`exchange-simulator` has no `src/test`: it is itself a generator used *by* other modules' tests
and by the seeded stress runners documented in the root README, not a component with logic to
verify in isolation.

## Required invariants and where each is proven

The Master-Prompt lists seven invariants a trading platform must never violate. Every one now has
at least one test that asserts it as a general property over many generated inputs, not only a
hand-picked example.

| Invariant | Proven by |
| --- | --- |
| Executed quantity can never exceed order quantity | `MatchingEngineInvariantTest.quantityStatusAndBookOrderingInvariantsHoldAcrossRandomizedCommandSequences` (200 seeded trials) |
| Order remaining quantity cannot become negative | Same test, same assertion (`remainingQuantity() >= 0 && remainingQuantity() <= quantity()` after every command) |
| Cancelled orders cannot execute afterward | `MatchingEngineTest.cancellationRemovesLiquidityAndPreventsExecution` (fixed example); generalized by `MatchingEngineInvariantTest`'s terminal-status check, which fails if a `CANCELLED` order's status ever changes again |
| Filled orders cannot return to active state | `MatchingEngineTest.filledAndCancelledOrdersCannotBeActedOnAgain` (fixed example); generalized the same way as above |
| Each execution affects positions exactly once | `PortfolioUpdaterTest.applyingTheSameExecutionTwiceMovesThePositionOnce` and `.concurrentApplicationsOfTheSameExecutionResultInExactlyOneAppliedCall` (portfolio-service); `ExecutionsDedupTest` (execution-pipeline, unit-level, no broker); `ExecutionPipelineIntegrationTest.redeliveringTheSameExecutionDoesNotMoveThePositionTwice` (trading-gateway, full stack) |
| Bid/ask ordering remains valid | `MatchingEngineInvariantTest`: best-bid/best-ask non-crossing and non-increasing/non-decreasing price ordering, checked after every command in every trial |
| Replaying identical journal/events recreates identical state | `MatchingEngineTest.replayingTheSameCommandsProducesIdenticalResultsAndBook` (fixed example, single engine); `MatchingEngineInvariantTest.replayingRandomizedCommandSequencesProducesIdenticalResultsAndBookState` (50 seeded trials, two fresh engines); `OrderRouterTest.orderedReplayProducesIdenticalDomainResultsAndState`; `OrderRouterRecoveryTest` (journal replay after simulated restart); `SeededWorkloadIntegrationTest.replayingTheSameSeededSequenceThroughTheFullStackReachesTheSameFinalPositions` (same logical sequence, twice, through HTTP → risk → matching → Kafka → Postgres) |

## Deterministic seeded workloads

Every randomized test in this codebase uses a fixed, literal seed, so a failure reproduces exactly
by rerunning the same test — there is no hidden clock- or thread-order-dependent input anywhere in
this list. There is no property-based testing library in this repository (no jqwik, no
vavr-test); every randomized test below is hand-rolled in the same style
`portfolio-service`'s `PositionStateTest.aRandomRoundTripRealizesExactlyProceedsMinusCost()`
established first — a fixed `Random`/`SplittableRandom` seed, N generated trials, one invariant
asserted every trial.

| Test | Seed(s) | Trials |
| --- | --- | --- |
| `PositionStateTest.aRandomRoundTripRealizesExactlyProceedsMinusCost` | `20260818L` | 200 |
| `MatchingEngineInvariantTest` (both methods) | `20260822_000_000L + trial` / `20260822_500_000L + trial` | 200 / 50 |
| `SeededWorkloadIntegrationTest` (both methods) | `2026_08_22L` | 1 sequence of 50 orders, submitted through 3 independent full-stack runs |
| `ConcurrentOrderFlowSimulator` (stress runner, not a unit test) | `--seed=` flag, default `7_921` | configurable |

`MatchingEngineInvariantTest` additionally generates its command sequences the same way
`ConcurrentOrderFlowSimulator` generates order flow: a `SplittableRandom` per run, cancels and
replaces chosen uniformly among *all* order ids seen so far (not just currently active ones), so
rejection paths — cancel-after-fill, replace-after-cancel — get exercised as often as the happy
path, not only when a trial happens to hit them by chance.

## Testcontainers usage

`portfolio-service` and `trading-gateway` both use Testcontainers' documented singleton-container
pattern: `PortfolioTestBase` starts one `PostgreSQLContainer` in a static initializer, deliberately
*not* annotated with `@Container`/`@Testcontainers`, so it survives across every test class in the
JVM rather than stopping when the first subclass's tests finish. `trading-gateway`'s
`KafkaPipelineTestBase extends PortfolioTestBase` and adds one static `ConfluentKafkaContainer` the
same way. Because the container — and the Spring context, order books and Kafka topics it holds —
is shared across the whole test run, every test that needs isolation gets it from its own account,
symbol, or strategy id, not from a fresh container. `SeededWorkloadIntegrationTest` goes one step
further than most: it reserves three symbols (`SEEDA`/`SEEDB`/`SEEDC`) that no other test ever
trades, because its correctness check compares the real matching engine's outcome against a
from-scratch reference computation that assumes an empty starting book — a resting order left
behind by another test on a shared symbol would make that reference wrong, even though positions
themselves are already isolated by strategy id.

`KafkaOutageIntegrationTest` and `DatabaseOutageIntegrationTest` are the exception: they need
exclusive control of the broker/database lifecycle (pausing a container mid-test), so they extend
`PortfolioTestBase` directly and bring their own reserved symbol (`OUTAGE`) rather than sharing
`KafkaPipelineTestBase`'s broker.

## Concurrency tests

* `OrderRouterTest`: simultaneous submissions for one symbol have exactly one owner and no lost
  orders; concurrent arrivals still resolve to FIFO time priority; different symbols are owned by
  different workers with non-colliding execution ids.
* `PortfolioUpdaterTest.concurrentApplicationsOfTheSameExecutionResultInExactlyOneAppliedCall`:
  the same execution applied from multiple threads at once still only moves the position once.
* `ExecutionsDedupTest.concurrentRedeliveryOfTheSameExecutionAppliesExactlyOnce`: the same
  race, isolated to the execution-pipeline consumer with no broker involved.
* `BufferedKafkaEventPublisherTest`: FIFO dispatch order through the single dispatcher thread;
  `close()` interrupts a dispatcher stuck inside a blocking producer call rather than hanging.

## Replay tests

Mechanism and rationale live in `replay-and-reconciliation.md`; this section only names the tests
that prove each mechanism actually works. Matching-engine journal replay:
`FileCommandJournalTest`, `OrderRouterRecoveryTest`, `MatchingRestartIntegrationTest`
(trading-gateway, a real process-level restart). Executions-topic replay into risk state:
`consumersReportThemselvesCaughtUpSoTheGatewayCanBecomeReady`
(`ExecutionPipelineIntegrationTest`), which asserts `ReplayReadiness.isCaughtUp` for every
consumer group. Position reconciliation: `PositionReconciliationTest`. Randomized, full-stack
replay of the same logical input twice: `SeededWorkloadIntegrationTest` (see the invariant table
above).

## Failure tests

`failure-matrix.md` is the authoritative table — 15 rows, each with an "Operational alert" column
that names the metric/log signal an operator would watch and, where one exists, the test that
proves the row's "Recovery" behavior. That column is about alerting, not test coverage, so a row
that doesn't name a test there is not necessarily untested — row 8 (duplicate order submission),
for instance, is proven by `OrderApiTest` even though its alert column only discusses ADR-004.
The one row that is a genuine, disclosed test gap is row 15 (a Kafka producer `send()` call
failing outright): no integration test forces a real per-send NACK, because doing so needs a
broker-side fault injection this stack does not have. Row 9 needs no dedicated test at all — its
ordering guarantee is structural (per-partition Kafka ordering, ADR-007), not a runtime condition
to reproduce. Row 13 points back at row 1's Kafka-outage coverage rather than duplicating it.

## Load tests

`benchmark`'s `LoadTestRunner` and its per-workload harnesses (`steady`, `burst`,
`single-hot-symbol`, `many-symbols`, `high-cancel-rate`, `high-match-rate`, `low-match-rate`, plus
the three Stage 10 network targets) measure throughput and latency percentiles, not correctness —
they are not part of the invariant table above. `benchmark`'s own 28 tests are unit tests of the
load-generation harness itself (argument parsing, latency recording, submission targets), not
load tests in their own right. Methodology, hardware disclosure and results live in
`benchmark-methodology.md` and `performance-engineering.md`.

## The end-to-end test

`ExecutionPipelineIntegrationTest` and `SeededWorkloadIntegrationTest` (both in
`trading-gateway`, both extending `KafkaPipelineTestBase`) are the platform's canonical
generate → submit → risk → matching → executions → Kafka → positions/P&L → persistence → verify
path the Master-Prompt asks Stage 11 to build. `ExecutionPipelineIntegrationTest` proves it with
fixed, named scenarios (a two-sided fill, a risk rejection reaching the audit trail, redelivery
idempotency, a self-trade, average-entry-price and realized P&L after a partial close, consumer
replay-readiness). `SeededWorkloadIntegrationTest` proves the same path holds as a general
property: it generates a seeded random order sequence, submits it over real HTTP through real risk
checks into the real matching engine, lets the resulting executions cross a real Kafka broker into
a real Postgres database, and asserts the durable result matches an independently computed
reference answer — not one hand-picked trade, but whatever the generated sequence happened to
produce — for every account touched, and that the risk engine's synchronous view agrees with the
durable one.

## How to reproduce a failure

Every test in this suite is a plain JUnit 5 test; a single class or method reproduces with:

```bash
mvn -pl <module> test -Dtest=<ClassName>
mvn -pl <module> test -Dtest=<ClassName>#<methodName>
```

For the seeded tests specifically, the seed is a literal constant in the test file itself — there
is nothing else to configure. A red run reproduces by rerunning the same command; the assertion
messages in `MatchingEngineInvariantTest` additionally name the exact trial index, command index
and order id that failed, so a failure found in CI is reproducible without needing CI's own logs.

## Known limitations

* No property-based testing library is used anywhere in this codebase. This is a deliberate
  choice, not an oversight: the hand-rolled seeded-`Random`-plus-trials style established by
  `PositionStateTest` before Stage 11 existed is what every randomized test in this document
  follows, rather than introducing a second idiom (jqwik or similar) alongside it.
* `trading-domain` has no test suite. Every type in it is a record or enum with at most one
  derived getter (`OrderSnapshot.executedQuantity()`) and zero branching logic; that arithmetic is
  exercised indirectly by every other module's tests. Adding a dedicated suite here would test the
  Java record/enum language feature, not this codebase.
* `execution-pipeline`'s new unit tests cover `EventIds`, `TradingEventValidator`,
  `BufferedKafkaEventPublisher`'s dispatch/overflow/close behavior, the retryable-vs-not-retryable
  classification in `KafkaPipelineConfiguration`, and execution dedup — they do not exercise the
  consumer/container/DLQ wiring itself, which needs a real broker to mean anything; that coverage
  is `trading-gateway`'s Testcontainers-based tests (`PipelineFailureIntegrationTest` and
  neighbors), not duplicated here.
* `SeededWorkloadIntegrationTest` generates 50 orders per run — small enough to settle within its
  20-second Awaitility window three times per test run, not a stress workload. It proves
  correctness under randomized input, not throughput; `benchmark`'s `LoadTestRunner` is the
  throughput tool, and the two are not substitutes for each other.
* `MatchingEngineInvariantTest`'s randomized command grammar does not generate `MARKET` replace
  attempts distinctly from `LIMIT` ones beyond what mixed-order generation already produces
  incidentally — `REPLACE_NOT_SUPPORTED` for market orders is covered by
  `MatchingEngineTest`'s fixed examples, not by a dedicated randomized case.
