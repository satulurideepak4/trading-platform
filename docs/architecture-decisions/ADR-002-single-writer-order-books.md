# ADR-002: Single-writer ownership for order books

* Status: Accepted
* Date: 2026-08-18

## Context

Stage 2 must process independent instruments concurrently without allowing races inside price-time
priority state. Coarse locking around the whole engine would serialize unrelated symbols and make
lock contention part of the matching path. Fine-grained locks inside book structures would spread
concurrency concerns through correctness-sensitive code and make deterministic ordering harder to
reason about.

The system also needs explicit overload behavior. An unbounded executor queue could hide overload
until memory exhaustion.

## Decision

Hash each symbol to one stable worker. Each worker owns a private `MatchingEngine` and may own many
books, but no book has multiple writers. Producers offer commands to fixed-capacity
`ArrayBlockingQueue`s. The router retains order ownership so symbol-less cancel and replace commands
reach the same worker as submission.

A short router admission critical section establishes queue order and updates identity ownership.
It does not execute matching. Queue-full admission fails immediately and does not reserve command
identifiers. Workers assign local processing sequences at dequeue time.

Execution IDs use disjoint deterministic arithmetic sequences per worker rather than a shared
atomic counter. Replay requires the same command admission order and worker-count configuration.

## Consequences

* A book needs no internal locks, and the Stage 1 engine stays independent of threading libraries.
* Independent workers match concurrently; symbols mapped to one worker intentionally serialize.
* FIFO queue order gives a precise ordering point for simultaneous producers.
* Bounded queues expose overload and bound queued-command memory.
* Hot-symbol load cannot be solved by adding workers because one book still has one owner.
* The admission lock and `ArrayBlockingQueue` are measurable baseline contention points.
* Reconfiguring worker count changes routing and execution-ID allocation and therefore requires a
  controlled stop/replay or migration protocol not implemented in Stage 2.

## Alternatives considered

* **One coarse engine lock:** simple, but prevents unrelated books from progressing concurrently.
* **One lock per book:** permits concurrency but requires lock lifecycle, multi-map coordination,
  and a defined acquisition model for order-ID lookups and future cross-instrument operations.
* **Lock-free/specialized queues:** may improve latency but add complexity without baseline profiler
  evidence.
* **Dynamic book migration:** may help skew but needs a handoff barrier and recovery protocol to
  prove that exactly one owner exists throughout migration.

## Stage 9 addendum: measured coarse-lock comparison

This ADR named "the admission lock and `ArrayBlockingQueue`" as measurable baseline contention
points and listed "one coarse engine lock" as an alternative considered without profiler evidence.
Stage 9 measured it: a benchmark-local `CoarseLockedMatchingHarness` (one shared `MatchingEngine`,
one `ReentrantLock`, no sharding) against the real `OrderRouter`, same hardware, same workload,
500,000 and 1,500,000 orders. Full write-up, numbers, and JFR profiler evidence in
[performance-engineering.md § Experiment 2](../performance-engineering.md#experiment-2-coarse-lock-vs-single-writer-per-book-adr-002).

**Headline result**: the coarse lock outperformed `OrderRouter`'s single-writer-per-book queue
architecture on both throughput and every latency percentile, by roughly 2x at 1.5M orders. JFR
sampling attributes the difference to `OrderRouter`'s cross-thread admission machinery
(`CompletableFuture` completion, `ArrayBlockingQueue` offer/poll signaling) costing more than the
sub-microsecond matching work itself — not to the admission lock this ADR anticipated as the likely
contention point, which turned out to be cheap at the producer thread counts tested.

**This does not overturn the decision.** The coarse-lock harness has no admission queue (no bounded
backpressure signal), no journal integration (Stage 7's durability model assumes one worker owns
one book's write path), and does not parallelize matching across cores for CPU-heavier symbols —
irrelevant at current sub-microsecond per-command costs, but a real difference this ADR's
correctness/durability/backpressure goals depended on, not measured throughput in isolation. The
measurement is a real, now-quantified cost of this ADR's choice; it does not replace the reasons
the choice was made.
