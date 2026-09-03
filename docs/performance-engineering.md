# Performance engineering

Every number below comes from an actual run in this repository's benchmark environment — see
[Benchmark methodology](benchmark-methodology.md) for the hardware/JDK disclosure and how to
reproduce any of it. Nothing here is estimated. Where an experiment's result contradicts what
might have been the more flattering expectation (Experiment 4, most sharply), it is reported as
measured, not adjusted or omitted — that is the discipline this stage exists to enforce.

## Measurement coverage

The master prompt's measurement checklist is throughput, p50/p95/p99/p99.9, maximum latency, CPU
usage, memory usage, allocation rate, GC pause, queue depth, and Kafka lag. Fully, systematically
covered across every workload below: throughput, p50/p95/p99/p99.9, max latency, queue depth (max
and average). GC pause is covered for Experiment 4 specifically, where it's the point of the
experiment.

CPU usage and heap usage were **not** captured for every workload run — only incidentally, where
JFR happened to already be running (the two 1.5M-order Experiment 2 recordings). Reported here
rather than omitted, but this is real coverage for two runs, not a systematic per-workload
measurement:

| Run | CPU (JVM user / system / machine total) | Heap used range |
|---|---|---|
| `router` (1.5M orders) | 35.9% / ~11.6% / ~82% (2 samples, ~1s apart) | 31.7 MB – 910.0 MB |
| `coarse-lock` (1.5M orders) | no sample captured — run finished (1.77s) before JFR's periodic CPU-load event fired | 31.0 MB – 728.0 MB |

The ~82% machine-total CPU figure includes whatever else was running on this shared development
laptop at the time (see `benchmark-methodology.md`), not just this JVM — consistent with that
document's disclosure that this is not an isolated benchmark rig.

**Allocation rate was not reliably captured and is not reported as a number.** The JFR recordings
include `jdk.ObjectAllocationSample` and `jdk.ThreadAllocationStatistics` events, but summing them
correctly (excluding JFR's own internal bookkeeping allocations, which show up in the same sample
stream) needs more careful per-thread reconstruction than this stage's benchmark tooling does
automatically. An early attempt at this materially undercounted (a few kilobytes over a run that
demonstrably allocates hundreds of megabytes, per the heap-used swings above) — publishing that
number would have been exactly the kind of fabricated-looking figure this stage exists to avoid, so
it is left unreported instead of guessed.

## Baseline numbers

### `MatchingEngine.submit()` in isolation (JMH)

Two shapes: pure insertion (orders that never cross, so the cost is purely book insertion) and
pure matching (each submission crosses exactly one resting counter-order). 2 forks, 5×1s warmup,
5×1s measurement each; full raw output in `benchmarks/results/jmh-results.json`.

| Benchmark | Throughput | Mean | p50 | p95 | p99 | p99.9 |
|---|---|---|---|---|---|---|
| Resting-order insertion | 1.20 Mops/s | 0.896 µs | 0.417 µs | 1.082 µs | 4.160 µs | 21.6 µs |
| Crossing order | 2.00 Mops/s | 2.281 µs | 0.167 µs | 0.458 µs | 1.958 µs | 28.7 µs |

Both distributions have a heavy tail (p99.9 more than 10x the median) driven by JIT warmup edges
within each measurement iteration and GC pauses — visible directly in the raw histograms in the
JSON output, not smoothed over here. The crossing benchmark's `@Setup(Level.Invocation)` resting
order (see its Javadoc) means its absolute numbers should be read relative to the resting-order
benchmark, not as an independent figure: JMH's own documented caveat that per-invocation setup has
real cost at this operation scale, even though the setup work itself is outside the timed method.

### `OrderRouter` under concurrent load (`LoadTestRunner`, `--target=router`)

All seven in-process workloads, baseline configuration (16 symbols, 4 workers, 8,192-deep queues,
500,000 orders, 4 producer threads):

| Workload | Throughput | p50 | p95 | p99 | p99.9 | Max queue depth | Executions |
|---|---|---|---|---|---|---|---|
| steady (50k/s paced) | 50,000/s | 4.63 µs | 18.17 µs | 215.83 µs | 27.9 ms | 968 | 386,761 |
| burst (unthrottled) | 447,014/s | 29.13 µs | 7.80 ms | 17.99 ms | 25.9 ms | 6,248 | 386,876 |
| single-hot-symbol | 50,000/s | 4.08 µs | 17.25 µs | 360.79 µs | 8.58 ms | 612 | 387,240 |
| many-symbols (64) | 50,000/s | 4.92 µs | 26.83 µs | 256.29 µs | 7.01 ms | 638 | 385,836 |
| high-cancel-rate (0.4) | 50,000/s | 4.33 µs | 18.75 µs | 139.96 µs | 5.52 ms | 219 | 211,652 |
| high-match-rate (±2 ticks) | 50,000/s | 4.42 µs | 14.96 µs | 90.21 µs | 2.66 ms | 40 | 419,069 |
| low-match-rate (±2,000 ticks) | 50,000/s | 4.63 µs | 16.71 µs | 118.79 µs | 2.46 ms | 75 | 387,185 |

Two things worth calling out rather than glossing over:

* **Burst is the only workload that saturates the target** — at 50,000 orders/s paced, every other
  workload's queue barely builds (max depth in the hundreds); unthrottled, the queue reaches
  6,248 deep and tail latency balloons to 26–30ms. This is exactly the shape Experiment 1 below
  digs into.
* **`high-match-rate`'s realized match rate is only modestly higher than `low-match-rate`'s**
  (83.8% of accepted orders executed at least once vs. 77.4%), even though the price band
  controlling it is 1,000x narrower (2 ticks vs. 2,000). Both sides draw from the same reference
  price independently, and at 500,000 orders across 16 symbols even a wide band produces enough
  overlap in the middle of the distribution for most orders to eventually cross. The match-band
  parameter reliably moves realized match rate in the intended direction, but not proportionally
  to its own width — reported as measured, not adjusted to look more dramatic.

### Slow downstream consumer (docker compose, real REST traffic)

Not an in-process `LoadTestRunner` run — see
[`benchmarks/results/slow-downstream-consumer-docker-compose-20260819.md`](../benchmarks/results/slow-downstream-consumer-docker-compose-20260819.md)
for the full account. Summary: under real burst REST traffic against the containerised gateway,
the default rate limiter (200/s steady, 400 burst) throttles producer-side throughput before
sustained Kafka consumer lag can build. One consumer group did show real, measured lag
(`audit-processor` on `trading_orders_v1`, briefly 783 records behind, the highest-volume topic),
and it drained fully within 10 seconds of the burst ending with no intervention.

## Experiment 1: worker/partition count

**Hypothesis**: more workers should increase throughput (more matching parallelism) up to the
point where matching stops being the bottleneck, then flatten or fall as coordination/scheduling
overhead grows.

**Before / Method**: `burst` workload (unthrottled, 500,000 orders, 4 producer threads — fixed
across the whole experiment) against the real `OrderRouter`, `--workers=1,2,4,8`.

| Workers | Throughput | p50 | p99 | p99.9 | Max queue depth | Saturated |
|---|---|---|---|---|---|---|
| 1 | 551,569/s | 9.66 ms | 34.36 ms | 35.01 ms | 8,192 (full) | 29,429 |
| 2 | 751,830/s | 106.58 µs | 38.96 ms | 40.14 ms | 8,870 | 3 |
| 4 | 507,280/s | 43.33 µs | 38.63 ms | 41.05 ms | 10,737 | 1 |
| 8 | 293,381/s | 17.50 µs | 3.17 ms | 5.51 ms | 227 | 0 |

**Profiler evidence**: not needed to explain the headline number here — the queue-depth and
saturation columns already tell the story directly. 1 worker saturates its queue outright (29,429
commands rejected for a full queue) because 4 producer threads can generate work faster than one
worker can drain it. 2 and 4 workers both admit essentially everything but sit on deep queues
(8,870/10,737 max), which is why their throughput figures are close but their p50s differ by
~2.5x — 4 workers spend more wall-clock time context-switching between smaller units of queued
work on an 8-core machine already running 4 producer threads plus the workers themselves.

**After**: 8 workers is the clear tail-latency winner — p99 drops from 38.6ms (4 workers) to
3.17ms, and the queue essentially never builds (max depth 227 vs. 10,737). Its *throughput* number
is the lowest of the four, though: on this 8-core machine, 4 producers + 8 matching workers + the
harness's own queue-depth sampler thread is 13 runnable threads contending for 8 cores, and that
oversubscription shows up as lower aggregate throughput even though each individual command is
processed faster once it's dequeued.

**Trade-offs**: this is a genuine trade-off, not a strictly-better setting. More workers than
producer+matching capacity can use concurrently trades raw throughput for dramatically better,
more predictable tail latency. Which one to pick depends on whether the system's actual bottleneck
is producer-side admission rate or per-command latency — a decision that belongs to the deployment
target's actual traffic shape, not to this benchmark.

**Conclusion**: `MATCHING_WORKERS=4` (the shipped default) is a reasonable middle point on this
8-core machine, not a proven optimum — 2 workers showed higher raw throughput here, and 8 showed
far better tail latency. Reconfiguring it, per ADR-002's own stated consequence, requires a
controlled stop/replay since worker count changes routing and execution-ID allocation; this
experiment did not attempt that migration, only measured the destination configurations.

## Experiment 2: coarse lock vs. single-writer-per-book (ADR-002)

**Hypothesis**: `OrderRouter`'s single-writer-per-book sharding (ADR-002) should outperform one
coarse lock guarding a single shared `MatchingEngine`, since it lets independent symbols match
concurrently across real CPU cores instead of serializing behind one lock.

**Before / Method**: `burst` workload, `--target=router` (4 workers, the shipped default) vs.
`--target=coarse-lock` (`CoarseLockedMatchingHarness` — one `MatchingEngine`, one `ReentrantLock`,
every symbol, built specifically for this comparison; see its Javadoc). Run at both 500,000 and
1,500,000 orders (the larger run additionally profiled with JFR):

| Target | Orders | Throughput | p50 | p99 | Max queue depth |
|---|---|---|---|---|---|
| `router` (single-writer, 4 workers) | 500,000 | 507,280/s | 43.33 µs | 38.63 ms | 10,737 |
| `coarse-lock` | 500,000 | 557,580/s | 0.67 µs | 104.88 µs | 0 |
| `router` (single-writer, 4 workers) | 1,500,000 | 476,278/s | 27.71 µs | 26.26 ms | 15,155 |
| `coarse-lock` | 1,500,000 | 846,558/s | 0.54 µs | 97.79 µs | 0 |

The result is the opposite of the hypothesis, and consistent at both scales: `coarse-lock` beats
`router` on both throughput and every latency percentile, by roughly 2x at the larger scale.

**Profiler evidence** (JFR, `-XX:StartFlightRecording=settings=profile`, 1.5M-order runs — full
recordings not checked in, reproducible via the command in `benchmark-methodology.md`): sampling
`router`'s execution, real CPU time is attributable to `OrderRouter`'s own async machinery, not to
matching:

```
9 samples  java.util.concurrent.CompletableFuture.postComplete()
9 samples  java.util.concurrent.ForkJoinTask.compareAndSetForkJoinTaskTag(...)   (CompletableFuture's internal completion-stack bookkeeping)
4 samples  java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.signal()  (ArrayBlockingQueue.offer's wake-up of a blocked poller)
```

None of these three appear anywhere in `coarse-lock`'s sample set. Its samples are dominated by
the load generator's own `produce()` loop and `MatchingEngine.match()` itself — the actual
business logic, with essentially nothing attributable to cross-thread coordination. `router`'s
`AbstractQueuedSynchronizer.signalNext` overhead (the coarse lock's own uncontended-acquire
bookkeeping) appears once in `coarse-lock`'s samples, at a fraction of the frequency.

**After**: the queue depth and profiler evidence together explain the reversal. Individual
`MatchingEngine.submit()` calls cost well under a microsecond (per the JMH baseline above). At that
scale, `OrderRouter`'s cross-thread handoff — offering a command to an `ArrayBlockingQueue`,
waking the worker thread, the worker completing a `CompletableFuture` that a different thread
observes — costs *more* than the matching work itself, and that handoff cost is exactly what a
purely synchronous, same-thread, lock-protected call avoids. With only 4 producer threads
contending on one `ReentrantLock`, its uncontended fast path is cheap enough that the lock itself
was never the bottleneck this experiment set out to find.

**Trade-offs**: this is not a case for abandoning ADR-002. `CoarseLockedMatchingHarness` has no
queue (so no bounded backpressure signal — a caller that gets through the lock always
completes synchronously, there is no "try again shortly" admission-control point), no journal
integration (Stage 7's durability model is built around one worker owning one book's write path),
and no independent per-symbol ordering guarantee under contention beyond whatever order threads
happen to acquire the lock in. It also does not scale matching across cores for genuinely CPU-bound
symbols — irrelevant at these sub-microsecond per-command costs, but not irrelevant if match
complexity ever grows. ADR-002 was optimizing for correctness isolation, durability integration,
and per-symbol backpressure, not raw synchronous throughput at this operation size; this experiment
measures a real cost of that choice, it doesn't invalidate the choice.

**Conclusion**: at current per-command cost (~1µs) and current producer/worker thread counts, the
single-writer-per-book queue architecture pays a real, measured latency and throughput tax
relative to a synchronous coarse lock — roughly 2x at 1.5M orders. ADR-002's own "Consequences"
section already named the admission lock and `ArrayBlockingQueue` as "measurable baseline
contention points"; this is that measurement. See the addendum on
[ADR-002](architecture-decisions/ADR-002-single-writer-order-books.md#stage-9-addendum-measured-coarse-lock-comparison)
for the decision-record-level summary.

## Experiment 3: queue implementation

**Hypothesis**: `LinkedBlockingQueue`'s two-lock algorithm (separate `putLock`/`takeLock`) should
reduce producer/consumer contention relative to `ArrayBlockingQueue`'s single shared lock, at some
throughput or latency cost from its per-node allocation.

**Before / Method**: `QueueComparisonHarness` (a minimal single-writer-per-book harness built
specifically so this comparison never has to touch `OrderRouter`'s own queue — see its Javadoc),
parameterized by queue factory, `--target=queue-array` vs `--target=queue-linked`, `burst`
workload, 4 workers, queue capacity 8,192 (the shipped default), 500,000 orders.

*A bug surfaced and was fixed before these are the numbers reported*: the harness originally
signaled a full queue by completing its returned future exceptionally rather than throwing
synchronously from `submit()`, which doesn't match `SubmissionTarget`'s documented saturation
contract — `LoadTestRunner` only recognizes a synchronous throw as a saturation event, so every
queue-full rejection was being miscounted as an ordinary `rejected` outcome instead of `saturated`.
Fixed in `QueueComparisonHarness.dispatch()` (`throw new QueueSaturatedException()` instead of
`future.completeExceptionally(...)`) before this experiment's numbers were captured.

| Queue | Throughput | p50 | p95 | p99 | Max queue depth | Saturated |
|---|---|---|---|---|---|---|
| `ArrayBlockingQueue` | 699,018/s | 193.50 µs | 22.51 ms | 39.33 ms | 15,242 | 413 |
| `LinkedBlockingQueue` | 889,463/s | 66.46 µs | 5.23 ms | 16.88 ms | 5,299 | 0 |

**Profiler evidence**: not separately captured for this experiment (JFR budget was spent on
Experiment 2, the narratively load-bearing one); the queue-depth and saturation columns are
themselves direct, measured evidence of `ArrayBlockingQueue` backing up further under the same
producer pressure.

**After**: `LinkedBlockingQueue` outperformed `ArrayBlockingQueue` here on every column — higher
throughput, lower latency at every percentile, shallower queues, zero saturation vs. 413 rejected
commands. This lines up with the textbook explanation: `ArrayBlockingQueue` uses one lock shared
by both `offer` and `poll`, so with 4 producer threads and 1 consumer thread per shard, offering
and polling compete directly for the same lock; `LinkedBlockingQueue`'s two-lock algorithm lets
producers and the consumer proceed independently except at empty/full boundaries.

**Trade-offs**: `LinkedBlockingQueue`'s better numbers come from per-node heap allocation on every
`offer` — more GC pressure per command than `ArrayBlockingQueue`'s pre-allocated backing array,
not visible in this experiment's own numbers but real, and it does not bound memory as tightly:
capacity is still enforced, but that capacity ties up whatever the JVM's allocator does for
`LinkedBlockingQueue.Node` on top of the pre-sized array `ArrayBlockingQueue` reuses.

**Conclusion**: swapping `OrderRouter`'s own `ArrayBlockingQueue` (`MatchingWorker.queue`) for a
`LinkedBlockingQueue` is a plausible, real, measured win under this specific producer/consumer
shape (many producers, one consumer, per shard) — but was deliberately not applied to
`OrderRouter` itself, per this stage's own scope: a production-code change belongs to a change with
its own review, not a side effect of a benchmark comparison. This is the evidence such a change
would cite, not the change itself.

## Experiment 4: G1GC vs. ZGC

**Hypothesis**: ZGC's sub-millisecond pause target should reduce tail latency relative to G1 under
sustained allocation pressure, at some throughput cost from its more concurrent (and more
CPU-hungry) collection work.

**Before / Method**: `burst` workload at 3,000,000 orders (large enough to force multiple GC
cycles regardless of collector — the baseline 500,000-order runs barely allocate enough to
trigger one), `--target=router`, `-Xlog:gc` capturing every collector's own pause/allocation log.
Default JVM ergonomics on this 16GB machine size the heap at `MaxHeapSize=4GiB` unless overridden.

| Collector | Heap | Throughput | p50 | p95 | p99 | p99.9 | Max latency |
|---|---|---|---|---|---|---|---|
| G1 (default) | 4GiB (ergonomic) | 521,282/s | 162.17 µs | 13.27 ms | 84.45 ms | 151.83 ms | 176.50 ms |
| ZGC | 4GiB (ergonomic) | 199,074/s | 1.31 ms | 250.63 ms | 357.83 ms | 5.91 **s** | 5.96 **s** |
| ZGC | `-Xmx8g` | 238,583/s | 3.54 ms | 210.61 ms | 382.60 ms | 892.09 ms | 956.00 ms |

**Profiler evidence**: the GC logs themselves, not a separate profiler run. G1's log shows 25 GC
events in the run: young-generation pause times grow with heap occupancy over the run, from 3.2ms
at the first collection up to 168.6ms by the 21st (heap around 3GB by then), no allocation stalls
at any point. ZGC's log
at the default (ergonomic) heap shows real `Allocation Stall` events — application threads
(`matching-worker-1/2/3`, a producer, the harness's own sampler thread) blocked for **2.3–4.1
seconds each** waiting for memory to be reclaimed, and one `Garbage Collection (Allocation Stall)
Aborted` event. Raising the heap to 8GB removed the stall events entirely but did not close the
throughput or latency gap with G1.

**After**: G1 outperformed ZGC substantially on every measured dimension at both heap sizes — 2.2x
higher throughput and orders of magnitude better tail latency at the default heap, still roughly
2.2x higher throughput and a materially better tail even with double the heap.

**Trade-offs**: this is the one experiment here where the result runs directly against ZGC's usual
low-latency reputation, and it is reported as measured rather than reconciled with that reputation.
The most likely explanation, based on the allocation-stall evidence: this workload's allocation
rate at 3M orders in a few seconds, combined with only 8 cores available for concurrent GC work
*and* 4 producer threads *and* up to 8 matching-worker threads all competing for the same cores, is
enough to outrun ZGC's concurrent reclamation on this specific machine — a resource-constrained
laptop, not the many-core server ZGC is more typically evaluated on. This experiment does not
attempt to isolate core count as its own variable (that would be a fifth experiment); it reports
what this 8-core machine, running everything else it was already running, actually did.

**Conclusion**: switching the shipped default from G1 to ZGC is not supported by this measurement,
on this hardware, for this workload — the opposite of the more commonly expected result. G1 stays
the default. This is exactly the kind of negative result the plan for this stage committed to
keeping rather than omitting.

## What this stage did not attempt

JSON-vs-Protobuf and REST-vs-TCP networking experiments are explicitly out of scope here — they
are Stage 10's own named topic ("Low-Latency Networking Experiment"), and attempting them now would
be scope creep into a stage that owns them. Answered, with real measured numbers, in
[Networking comparison](networking-comparison.md).
