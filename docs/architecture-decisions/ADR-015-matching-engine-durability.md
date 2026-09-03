# ADR-015: Append-only per-worker journal; snapshotting deferred

* Status: Accepted
* Date: 2026-08-19

## Context

Every matching worker's book lives in a plain `HashMap` inside a `MatchingEngine` instance — nothing
about it survives the process. Stage 1 through 6 built genuine durability everywhere *downstream* of
a trade (Kafka's durable log, Postgres's positions and executions, reconciliation from execution
history) while the matching engine itself, the one component that actually decides what a trade is,
stayed exactly as durable as the JVM's uptime. That gap is what Stage 7 exists to close: "do not
pretend an in-memory matching engine can survive process loss without addressing state recovery."

Two things make the design space smaller than it looks. First, matching is deterministic — Stage 1's
own design goal, and proven for in-memory replay by
`OrderRouterTest.orderedReplayProducesIdenticalDomainResultsAndState`: the same ordered command
sequence always produces the same book and the same executions. Second, `OrderRouter` already knows
exactly when a caller is allowed to observe a command as successful — not at admission, but after
`MatchingWorker.processCaptured` has run it, since the `CompletableFuture` the caller is waiting on
does not complete until then.

## Decision

**An append-only journal, one file per worker, written on the worker's own thread immediately before
each command is applied — not at admission.** By the time any caller observes a command as
successful, it is already durably journaled, because that observation *is* the completion that fires
right after journaling in the same method. A command still sitting in the in-memory queue at crash
time was never acknowledged to anyone either; that is an ordinary "retry me," not a durability
violation, and it is what keeps journaling I/O off the latency-sensitive admission path.

**Recovery replays directly against the engine, never through the router's public API.** Replay
calls `engine.submit/cancel/replace` straight from `OrderRouter`'s constructor, bypassing the async
queue entirely and running synchronously in journal order, before the router starts accepting live
traffic. Nothing above the router — `OrderIngressService`, `TradingEventEmitter`, the Kafka publisher
— is ever involved in replay, so recovering the engine **never re-publishes an event a downstream
consumer already saw**. Because replay reproduces the identical command sequence, it also reproduces
identical execution ids (the engine's own counters advance exactly as they did the first time), so
even if some future change did wire replay into publishing, existing idempotent consumers would just
see harmless redelivery. See `docs/replay-and-reconciliation.md`.

**Hand-rolled binary encoding**, the same choice `market-data`'s `BinaryMarketDataCodec` made and for
the same reason (ADR-014): three flat command records, no schema-evolution need, no reason to add a
dependency `matching-engine` doesn't otherwise have. Full layout in `JournalCodec`.

**`FlushPolicy.EVERY_RECORD` — fsync after every command — is the default.** This is a recovery-
point-versus-latency trade-off, not a style choice, and it is made explicitly rather than left to
whatever the OS happens to do. The cost is one fsync per command on the worker thread; no throughput
number is claimed for it here, because none has been measured — that is Stage 9's job.
`FlushPolicy.BUFFERED` exists for tests and a future comparison, and is not used by `trading-gateway`.

**Sequence verification, not checksumming.** Each record carries a monotonically increasing
per-worker journal sequence. A gap anywhere but the last record is fatal (`JournalCorruptionException`)
— this writer never produces one, so seeing one means the file cannot be trusted. A torn trailing
record — a length prefix promising bytes that were never written, exactly what a crash mid-`append`
leaves behind — is silently dropped; everything before it is intact. Full corruption detection
(checksums per record) was considered and rejected as more machinery than this stage's actual risk
profile (a local disk, not a network) justifies.

**Snapshotting is evaluated here and explicitly deferred, not omitted by oversight.**
`MatchingEngine` has no bulk-state-import path today — `OrderState`'s only constructor takes a
`SubmitOrder`, not arbitrary saved fields — so a real snapshot needs new engine API surface with its
own correctness burden (getting a saved `PARTIALLY_FILLED` order's exact remaining quantity, status
and priority sequence back is not simpler than replaying the commands that produced it). Meanwhile
replay-from-empty is what this codebase's own smoke tests show to be fast: thousands of commands in
milliseconds. Building compaction before there is a measured reason to is the exact premature
optimization Stage 1's own rules warn against. **Trigger for revisiting this:** replay time at
startup becomes operationally significant — measured, not assumed, the same standard the rest of
this repository holds performance claims to.

**Opt-in at the `matching-engine` API, on by default in `trading-gateway`.** The module's existing
unit tests keep using the no-journal constructor — fast, unchanged, zero new failure surface for
tests that aren't about durability. The gateway wires a real journal directory by default
(`trading.matching.journal-enabled=true`) because shipping Stage 5/6 durability everywhere except the
one component that decides what a trade is would be exactly the thing this ADR exists to stop.

## Consequences

* A gateway restart (or a worse crash) no longer loses resting orders. Proven at two levels:
  `OrderRouterRecoveryTest` (matching-engine, in-process) and `MatchingRestartIntegrationTest`
  (trading-gateway, two independent `ApplicationContext`s against the same journal directory).
* One fsync per mutating command, on the worker thread that also does the matching. This is real
  added latency on the admission path's dependent processing, not free durability — disclosed here,
  not benchmarked here.
* **The journal grows without bound.** Nothing truncates or compacts it; every command a worker has
  ever processed stays on disk forever. Acceptable for now given the deferred-snapshot decision
  above, and a real operational limitation until that trigger is hit — see
  `docs/disaster-recovery.md`.
* Journaling only covers the three mutating commands. `findOrder`/`book` are read-only and need
  nothing recovered; a bug that accidentally journaled them would only waste disk, not lose
  correctness, but the code path deliberately never constructs a `JournalRecord` for them at all.
* Corruption detection is sequence-only. A record whose bytes were silently altered in place (bit
  rot, a misbehaving disk) without breaking the sequence would decode as some other valid-looking
  command and replay wrong, with nothing here to catch it. Local-disk bit rot is judged out of scope
  for this stage; a networked or replicated store would need to reconsider this.
* Recovery is synchronous at `OrderRouter` construction. A worker with a very large journal (once one
  exists — see the unbounded-growth point above) delays that worker's readiness, and by extension the
  whole router's, since construction does not return until every worker has replayed.

## Alternatives considered

* **Snapshot the book state instead of, or alongside, a command journal:** bounds recovery time
  independent of history length, but needs new `MatchingEngine` API to export and re-import exact
  order state, and a compaction/retention policy for old snapshots — real engineering the deferred
  decision above explains isn't justified by anything measured yet.
* **Journal at admission time, not on the worker thread:** would journal a command before it is
  known to even be valid, adds synchronous disk I/O to the router's admission lock, and — the
  decisive point — is unnecessary, since nothing observes a command as successful until the worker
  has already run it.
* **Route the journal through Kafka instead of a local file:** already durable, already ordered per
  partition, but adds a broker round trip to the matching path for a mechanism whose entire point is
  to work when the rest of the system, including Kafka, might not be reachable — the same reasoning
  ADR-013 uses to keep market data off Kafka, applied to something even more latency-sensitive.
* **Full per-record checksums:** stronger corruption detection, more machinery, more CPU per record,
  for a threat model (local disk bit rot) this stage does not treat as the priority; sequence
  verification already catches the failure mode replay actually needs to worry about — a gap.
