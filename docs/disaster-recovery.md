# Disaster recovery

A runbook per component: what's durable, what isn't, and what to actually do. RPO (recovery point —
how much could be lost) and RTO (recovery time — how long until healthy) are stated qualitatively
where no benchmark backs a number. This repository does not publish performance numbers it hasn't
measured; the same rule applies here; see `docs/performance-engineering.md` once Stage 9 exists.

## Matching engine

**Durable:** the order book, per worker, via the journal introduced in Stage 7 (ADR-015).
**Not durable:** nothing else — the book is the entire state that matters here.

**RPO:** zero for any command a caller ever saw as successful. `FlushPolicy.EVERY_RECORD` fsyncs
before the caller's `CompletableFuture` completes, so "acknowledged" and "durable" happen together. A
command still in a worker's in-memory admission queue at crash time was never acknowledged, so its
loss is an ordinary retry, not an RPO violation.

**RTO:** one journal replay per worker, done synchronously in `OrderRouter`'s constructor before any
worker starts accepting traffic. No number is published because none has been measured under
load — see ADR-015's snapshot-deferral reasoning for what would trigger measuring and bounding it.

**Recovery procedure:**
1. Restart the process (or the container/pod) with `trading.matching.journal-directory` pointed at
   the same durable volume the crashed instance used.
2. Nothing else — replay happens automatically before the gateway accepts requests.
3. Verify with `GET /orderbook/{symbol}` for any symbol known to have resting orders before the
   crash.

**If the journal itself is lost or corrupted** (volume loss, not a process crash): there is no
snapshot to fall back to (ADR-015). The book for every affected worker starts empty. This is a real
gap, not a hidden one — see Known limitations below.

## Gateway control-plane state

**Durable:** nothing. The order registry (idempotency), rate-limit buckets, and the risk engine's
*own* in-memory copy of positions are all plain in-memory structures.

**Not durable, with mitigation:** risk positions are rebuilt automatically by replaying the Kafka
executions topic from the beginning (Stage 4's `ReplayReadiness` — the gateway reports itself out of
service until that replay catches up, so it never checks a limit against half-rebuilt state).

**Not durable, no mitigation:** the idempotency registry, rate-limit buckets, and the order-id
allocator inside `OrderRegistry`. A client retry that races the exact restart window can duplicate
an order or get an extra burst of rate-limit budget. The order-id allocator is the sharper case:
it restarts counting from 1 regardless of what the matching engine recovers, so **the first order
submitted after a restart with any recovered resting orders is guaranteed to collide** —
`DUPLICATE_ORDER_ID`, not a request that silently succeeds twice, but still a rejection an
otherwise-healthy client will see. Reproduced directly: submit an order, `kill -9` the gateway,
restart it against the same journal directory, submit any new order — it fails before a second
attempt with a fresh id succeeds. Narrow window for the idempotency registry, unconditional for the
id allocator; neither is fixed — see Known limitations.

**RPO/RTO:** risk state — RPO zero (rebuilt from Kafka, the durable record of truth), RTO bounded by
how much of the executions topic there is to replay. Idempotency/rate-limit state — RPO is "whatever
was in memory," effectively unbounded for those specific structures.

**Recovery procedure:**
1. Restart the process.
2. Wait for `/actuator/health` to report ready — this is `ReplayReadiness` confirming the risk
   projection has caught up.
3. Do not resume traffic before step 2; an order admitted against a stale risk view could exceed a
   limit no one is checking yet.

## Kafka

**Durable:** everything published — `trading.orders.v1` and `trading.executions.v1` and their DLQs,
retained per the broker's own retention policy (not configured by this application; an operational
decision for whoever runs the cluster).

**Not durable:** whatever never made it out of `BufferedKafkaEventPublisher`'s in-process buffer
during an outage that outlasted the buffer's capacity — see the failure matrix, row 1.

**RPO:** zero for anything actually published. Unbounded (within the buffer's capacity) for what
wasn't, during a sufficiently long outage.

**RTO:** however long broker recovery itself takes — outside this application's control. Once the
broker returns, every consumer resumes from its last committed offset automatically; no manual step.

**Recovery procedure:**
1. Restore broker availability (infrastructure concern, not application).
2. Confirm consumer lag is draining: `kafka.consumer.fetch.manager.records.lag.max` trending down
   for every group.
3. If the outage was long enough that events were dropped (`trading.events.dropped` > 0 during the
   window), run `PositionReconciliation.check()` — see Replay and reconciliation — to find out
   whether any trade never reached Postgres.

## PostgreSQL

**Durable:** executions and positions, per Stage 5's schema and Flyway migrations.

**Not durable:** nothing in the application layer — this is the durable store everything else
reconciles against.

**RPO/RTO:** governed by whatever backup/replication strategy the database itself runs (not
something this application configures). Application-level RPO against Postgres being reachable at
all is zero: consumers retry indefinitely on `TransientPersistenceException` rather than
dead-lettering, per the failure matrix's row 2.

**Recovery procedure:**
1. Restore the database (from replica promotion, backup restore, or simply the same instance coming
   back — an infrastructure decision outside this repository's scope).
2. Run Flyway's own migration check if this is a restored/rebuilt instance rather than the same one
   (`spring.flyway.baseline-on-migrate: false` — the gateway refuses to serve traffic against a
   schema nobody has verified, rather than guessing).
3. Run `PositionReconciliation.check()`. If a restore lost committed-but-not-yet-consumed writes
   relative to Kafka's own record, this is what surfaces the drift; `repair()` fixes it from
   execution history, the record of truth.

## Market data

**Durable:** nothing, deliberately (ADR-013). The feed is disposable by design — a lost tick is a
lost tick, not a lost trade.

**RPO/RTO:** not applicable in the traditional sense. A restarted feed starts from whatever the
simulator's seed produces next; a reconnecting WebSocket client gets an immediate current-state
snapshot rather than replayed history (`SnapshotUpdates`).

**Recovery procedure:** restart the process. Nothing to restore.

## Known limitations

* Gateway idempotency and rate-limit state are not durable; see the gateway section above. Fixing
  this would mean the gateway's own control-plane state needs a store of its own — out of scope for
  Stage 7, whose durability work was specifically the matching engine.
* The matching-engine journal has no backup story beyond whatever the durable volume it lives on
  provides. Losing that volume without a snapshot mechanism (deferred, ADR-015) means losing the book
  for every affected worker.
* No number in this document is a benchmark result. Where an RTO is described as "however long X
  takes," that is an honest statement that X has not been measured, not an estimate dressed up as
  one.
