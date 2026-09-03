# Concurrency model

## Ownership and routing

`OrderRouter` computes `floorMod(symbol.hashCode(), workerCount)` and assigns the symbol to that
worker for the router's lifetime. A worker owns a private `MatchingEngine`; neither the engine nor
its books are exposed to producer threads. Cancellation and replacement commands contain only an
order ID, so the router retains an order-ID-to-worker mapping established when a submission is
admitted.

The configured worker count is part of the deterministic deployment configuration. Changing it
changes symbol ownership and the execution-ID allocation pattern, so replay must use the original
worker count.

## Ordering

Producer calls briefly enter an admission critical section. That section assigns a monotonically
increasing routing sequence, selects the worker, and performs a non-blocking queue offer. It never
runs matching logic. Each worker dequeues in FIFO order and assigns a worker-local processing
sequence immediately before invoking its engine.

Commands for one symbol always enter one queue and are processed in admission order. Commands for
different symbols on different workers may complete in either order. There is deliberately no
global execution order across independent instruments.

Every worker uses a deterministic, disjoint execution-ID sequence. With `N` workers, worker `i`
emits `i + 1`, `i + 1 + N`, and so on. This avoids a contended global counter and avoids duplicate
IDs while keeping replay stable for a fixed worker count.

## Backpressure and overload

Each worker uses a fixed-capacity `ArrayBlockingQueue`. Admission uses `offer`, never an unbounded
insert and never a wait. If the selected queue is full, the router throws
`OrderRoutingRejectedException` with reason `QUEUE_FULL`. The command did not enter the engine, and
its identifiers are not reserved, so an upstream component may retry it. Retry rate, backoff, and
maximum attempts belong to the future gateway stage; this layer does not retry blindly.

A hot symbol can saturate its owner while other workers remain available. This is intentional: the
router does not move a live book between workers because doing so would complicate ownership and
ordering guarantees. Queue-saturation counts identify that condition.

## Measurements

`OrderRouter.metrics()` exposes per-worker:

* current queue depth and configured capacity;
* admitted and saturated command counts;
* successfully processed and failed command counts;
* total queue wait time;
* total, average, and maximum processing time.

Each completed `RoutedResult` also includes queue-wait and processing nanoseconds. These use
`System.nanoTime()` and are operational measurements, not deterministic domain state. Micrometer
histograms and latency percentiles are intentionally deferred to the observability stage.

## Shutdown

Closing the router stops admission, asks each worker to drain its accepted queue, and waits up to ten
seconds. Commands accepted before shutdown normally complete. A worker that cannot drain within the
deadline is interrupted and its remaining queued futures fail explicitly.

## Baseline limitations

The admission section serializes queue admission across producers and the standard JDK queue uses
locks internally. Both are deliberate baseline choices. Stage 2 provides no durable journal, so a
process failure still loses in-memory books and queued commands. Worker rebalancing, durable replay,
histogram metrics, CPU affinity, specialized queues, and lock-free structures require measurement
or later-stage recovery infrastructure.
