# ADR-013: Per-subscriber bounded queues, drop-oldest, in-process fan-out

* Status: Accepted
* Date: 2026-08-19

## Context

Market data has one producer — the feed — and an unbounded number of consumers, each reading at
whatever rate its own network and processing allow. A slow WebSocket client is normal, not
exceptional: a browser tab in the background, a client on a congested link, a strategy doing more
work per tick than the feed produces ticks. The design has to decide what happens when one of them
falls behind, and it has to decide that without letting the slow one affect anyone else or the feed
itself.

There is a second, prior question: should the live tick path go through Kafka at all, given Stage 4
already built a durable, ordered, replayable event backbone?

## Decision

**Market data does not go through Kafka.** The target architecture (`Master-Prompt.md`) routes
`Market Data Processor → normalized feed → Strategies/WebSocket` directly, not through the durable
event path, and that matches how real market-data infrastructure is built: the primary tick feed to
strategies is direct pub/sub or multicast, not a durable broker hop, because a strategy reacting to
a stale price is worse than a strategy that occasionally missed one. Kafka remains exactly what
Stage 4 built it for — the execution event backbone — and gains no involvement in ticks. (Mark
prices *do* reach Postgres, but on a polling schedule out of `MarketDataProcessor`'s own state, not
by consuming a Kafka topic — see `docs/market-data.md`.)

**Every subscriber gets its own bounded queue and its own drain thread.** `MarketDataHub.publish`
loops over subscribers and does a non-blocking `offer` into each one's queue
(`QueuedMarketDataSubscription`), then moves on immediately. Delivery to the actual client — the
WebSocket `sendMessage` call, which can block on the network — happens only on that subscriber's own
thread. A slow subscriber's queue filling up is therefore invisible to every other subscriber and to
the feed: the `offer` for a full queue returns immediately either way.

**On overflow, drop the oldest queued update and keep the newest.** For market data, current state
is what matters — a client that reconnects or catches up cares what the book looks like *now*, not
the full history of what it missed. Displacing the oldest queued item for the newest one is a better
use of a fixed buffer than blocking or dropping the newest arrival.

**Sustained overflow disconnects the subscriber.** A queue that keeps overflowing is not merely
bursty, it is stuck or gone, and its queue and drain thread are pure cost from then on.
`maxConsecutiveDrops` (config: `trading.marketdata.max-consecutive-drops`) bounds how many drops in a
row are tolerated before the subscription closes itself.

**A newly subscribing client is not left waiting for the next tick.** `MarketDataWebSocketHandler`
pushes the symbol's current `SymbolSnapshot` (via `SnapshotUpdates`) the moment a subscription is
registered, from the handler's own thread — not through the queue. A reconnecting client sees
current state immediately rather than whatever arrives next.

## Consequences

* Publish is O(subscriber count) non-blocking offers; it never waits on I/O. Measured behaviour, not
  assumed: `MarketDataHubTest` proves a subscriber sleeping 50-100ms per update does not slow or
  starve a subscriber with no delay on the same hub.
* A dropped update is gone. There is no replay of missed ticks beyond the current-state snapshot a
  new subscription gets; a client that needs the exact sequence of everything it missed would need a
  different design (a durable, replayable market-data log, which is explicitly out of scope here —
  see ADR-012 on why gaps are flagged rather than backfilled).
* Every session's outbound writes are additionally wrapped in a
  `ConcurrentWebSocketSessionDecorator` (`MarketDataWebSocketHandler`), because both the drain
  thread's live-update sends and the handler thread's initial-snapshot sends can target the same
  session concurrently, and a raw `WebSocketSession` is not safe for that.
* Queue capacity and the drop threshold are both configuration (`trading.marketdata.subscriber-queue-
  capacity`, `trading.marketdata.max-consecutive-drops`), not constants, so an operator can trade
  memory for tolerance of burstier clients without a code change.
* One thread per active subscriber. Fine at the subscriber counts this stage targets; a large
  subscriber count would need a different model (shared event-loop I/O rather than a thread per
  connection) that this design does not attempt.

## Alternatives considered

* **Route market data through Kafka, consumed by a WebSocket bridge:** gets durability and replay for
  free, at the cost of a broker hop on the primary path to every strategy — exactly the latency real
  market-data infra avoids. Also makes the "one durable backbone" story from Stage 4 muddier: Kafka
  would then carry both the trade-of-record and a high-volume, disposable tick stream with very
  different retention needs.
* **One shared queue, one dispatcher thread (the `BufferedKafkaEventPublisher` pattern from Stage
  4):** works well when there is one downstream (Kafka) and ordering into it matters. Here there are
  many independent downstreams with independent speeds; a shared queue means one slow subscriber's
  backlog can starve the queue for everyone else waiting behind it.
* **Block a slow subscriber's `sendMessage` from the publishing thread:** simplest to write, and
  exactly the bug this ADR exists to avoid — one slow client would stall the feed for every other
  subscriber.
* **Drop the newest update instead of the oldest:** keeps history at the expense of currency: a
  client would fall further and further behind rather than seeing stale-but-bounded history plus the
  present.
