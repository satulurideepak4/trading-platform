# Market-data engineering

A simulated exchange feed, cleaned up and normalized, fanned out over WebSocket, and used to keep
unrealized P&L honest between trades. Independent of the order-matching path end to end: nothing
here can slow down a matching worker, and nothing in matching or the gateway's order-admission path
depends on market data being healthy.

## Flow

```text
MarketSimulator (single generator thread, one process's worth of "the exchange")
      │  bid/ask/trade updates, deliberately imperfect: duplicates, out-of-order, gaps
      ▼
MarketDataProcessor
      │  validate → classify sequence (ADR-012) → update latest per-symbol state → normalize
      │
      ├─► MarketDataHub ── non-blocking per-subscriber fan-out (ADR-013)
      │        │
      │        ▼
      │   MarketDataWebSocketHandler ── /marketdata, one queue + drain thread per session
      │        │
      │        ▼
      │   subscribed clients
      │
      └─► MarkPriceUpdater ── @Scheduled poll of current snapshots, not a subscriber
               │
               ▼
          mark_prices (Postgres) ── source MARKET_DATA
```

Two independent consumers read `MarketDataProcessor`'s state: the WebSocket hub (event-driven, one
push per tick) and `MarkPriceUpdater` (time-driven, one poll per flush interval). Neither depends on
the other, and both depend only on `MarketDataProcessor`'s public `snapshot()`/`symbols()` — see
`MarketDataHub`'s Javadoc for why the hub itself has no reference to the processor at all.

## Why not Kafka

The target architecture (`Master-Prompt.md`) routes the normalized feed straight to
strategies/WebSocket, not through the durable event backbone Stage 4 built. Kafka stays exactly what
it already is — the execution event backbone — and does not become a hop on the tick path. Full
reasoning in [ADR-013](architecture-decisions/ADR-013-slow-consumer-handling.md).

## Sequencing, duplicates, out-of-order, gaps

`SymbolSequenceTracker` keeps one number per symbol — the last exchange sequence actually applied —
and classifies every incoming one against only that number: `IN_ORDER`, `DUPLICATE`,
`OUT_OF_ORDER`, or `GAP` (still applied, but flagged with how many sequence numbers were skipped).
State only ever moves forward. Full reasoning, including why this needs no per-symbol history
window, in [ADR-012](architecture-decisions/ADR-012-market-data-sequencing.md).

`MarketSimulator` injects duplicates, out-of-order records and gaps on purpose
(`FeedImperfectionPolicy`), so this classification is exercised by every run — the CLI smoke test
and the integration suite both report nonzero counts of each, not just zero.

## Backpressure and slow subscribers

Every WebSocket subscription gets its own bounded queue and its own drain thread
(`QueuedMarketDataSubscription`). Publishing is a non-blocking offer per subscriber; a slow one
drops its own oldest queued update and, past a configurable number of consecutive drops, is
disconnected. None of this can block the feed or another subscriber. Full reasoning in
[ADR-013](architecture-decisions/ADR-013-slow-consumer-handling.md).

A newly subscribed symbol is pushed its current state immediately (`SnapshotUpdates`, tagged
`SNAPSHOT`), so a reconnecting client is never waiting on the next tick to learn what a symbol is
doing right now.

## Serialization

`MarketDataCodec` is pluggable: JSON (Jackson, default) or a compact hand-rolled binary layout,
selected by `trading.marketdata.codec`. Neither the processor, the hub, nor the WebSocket handler
knows which is active. Full reasoning, including why binary is hand-rolled rather than Protobuf for
now, in [ADR-014](architecture-decisions/ADR-014-market-data-serialization.md).

The WebSocket control channel — `subscribe`/`unsubscribe` messages — is always JSON regardless of
the configured data codec; only the tick stream itself is subject to the choice.

## WebSocket protocol

Connect to `/marketdata`. No authentication — see "Why no authentication" below.

```json
// client → server
{"type": "subscribe", "symbols": ["AAPL", "MSFT"]}
{"type": "unsubscribe", "symbols": ["MSFT"]}
```

The server streams `NormalizedUpdate`s for every subscribed symbol, encoded with the configured
codec (text frames for JSON, binary frames for the binary codec):

```json
{
  "symbol": "AAPL",
  "type": "BID",
  "price": 19000,
  "quantity": 100,
  "exchangeSequence": 4821,
  "exchangeTimestamp": "2026-08-19T09:30:00.123Z",
  "processorSequence": 91234,
  "receivedAt": "2026-08-19T09:30:00.124Z",
  "outcome": "IN_ORDER",
  "gapSize": 0
}
```

`GET /marketdata/{symbol}` returns the same state as a point-in-time REST snapshot — no WebSocket
connection required — or `404` if the symbol has not been seen yet.

## Why no authentication

Every other endpoint — `/orders`, `/positions`, `/pnl`, `/risk/exposure` — carries per-account state
and needs to know who is asking. Market data does not: it is undifferentiated reference data, the
same way a real exchange's public feed has no notion of which client is reading it. `/marketdata/**`
and the WebSocket endpoint are deliberately left off `WebConfiguration`'s authenticated path list.

## Closing the Stage 5 gap: mark prices

Through the end of Stage 5, `mark_prices` was fed only by `PortfolioUpdater` writing the last traded
price inside the same transaction as the trade — accurate the moment a trade happens, frozen
afterward. `MarkPriceUpdater` closes that: on a fixed schedule
(`trading.marketdata.mark-price-flush-interval`), it reads every tracked symbol's current
`SymbolSnapshot`, computes a reference price — mid of best bid/ask when both exist, else last trade —
and upserts it with source `MARKET_DATA`, but only when that price actually changed since the last
flush.

This runs on a schedule rather than reacting to every tick deliberately: the feed can produce
hundreds of events per second across many symbols, and a database write per tick would make
ingestion rate-limited by Postgres for no benefit — nothing reads a mark price faster than the poll
interval anyway. See `MarkPriceUpdater`'s Javadoc, and
[position-calculation.md](position-calculation.md) for how the mark feeds into unrealized P&L.

`upsertMarkPrice`'s existing `WHERE mark_prices.as_of <= EXCLUDED.as_of` guard (Stage 5) already
means whichever source — a real trade or a market-data tick — has the more recent price wins; no
ordering between the two sources needed to be added.

`MarketDataPortfolioBridgeConfiguration` — the Spring wiring for this one piece — is conditional on
**both** `trading.marketdata.enabled` and `trading.pipeline.enabled`, because `PortfolioRepository`
only exists when the pipeline is on. Every other market-data bean is conditional on
`trading.marketdata.enabled` alone.

## Metrics

Exposed at `/actuator/metrics`.

| Metric | Answers |
| --- | --- |
| `trading.marketdata.received` | raw events ingested |
| `trading.marketdata.invalid` | events rejected by validation |
| `trading.marketdata.duplicate` | events at or before the last applied sequence |
| `trading.marketdata.out_of_order` | events superseded by a later one already applied |
| `trading.marketdata.gap` | sequence numbers skipped, summed across every gap |
| `trading.marketdata.subscribers` | active WebSocket subscriptions |

Per-subscription drop counts (`Subscription.droppedCount()`) are available in-process but not yet a
registered metric; see Known limitations.

## Tested scenarios

| Scenario | Test |
| --- | --- |
| Duplicate/out-of-order/gap classification | `SymbolSequenceTrackerTest` |
| Validation, normalization, snapshot merging | `MarketDataProcessorTest` |
| Snapshot-on-subscribe conversion | `SnapshotUpdatesTest` |
| Slow subscriber drops rather than blocks publish | `MarketDataHubTest.aSlowSubscriberDropsUpdatesRatherThanBlockingPublish` |
| Sustained overflow disconnects the subscriber | `MarketDataHubTest.aSubscriberStuckPastTheDropThresholdIsDisconnected` |
| One slow subscriber does not affect a healthy one | `MarketDataHubTest.oneSlowSubscriberDoesNotAffectDeliveryToAHealthyOne` |
| Unsubscribe stops delivery | `MarketDataHubTest.unsubscribingStopsFurtherDelivery` |
| JSON/binary codec round-trip and relative size | `MarketDataCodecTest` |
| Live WebSocket delivery, no authentication | `MarketDataIntegrationTest.aSubscribedWebSocketClientReceivesUpdatesWithNoAuthentication` |
| REST snapshot reflects the live feed | `MarketDataIntegrationTest.theRestSnapshotEndpointReflectsTheLiveFeedWithNoAuthentication` |
| Unrealized P&L moves with no second trade | `MarketDataIntegrationTest.theMarkPriceFeedMovesUnrealizedPnlWithoutASecondTrade` |
| Fault injection is exercised in bulk | `exchange-simulator`'s `MarketDataFeedSimulator` CLI, run against a fresh seed |

## Known limitations

* **The simulated book can be crossed.** `MarketSimulator` generates bid, ask and trade updates as
  independent events around a mid-price that keeps random-walking between them, not as a matched
  order book — so a bid update can land above a still-current ask update from a moment earlier, and
  `SymbolSnapshot` will report `bestBidPrice > bestAskPrice` until the next ask arrives. Observed
  directly in manual testing, not hypothetical. A real order book cannot cross; this simulated
  top-of-book feed can, briefly, and nothing here corrects for it. Fine for exercising sequencing,
  backpressure and the mark-price bridge, which is what this stage needs; not a realistic quote feed.
* `MarketSimulator` is one thread per process. It comfortably clears the rates this stage targets,
  but it is a smoke-test generator, not a benchmarked producer — see Stage 9 for controlled
  throughput numbers.
* A dropped update is gone; there is no replay of missed ticks beyond the current-state snapshot a
  new subscription receives. A durable, replayable market-data log is out of scope for this stage.
* One thread per active WebSocket subscriber. Fine at today's expected subscriber counts; a large
  subscriber count would need event-loop I/O rather than a thread per connection.
* Per-subscription drop counts are not yet exposed as a registered Micrometer metric, only via the
  in-process `Subscription.droppedCount()` — worth adding alongside the Stage 8 observability work.
* The binary codec has no schema evolution story: every reader and writer must change together. Not
  a problem with one producer and one consumer implementation in this repository; would need
  revisiting if either changed. See [ADR-014](architecture-decisions/ADR-014-market-data-serialization.md).
