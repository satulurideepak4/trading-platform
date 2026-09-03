# Matching algorithm

## Price-time priority

Each symbol has an independent order book:

* bids are ordered by descending price;
* asks are ordered by ascending price;
* orders within a price level retain engine arrival order.

An incoming limit order crosses when a buy price is greater than or equal to the best ask, or a
sell price is less than or equal to the best bid. An incoming market order crosses every available
opposite price. A trade always executes at the resting maker order's price.

For each match, the engine executes the smaller of incoming and resting remaining quantity, updates
both orders, emits an immutable `Execution`, and removes a completely filled maker. Matching
continues until the incoming order is filled, the book has no opposite liquidity, or the next price
does not cross.

An execution contains:

* deterministic execution ID and caller-supplied event timestamp;
* symbol, execution price, and quantity;
* buy and sell order/client-order IDs;
* maker and taker order IDs;
* both orders' remaining quantities after the execution.

These fields describe the state transition caused by each trade without requiring mutable order
objects. Lifecycle commands and executions together are the replay input; persistence is outside
Stage 1.

## Determinism

The engine is synchronous and deliberately not thread-safe. In Stage 2, its matching worker is the
sole owner and serializes commands. The engine does not use wall-clock time, random values,
unordered map iteration for matching, or external systems. Priority and execution IDs are
monotonic sequences driven solely by input order. Processing the same ordered commands with the
same worker-count configuration therefore produces equal results, books, and executions.

The router gives each worker a disjoint execution-ID arithmetic sequence. This retains uniqueness
without putting a shared atomic counter in the matching path. `RoutedResult.processingSequence`
records the actual FIFO dequeue order for replay analysis; nanosecond timing measurements are not
part of deterministic domain state.

## Complexity

Let `P` be the number of price levels, `K` the number of resting orders consumed by a command, and
`L` the number of price levels exhausted.

| Operation | Time | Notes |
| --- | --- | --- |
| Rest a limit order | `O(log P)` | Locate or create its sorted price level; FIFO insertion is `O(1)`. |
| Find best price | `O(log P)` | `TreeMap.firstEntry()` follows the tree to its boundary. |
| Match | `O(K + L log P)` | Each maker is visited once; exhausted levels are removed from the tree. |
| Cancel | `O(log P)` worst case | Order lookup and FIFO removal are `O(1)` average; empty-level removal is `O(log P)`. |
| Replace | cancel + match/rest | All accepted replacements lose priority and are treated as new takers. |
| Order lookup | `O(1)` average | Hash-map lookup by order ID. |

The analysis uses expected constant-time hash operations. No latency claims are made; measurement
and profiling are intentionally deferred.

## Stage 1 limitations

* No concurrency, symbol partition ownership, or command queue.
* No durable journal, snapshots, recovery, or external event publication.
* No tick-size/instrument master, trading calendar, account, or risk checks.
* No self-trade prevention or advanced order instructions such as IOC, FOK, or iceberg orders.
* Long integer prices are ticks with no currency-scale interpretation yet.
* In-memory identifiers and counters reset with a new engine instance.
