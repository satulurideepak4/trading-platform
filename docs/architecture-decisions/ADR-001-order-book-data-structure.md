# ADR-001: Sorted price maps with insertion-ordered price levels

* Status: Accepted
* Date: 2026-08-18

## Context

Stage 1 needs deterministic price-time-priority matching, efficient best-price access, and direct
cancellation without introducing specialized collections before measurement. Orders at the same
price must retain arrival order, while buy and sell prices require opposite sort directions.

## Decision

Maintain one book per symbol with two `TreeMap<Long, PriceLevel>` instances:

* bids use reverse natural ordering so the highest price is first;
* asks use natural ordering so the lowest price is first;
* each `PriceLevel` uses `LinkedHashMap<Long, OrderState>`.

The linked map preserves insertion order for FIFO matching and supports expected `O(1)` removal by
order ID. A separate engine-wide hash map locates order state for cancel and replace commands.
Prices are positive `long` ticks rather than floating-point values or `BigDecimal` objects.

## Consequences

The representation is based entirely on JDK collections, is easy to inspect, and makes ordering
behavior explicit. Creating/removing a price level costs `O(log P)` and iterating makers at the best
level is constant work per execution. It does allocate collection nodes and boxes primitive keys,
so it is a correctness baseline rather than a claimed low-allocation final design.

Alternative structures such as arrays indexed by price, heaps, intrusive queues, primitive maps,
and custom off-heap books are deferred until realistic benchmarks and profiles demonstrate a need.
Arrays can offer fast access for bounded dense price domains but require instrument-specific bounds;
heaps complicate arbitrary cancellation and stable FIFO handling.

## Invariants

* Only active limit orders may be present in a price level.
* An active resting order appears exactly once.
* Empty price levels are removed.
* The first order in the best opposite price level is always the next maker.
* Any accepted replacement removes the old entry and appends the replacement with a new sequence.
