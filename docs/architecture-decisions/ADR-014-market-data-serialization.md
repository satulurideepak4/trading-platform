# ADR-014: Pluggable codec; hand-rolled binary now, not Protobuf

* Status: Accepted
* Date: 2026-08-19

## Context

Stage 6 asks for serialization to be pluggable "so we can benchmark JSON versus binary encoding
later" (Stage 9). That means two things have to be true today: there has to be more than one working
encoding, and switching between them has to be a configuration change, not a rewrite of the
processing or transport code.

The two realistic binary options are a hand-rolled fixed-layout encoding and Protobuf.

## Decision

**`MarketDataCodec` is an interface** (`market-data/.../codec/MarketDataCodec.java`) with two
implementations: `JsonMarketDataCodec` (Jackson, the default) and `BinaryMarketDataCodec` (a
hand-rolled fixed layout). Neither `MarketDataProcessor`, `MarketDataHub`, nor
`MarketDataWebSocketHandler` knows which one is active — the handler asks `codec.isText()` to decide
whether to send a `TextMessage` or a `BinaryMessage` and nothing else about it changes. Selection is
one property, `trading.marketdata.codec: json|binary`.

**The binary codec is hand-rolled, not Protobuf, for now.** No `.proto` compiler is wired into this
build, and `NormalizedUpdate` is one flat record with nine fields — generating code for a schema
compiler to solve is more machinery than the message needs today. The layout
(`BinaryMarketDataCodec`'s class Javadoc has the exact byte-for-byte format) is a length-prefixed
symbol followed by fixed-width fields, chosen to be exactly what `NormalizedUpdate` needs and nothing
else.

**No throughput claim is made for either encoding.** `MarketDataCodecTest` asserts the binary
encoding is *smaller* than JSON for the same update, because that is a static, structural fact about
the two formats. It does not assert either is *faster*, because that has not been measured under
load — doing so honestly is Stage 9's job, with JMH and a controlled workload, consistent with this
repository's standing rule against fabricated performance numbers (see the benchmarking discipline
already established for the matching engine and the execution pipeline).

## Consequences

* Adding a third codec — Protobuf, if a later stage decides the schema is complex enough to warrant
  it, or FlatBuffers, or anything else — is one new class implementing `MarketDataCodec` and one more
  branch in `MarketDataConfiguration.marketDataCodec`. Nothing upstream or downstream of the codec
  changes.
* The binary format has no schema evolution story. Adding a field means every reader and writer
  change together; there is no forward/backward compatibility the way a Protobuf schema with field
  numbers would give. Acceptable because this codec has exactly one producer and one consumer
  implementation, both in this repository, both deployed together — the situation Protobuf's
  cross-version compatibility exists to solve does not apply yet.
* The control channel (WebSocket `subscribe`/`unsubscribe` messages) is always JSON regardless of
  which codec is configured for data frames — see `docs/market-data.md`. Only the high-volume tick
  stream is subject to this choice.
* When Stage 9 does benchmark the two, `MarketDataCodec` is exactly the seam to measure across: swap
  the bean, run the same workload, compare. No other change is needed to run that experiment.

## Alternatives considered

* **Protobuf from the start:** gives schema evolution and a well-understood wire format, at the cost
  of a build-time code generator and `.proto` schema to maintain for a nine-field message. Left as
  the natural next step if the message grows enough fields, or enough downstream consumers outside
  this repository, to make schema evolution a real concern — a one-class change away given the
  `MarketDataCodec` seam.
* **JSON only, no binary codec at all:** would satisfy "the feed works" but not the explicit Stage 6
  requirement to make the choice pluggable and benchmarkable, and would foreclose Stage 9's
  comparison entirely.
* **A binary codec that also uses Jackson (CBOR/Smile via `jackson-dataformat-*`):** would have
  reused the existing `ObjectMapper`-based codec shape almost unchanged, at the cost of being a less
  interesting comparison point for Stage 9 than a hand-rolled fixed layout — CBOR is still
  self-describing and general-purpose, closer to JSON's trade-offs than to a truly compact wire
  format.
