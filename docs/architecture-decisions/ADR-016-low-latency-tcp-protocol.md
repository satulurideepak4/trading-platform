# ADR-016: A persistent TCP order-entry protocol alongside REST

* Status: Accepted
* Date: 2026-08-20

## Context

Every order-entry path so far has been REST over HTTP/1.1: one TCP connection (or a pooled one)
per request, JSON in and out, a new request for every submit/cancel/replace. That is the right
default for usability — any HTTP client can talk to it, no protocol library needed — but it is not
what a latency-sensitive venue connection looks like in practice: connection setup, header parsing
and JSON (de)serialization are all real, measurable costs on the hot path, and a fresh TCP
handshake (or even just a fresh request on a pooled connection) is overhead a persistent,
authenticated session pays once instead of on every order. The master prompt asks for this
explicitly as an experiment alongside REST, not a replacement for it, with one hard constraint:
measure before claiming anything, including that TCP is faster at all.

## Decision

**A persistent, authenticated, binary connection.** A client connects once, sends one `AUTH` frame
carrying its API key, and — once accepted — can submit, cancel and replace orders for the rest of
the connection's life without repeating identity or paying a new connection's setup cost per order.
Symmetric to REST: same `OrderIngressService`, same risk checks, same idempotency registry, same
matching engine. See `docs/tcp-protocol.md` for the exact wire format.

**Length-prefixed binary framing**, the same shape `matching-engine`'s `JournalCodec` already uses:
a 4-byte length, then that many payload bytes. Unlike the journal's own trusted files, a socket is
untrusted input from the kernel's perspective — `FrameIO` rejects an oversized declared length
before allocating anything for it, and every fixed-size read goes through
`DataInputStream.readFully`, not a single `read()` call, because TCP is a byte stream and a length
prefix (or a whole frame) can legitimately arrive split across several reads. A dedicated test
(`FrameIOTest`) proves this against a stream that deliberately hands back 1-3 bytes per call.

**Wire codes are protocol-owned, never `.ordinal()`.** `WireCodes` maps `Side`, `OrderType`,
`OrderStatus` and `RejectionSource` to explicit byte constants in both directions, independent of
how those enums are declared. This is a direct lesson from a real bug found and fixed earlier in
this project's history: `matching-engine`'s own journal once encoded `Side`/`OrderType` ordinals
with no protection against a future reordering silently changing what an existing record decoded
to. A network protocol has the same failure shape with a worse blast radius — every connected
client instead of one file — so this protocol does not repeat it.

**Frame parsing and command dispatch are two different threads.** A connection's reader thread only
parses; it hands the decoded command to a shared, bounded `ThreadPoolExecutor` with
`CallerRunsPolicy` (`TcpServer`) rather than calling `OrderIngressService` itself. `submit`/`cancel`/
`replace` block their caller until the matching engine responds, and if the reader thread made that
call directly, one connection could only ever have one command in flight — exactly what a
persistent, pipeline-capable connection is supposed to avoid, since a client's own `requestId` on
every command exists specifically to let it have several outstanding at once. `CallerRunsPolicy`
was chosen deliberately over an unbounded queue or a dropped command: when the shared pool is
saturated, the *submitting* connection's own reader thread ends up running the blocking call
itself, which stops that connection draining its socket's receive buffer, which makes that
specific client's own next write block at the kernel level. That is real, observable backpressure
produced by an actual mechanism, not a documented intention with nothing behind it.

**`TCP_NODELAY` is set explicitly**, on both the server (`TcpConnection`) and every load-testing/
benchmark client. Left at the JDK default, Nagle's algorithm can hold a small write (an ACK, most
of what this protocol sends) for tens of milliseconds waiting to coalesce with more outbound data
that may never come — enough to make a "low-latency" path measure worse than plain HTTP and produce
a misleading comparison. This is named here rather than left as an implicit default precisely
because it would otherwise invisibly decide the outcome of the measurement this stage exists to
make honestly.

**Unsolicited fills are pushed via a `TradingEventPublisher` decorator, not a Kafka consumer
group.** `OrderIngressService.applyExecutions()` already has both sides' account ids in-process, on
the matching worker thread, at the exact moment a fill happens, and publishes an `ExecutionCreated`
through the existing `TradingEventPublisher` interface right after. `ExecutionPushingTradingEventPublisher`
decorates whichever real publisher is active (Kafka-backed or no-op, chosen the same way today) and,
for `ExecutionCreated` only, pushes an `EXECUTION_PUSH` frame to any open TCP connection for the
**maker's** side only — the taker's own connection already has the same fact synchronously, inline
in its own `ACK`, and pushing it there too would be both redundant and racy (the async push and the
synchronous ack come from different threads with no ordering between them; an early version of this
protocol did push to both sides and a real end-to-end test caught the taker's own fill arriving as
an unsolicited push before the ack it was waiting for). Using the existing publisher interface
rather than a new Kafka consumer group means this works identically whether
`trading.pipeline.enabled` is true or false, and never pays a broker round trip for what should be
the fastest notification path in the system. It is explicitly best-effort — a full per-connection
outbound queue drops the oldest push rather than blocking (mirroring `QueuedMarketDataSubscription`/
ADR-013's own drop-oldest, disconnect-after-sustained-overflow shape) — because the fill itself is
already durable via the untouched Kafka pipeline and always answerable from `GET /orders/{orderId}`;
the push is a convenience, not the system of record.

**A separate Protobuf HTTP path (`/protobuf/orders`) exists alongside REST, not instead of it**,
for the JSON-vs-Protobuf half of this stage's comparison. Same `OrderIngressService`, a generated
`OrderEntryProto` schema instead of Jackson DTOs, one self-contained `OrderAckProto` for both
accepted and rejected outcomes (mirroring `ServerMessage.Ack`'s own accept/reject union, for the
same reason: a client checks one flag rather than needing two response types). A separate path
rather than content negotiation on `/orders`: the two paths' response object graphs are necessarily
different Java types, so there is no method body to usefully share, and a separate path avoids any
Spring MVC ambiguity in choosing a converter for one route serving two encodings. Its own
`@ExceptionHandler` returns `OrderAckProto` for failures too — `GatewayExceptionHandler`'s global
advice returns the JSON-only `ErrorResponse` type, which a Protobuf-negotiating client cannot accept
at all.

## Consequences

* No changes to `OrderIngressService`, `OrderRegistry`, `OrderController` or `TradingEventEmitter`.
  Every new path here is a transport/encoding layer in front of the same ingress pipeline every
  request already went through — the concrete meaning of "engineering experiment, not a rebuild."
* A TCP connection carries real, if bounded, state per connection (an outbound queue, two threads)
  for its whole life, unlike a REST request. `trading.tcp.max-connections` bounds how many can be
  open at once; `trading.tcp.idle-timeout` reclaims one that authenticated and then went silent.
* The maker-only push means a taker never receives an `EXECUTION_PUSH` for its own aggressing
  order — by design, not omission; see the Decision section.
* Real measured numbers for whether any of this is actually faster than REST, and what each layer
  (connection cost, serialization, framing) actually costs, are in
  `docs/networking-comparison.md` — this ADR is the design record, not the evidence.

## Alternatives considered

* **NIO selectors / one event loop instead of a thread per connection:** scales to far more
  concurrent connections with fewer threads, at real implementation complexity (partial reads,
  partial writes, and backpressure all become explicit state machines instead of blocking calls).
  Not attempted here — the master prompt's own framing ("an engineering experiment... not
  rebuilding the entire platform") argues for the simpler, more explainable blocking-IO-per-
  connection model, and this project's connection counts do not need the event-loop model's
  scaling properties to produce a fair, real comparison.
* **Route the async execution push through a new Kafka consumer group**, matching the four existing
  independent consumer groups' pattern exactly. Rejected specifically because it adds a broker
  round trip (and a hard dependency on Kafka reachability) to what should be the fastest
  notification path in the system, when the same data is already available synchronously,
  in-process, at the moment the fill happens; see the Decision section's discussion.
* **Content-negotiate `/orders` for both JSON and Protobuf** instead of a separate
  `/protobuf/orders` path. Rejected: the two encodings' response types are not the same Java
  object graph, so there is no shared method body to gain, and a shared route risks Spring MVC
  converter-selection ambiguity for an experimental path with no compensating benefit.
* **Push executions to both sides of every trade**, treating the taker's connection the same as the
  maker's. Tried first, found wrong by a real integration test: it produces a race between the
  synchronous `ACK` and the asynchronous push for the exact same fact, observable as a client
  seeing its own fill announced before the acknowledgment it was waiting for arrives.
