# TCP order-entry protocol

Stage 10's experimental, persistent, binary order-entry protocol — enough detail here to
reimplement a client from scratch without reading `trading-gateway`'s source. See
`docs/architecture-decisions/ADR-016-low-latency-tcp-protocol.md` for *why* it's shaped this way;
this document is only the *what*. See `docs/networking-comparison.md` for measured results against
the existing REST/JSON and REST/Protobuf paths.

## Transport and framing

One TCP connection per client, kept open for the life of a session — not one connection per
request. `TCP_NODELAY` is set on both ends (see ADR-016 for why this matters).

Every message, in both directions, is one **frame**:

```
+----------------------+------------------------------+
| length (4 bytes, BE)  | payload (`length` bytes)     |
+----------------------+------------------------------+
```

`length` is a signed 32-bit big-endian integer (`DataOutputStream.writeInt` / `DataInputStream
.readInt`), the length of the payload that follows, not including itself. A server enforces a
configured `max-frame-length` (`trading.tcp.max-frame-length`, default 4096 bytes) and rejects a
larger declared length with a protocol error **before** allocating a buffer for it — the length
prefix is untrusted network input, not a trusted local file's own bookkeeping.

TCP is a byte stream, not a message stream: the kernel may split a single logical write across
several `read()` calls on the receiving end, including splitting the 4-byte length prefix itself.
Every implementation reading this protocol must use a fully-blocking read (`DataInputStream
.readFully`, or equivalent) for both the length prefix and the payload — never a single `read()`
call assumed to return everything at once. `FrameIOTest` in this repository proves the framing
survives a stream that hands back as little as 1–3 bytes per call.

## Message shape

The payload's first byte is always a message-type tag (see the table below); everything after it
is that message type's own fixed field layout, described in `OrderEntryCodec`'s class Javadoc
(reproduced here). All multi-byte numeric fields are big-endian (Java's `DataOutputStream`/
`DataInputStream` defaults). Strings are one **unsigned length byte** (0–255) followed by that many
UTF-8 bytes — no field this protocol carries is ever long enough to need more, the same convention
`matching-engine`'s `JournalCodec` uses for its own on-disk records.

### Message type tags

| Tag | Value | Direction | Meaning |
|---|---|---|---|
| `AUTH` | 0 | client → server | First frame on every connection |
| `AUTH_ACK` | 1 | server → client | Authentication accepted |
| `AUTH_REJECT` | 2 | server → client | Authentication rejected; connection closes after this frame |
| `SUBMIT` | 10 | client → server | Submit a new order |
| `CANCEL` | 11 | client → server | Cancel a resting order |
| `REPLACE` | 12 | client → server | Replace quantity/price of a resting order |
| `ACK` | 13 | server → client | Response to one SUBMIT/CANCEL/REPLACE |
| `EXECUTION_PUSH` | 20 | server → client | Unsolicited: this connection's resting order just filled |
| `ERROR` | 99 | server → client | Protocol-level problem not tied to one request |

Tag values are not sequential by design and are never derived from an enum's declaration order
(`ordinal()`) — see ADR-016 for the specific prior bug this guards against. A future message type
gets its own explicit number, not "whatever's next."

### Field layouts

```
AUTH           string  apiKey

AUTH_ACK       string  accountId

AUTH_REJECT    string  reason

SUBMIT         long    requestId       -- client-chosen, echoed back on the matching ACK
               string  clientOrderId
               string  symbol
               byte    side            -- WireCodes: BUY=0, SELL=1
               byte    orderType       -- WireCodes: LIMIT=0, MARKET=1
               long    quantity
               long    price
               string  strategyId      -- empty string means "use the account default"

CANCEL         long    requestId
               long    orderId         -- the gateway-assigned order id from that order's own ACK

REPLACE        long    requestId
               long    orderId
               long    quantity
               long    price

ACK            long    requestId       -- echoes the request this responds to
               long    orderId
               string  clientOrderId
               byte    duplicate       -- 0/1: an idempotent replay of an already-processed request
               byte    accepted        -- 0/1
               -- accepted == 1 only --
               string  symbol
               byte    side            -- WireCodes
               byte    orderType       -- WireCodes
               byte    status          -- WireCodes: NEW=0, PARTIALLY_FILLED=1, FILLED=2,
                                                       CANCELLED=3, REJECTED=4
               long    quantity
               long    remainingQuantity
               long    price
               long    createdAt (epoch second)
               int     createdAt (nano-of-second)
               long    updatedAt (epoch second)
               int     updatedAt (nano-of-second)
               int     executionCount
                         [executionCount times]:
                         long executionId
                         long price
                         long quantity
                         byte liquidity        -- WireCodes: MAKER=0, TAKER=1
                         long timestamp (epoch second)
                         int  timestamp (nano-of-second)
               -- accepted == 0 only --
               byte    rejectionSource -- WireCodes: RISK=0, MATCHING_ENGINE=1
               string  reason
               string  detail

EXECUTION_PUSH long    orderId
               long    executionId
               string  symbol
               long    price
               long    quantity
               long    remainingQuantity
               byte    liquidity       -- WireCodes; always MAKER=0 for a push (see below)
               long    timestamp (epoch second)
               int     timestamp (nano-of-second)

ERROR          string  message
```

## Session lifecycle

1. Client opens a TCP connection.
2. Client's **first frame must be `AUTH`**, carrying its API key, within `trading.tcp.auth-timeout`
   (default 5s). Anything else as the first frame, or silence past the timeout, closes the
   connection with no response.
3. Server replies `AUTH_ACK` (carrying the resolved `accountId`) or `AUTH_REJECT`. After a reject,
   the server closes the connection — there is no recovering a rejected auth on the same socket.
4. Once authenticated, the client may send any number of `SUBMIT`/`CANCEL`/`REPLACE` frames without
   waiting for each one's `ACK` first — this is what makes the connection *pipelined*, not
   request/response like REST. Each command carries a client-chosen `requestId`; the matching `ACK`
   echoes it back, which is how a client correlates responses that may arrive out of submission
   order under concurrent load.
5. If the connection goes idle (no frames in either direction) longer than
   `trading.tcp.idle-timeout` (default 5m), the server closes it.
6. An oversized or malformed frame gets a synchronous `ERROR` frame, then the connection closes.

## What never appears on the wire

Neither an `ACK`'s executions nor an `EXECUTION_PUSH` ever carries the counterparty's order id,
client order id, or account — only this connection's own account's side of the fill. This mirrors
`OrderResponse.ExecutionResponse`'s existing scoping on the REST path exactly; see `ServerMessage`'s
class Javadoc for the full rationale. A client cannot reconstruct who was on the other side of its
own trade from this protocol, by design.

## Unsolicited pushes: best-effort, not the system of record

`EXECUTION_PUSH` is sent to whichever connection (if any) is authenticated as the account whose
*resting* order (the maker side) was just filled by a command arriving on any connection or over
REST — never to the taker's own connection, since the taker already learns about the fill
synchronously in its own `ACK`. See `ExecutionPushingTradingEventPublisher`'s Javadoc for exactly
how this is wired to the matching pipeline.

This channel is intentionally best-effort: each connection has a small bounded outbound queue: a
slow reader gets its oldest undelivered push dropped rather than the connection's writer thread
blocking indefinitely, and a connection that stays behind past `max-consecutive-drops` is
disconnected outright. The fill itself is already durable through the untouched Kafka pipeline and
always queryable via `GET /orders/{orderId}`; the push exists purely to shave latency off finding
out, and losing one changes nothing about the fill's correctness.

## Configuration reference

All under `trading.tcp.*` (see `TcpProperties`); every property has a `TCP_*` environment variable
override, documented in `.env.example`.

| Property | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Whether the TCP listener starts at all |
| `port` | `9090` | Listener port inside the container (mapped to host `9091` by default in `docker-compose.yml`, to avoid colliding with Prometheus's own `9090`) |
| `max-connections` | `512` | Hard cap; the acceptor refuses beyond it |
| `dispatch-pool-size` | `8` | Shared thread pool size that runs commands parsed off every connection |
| `dispatch-queue-capacity` | `2048` | Bounded queue in front of the dispatch pool |
| `outbound-queue-capacity` | `256` | Per-connection bounded queue for pushes/acks waiting to be written |
| `max-consecutive-drops` | `50` | A connection is disconnected after this many consecutive dropped outbound messages |
| `auth-timeout` | `5s` | How long a connection has to send `AUTH` before being closed |
| `idle-timeout` | `5m` | How long a connection may go without any frame before being closed |
| `max-frame-length` | `4096` bytes | Largest declared frame length accepted before it's rejected |

## Reference implementation

`OrderEntryCodec` (encode/decode), `FrameIO` (framing), `WireCodes` (enum mappings) — all in
`trading-gateway/src/main/java/com/tradingplatform/gateway/tcp/protocol/` — are the canonical
implementation this document describes. `benchmark/.../load/TcpBinarySubmissionTarget.java` is a
second, independent client implementation (used for load testing) that never imports the server's
connection-handling code, only the shared protocol package — a useful check that the wire spec
above is actually sufficient to build a client from.
