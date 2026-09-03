# Trading gateway API

The gateway is the only way into the matching engine. Every request is authenticated, rate limited,
validated, de-duplicated and risk checked before a command reaches a matching worker.

## Request pipeline

```text
HTTP request
     │
     ▼  CorrelationIdFilter          establish or accept X-Correlation-Id
     ▼  ClientAuthenticationInterceptor
     │      authenticate  ─────────► 401 unrecognised API key
     │      rate limit    ─────────► 429 + Retry-After
     ▼  bean validation   ─────────► 400 malformed or inconsistent request
     ▼  OrderRegistry.reserve
     │      duplicate, same content ► replay the stored outcome (200)
     │      duplicate, new content  ► 409
     ▼  PreTradeRiskEngine.reserve ─► 422 with the failing rule
     ▼  OrderRouter.submit
     │      queue full    ─────────► 503 + Retry-After
     ▼  matching worker
     ▼  settle risk state, publish the outcome
     ▼  201 created / 200 replayed
```

Ordering is deliberate: identity before risk, so a duplicate never runs risk twice; risk before
routing, so a rejected order never occupies queue capacity.

## Authentication

Every trading endpoint requires `X-Api-Key`. Keys map to an account through the `GATEWAY_API_KEYS`
environment variable (`apiKey:accountId` pairs, comma separated). The gateway refuses to start with
no keys configured rather than running with an accidental identity.

Only the account id travels beyond authentication. Keys are never logged.

## Correlation

Send `X-Correlation-Id` to have it echoed on the response and attached to every log line for the
request. A missing or unsafe value is replaced with a generated UUID. Accepted shape is
`[A-Za-z0-9._-]{1,64}`, because the value ends up in log output.

Log lines carry `correlationId` and `accountId` from the MDC, plus `orderId`, `clientOrderId` and
`account` as explicit fields where relevant. A submission is settled on the matching worker thread,
which has no MDC of its own, so the correlation id is carried across explicitly — one id therefore
covers the whole path, including the lines written after the request thread has returned.

Setting `LOG_LEVEL=DEBUG` adds a `command_routed` line per command with the worker id, routing and
processing sequence numbers, and queue and processing times:

```text
correlationId=trace-me-42 event=command_routed operation=submit orderId=1 workerId=0 \
  routingSequence=1 processingSequence=1 queueWaitMicros=90 processingMicros=1042
correlationId=trace-me-42 event=order_accepted account=ACC-DEV orderId=1 clientOrderId=t-1 \
  symbol=AAPL status=NEW remainingQuantity=10 executions=0
```

## Endpoints

### `POST /orders`

```json
{
  "clientOrderId": "strategy-a-0001",
  "symbol": "AAPL",
  "side": "BUY",
  "type": "LIMIT",
  "quantity": 100,
  "price": 19050,
  "strategyId": "momentum-1"
}
```

Prices are integer ticks. `price` must be positive for `LIMIT` and absent or zero for `MARKET`.
`clientOrderId` is at most 32 characters of `[A-Za-z0-9._-]`. `strategyId` is optional and defaults
to `DEFAULT`; it is the attribution positions and P&L are reported under (see `GET /positions`), so
an account running more than one book should set it to keep them apart.

`201 Created` for an order this request created, `200 OK` when replaying an earlier identical
request, distinguished by `duplicate` in the body:

```json
{
  "orderId": 41,
  "clientOrderId": "strategy-a-0001",
  "symbol": "AAPL",
  "side": "BUY",
  "type": "LIMIT",
  "status": "PARTIALLY_FILLED",
  "quantity": 100,
  "remainingQuantity": 60,
  "executedQuantity": 40,
  "price": 19050,
  "createdAt": "2026-08-18T09:30:00.123Z",
  "updatedAt": "2026-08-18T09:30:00.123Z",
  "executions": [
    {
      "executionId": 7,
      "price": 19040,
      "quantity": 40,
      "liquidity": "TAKER",
      "timestamp": "2026-08-18T09:30:00.123Z"
    }
  ],
  "duplicate": false
}
```

`liquidity` is relative to this order: `MAKER` if it was already resting, `TAKER` if it aggressed.

### `DELETE /orders/{orderId}`

Cancels a working order. `200` with the cancelled order, `409` if it is no longer active, `404` if
it does not belong to the caller.

### `PUT /orders/{orderId}`

```json
{ "quantity": 150, "price": 19040 }
```

`quantity` is the replacement's **total** quantity, including anything already executed. A
replacement always loses time priority, and is risk checked like a new order because it can raise
exposure. Limit orders only.

### `GET /orders/{orderId}`

Returns the current state of one of the caller's own orders.

### Protobuf and persistent TCP order entry (Stage 10)

`POST/DELETE/PUT /protobuf/orders(/{orderId})` mirror the four endpoints above exactly — same
request pipeline, same authentication, same idempotency and risk checks — encoded with a generated
Protobuf schema instead of JSON, and returning one self-contained `OrderAckProto` for both accepted
and rejected outcomes rather than the JSON path's `OrderResponse`/`ErrorResponse` split. A separate
experimental persistent TCP protocol also exists on its own port, entirely outside this HTTP
pipeline. Neither is documented in detail here; see [TCP protocol](tcp-protocol.md) for the wire
format and [Networking comparison](networking-comparison.md) for how they measure against this
JSON path.

### `GET /orderbook/{symbol}?depth=10`

Aggregated depth by price level, best price first. Individual resting orders are not exposed.
`depth` defaults to 10 and is capped at 100.

```json
{
  "symbol": "AAPL",
  "bids": [{ "price": 19040, "quantity": 250, "orderCount": 3 }],
  "asks": [{ "price": 19050, "quantity": 100, "orderCount": 1 }]
}
```

### `GET /positions`

Every position the caller's account holds, across all strategies, sorted by strategy then symbol.
Reads from the durable Postgres projection, so it can lag a just-filled order by however long the
Kafka round trip and the portfolio consumer take — a second or so under normal load. For the
state a new order would be checked against *right now*, use `/risk/exposure` instead.

```json
[
  {
    "strategyId": "DEFAULT",
    "symbol": "AAPL",
    "netQuantity": 60,
    "averageEntryPrice": 19000.0,
    "markPrice": 19200,
    "realizedPnl": 800,
    "unrealizedPnl": 12000,
    "totalPnl": 12800,
    "boughtQuantity": 100,
    "soldQuantity": 40,
    "executionCount": 2,
    "updatedAt": "2026-08-19T09:30:01.436847Z"
  }
]
```

`netQuantity` is signed: positive is long, negative is short. `averageEntryPrice` is `null` for a
flat position — an average price of nothing is not a number. `markPrice` is `null` until the
instrument has traded or had a mark set manually, in which case `unrealizedPnl` is reported as `0`
rather than as a loss against a price of zero. See
[Position calculation](position-calculation.md) for the exact-arithmetic model behind these numbers.

### `GET /executions?limit=50&offset=0`

The caller's own trade history, most recent first. `limit` is 1–500 (default 50).

```json
[
  { "executionId": 91, "symbol": "AAPL", "side": "BUY", "price": 19050, "quantity": 4,
    "liquidity": "TAKER", "occurredAt": "2026-08-19T09:30:01.436847Z" }
]
```

`side` and `liquidity` are relative to the caller's own order, never the counterparty's. The
counterparty's account is never included: a venue does not reveal who was on the other side of a
trade, and there is no reason for this one to.

### `GET /pnl`

Account-level realized, unrealized and total P&L, plus the per-position detail it was summed from
(the same shape as `/positions`).

```json
{ "accountId": "ACC-1", "realizedPnl": 800, "unrealizedPnl": 12000, "totalPnl": 12800,
  "positions": [ /* … */ ] }
```

### `GET /risk/exposure`

The account's current **pre-trade risk state** — what the next order would be checked against right
now. Unlike the three endpoints above, this reads the in-memory, synchronous view rather than
Postgres, so it reflects an order accepted a moment ago with no wait.

```json
{
  "accountId": "ACC-1",
  "openOrders": 3,
  "exposures": [
    { "symbol": "AAPL", "netPosition": 40, "workingBuyQuantity": 20, "workingSellQuantity": 0,
      "longExposure": 60, "shortExposure": 40 }
  ]
}
```

`longExposure`/`shortExposure` are the worst-case positions if every working buy filled and none of
the sells did, and vice versa — the numbers `RISK_MAX_POSITION_QUANTITY` is actually checked
against. See [ADR-003](architecture-decisions/ADR-003-pre-trade-risk-architecture.md).

### `PUT /instruments/{symbol}/mark-price`

```json
{ "price": 19200 }
```

Sets the price unrealized P&L is measured against for an instrument. `204` on success, `404` for an
unknown instrument. Originally a placeholder ahead of Stage 6's market-data feed and still useful on
its own merits: the last trade and the live feed (see [market-data.md](market-data.md)) both mark a
position automatically, but this remains the way to mark an instrument that has gone quiet on both —
no trades and no market-data symbol configured for it. It trusts the caller completely — there is no
plausibility check against the last trade or the feed.

## Status codes

| Status | Meaning | Retry? |
| --- | --- | --- |
| 200 | Success, or a replay of an earlier identical submission | — |
| 201 | This request created the order | — |
| 400 | Malformed body, or a field combination that cannot be an order | No, fix the request |
| 401 | Missing or unrecognised API key | No |
| 404 | Unknown order, unknown instrument, or an order belonging to another account | No |
| 409 | clientOrderId reused with different content, or the order is no longer active | No |
| 422 | Well formed but refused by a pre-trade risk rule | Not until the account changes |
| 429 | Rate limit exceeded | Yes, after `Retry-After` |
| 503 | Matching queue saturated; the command was never admitted | Yes, same clientOrderId is safe |
| 504 | Admitted but no outcome within the timeout; the result is unknown | Yes, same clientOrderId returns the real outcome |

An order belonging to another account answers `404` rather than `403` so the gateway does not
confirm that an order id exists to a caller who is not entitled to know.

Errors carry the layer that refused the request, so a client can act on it without parsing prose:

```json
{
  "source": "RISK",
  "reason": "MAX_ORDER_NOTIONAL_EXCEEDED",
  "message": "notional 200000000 exceeds limit 100000000",
  "orderId": 42,
  "correlationId": "9f2c1b7e-..."
}
```

A risk-rejected order still has an order id, so it can be reconciled.

## Pre-trade rules

| Rule | Setting | Rejection reason |
| --- | --- | --- |
| Instrument is enabled | `GATEWAY_INSTRUMENTS` | `INSTRUMENT_NOT_TRADABLE` |
| Market order can be priced | instrument reference price | `REFERENCE_PRICE_UNAVAILABLE` |
| Order quantity | `RISK_MAX_ORDER_QUANTITY` | `MAX_ORDER_QUANTITY_EXCEEDED` |
| Order notional | `RISK_MAX_ORDER_NOTIONAL` | `MAX_ORDER_NOTIONAL_EXCEEDED` |
| Open orders per account | `RISK_MAX_OPEN_ORDERS` | `MAX_OPEN_ORDERS_EXCEEDED` |
| Worst-case position per instrument | `RISK_MAX_POSITION_QUANTITY` | `POSITION_LIMIT_EXCEEDED` |
| Duplicate clientOrderId | — | handled by idempotency, see ADR-004 |

Position limits are checked against the worst case, not the current position: working buys and
working sells do not offset, because only one side may fill. See ADR-003.

## Known limitations

* The gateway's own idempotency registrations (`OrderRegistry`, which is what makes a duplicate
  `clientOrderId` return the original response instead of creating a second order) are in memory and
  do not survive a restart. A client retry racing the exact restart window can still create a second
  order. Everything else this used to say here no longer applies: risk positions are rebuilt by
  replaying the executions topic (Stage 4), portfolio positions live durably in Postgres (Stage 5),
  and — as of Stage 7 — order ownership and the order book itself are recovered from the
  matching-engine journal (ADR-015), so a router-level restart no longer forgets which worker owns
  which order either. See [Recovery model](recovery-model.md).
* Registrations and rate-limit buckets are never evicted, so both grow for the life of the process.
* The gateway assumes it issued every order it sees a fill for. That holds while it is the only
  ingress into one in-process router, and stops holding as soon as there is a second instance.
* Running two gateway instances would give each its own idempotency map, rate-limit buckets and
  risk-engine instance, though both would agree on positions and executions, which live in the
  shared Postgres rather than in either process. The gateway is single-instance by design.
* The REST adapter blocks a servlet thread while the matching worker processes the command. The
  matching path itself is asynchronous; only the HTTP adapter waits.
* The portfolio endpoints (`/positions`, `/executions`, `/pnl`, mark-price) do not exist when
  `PIPELINE_ENABLED=false`: there is no durable projection to read without the pipeline running.
  `/risk/exposure` is unaffected, since it reads the in-memory risk engine that Stage 3 already
  provides.
