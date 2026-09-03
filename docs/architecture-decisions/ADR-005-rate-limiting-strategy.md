# ADR-005: Per-account token bucket at the gateway edge

* Status: Accepted
* Date: 2026-08-18

## Context

The matching workers already have bounded queues, so overload cannot exhaust memory. But queue
saturation is a blunt instrument: it degrades every account sharing a worker, including accounts
behaving normally. A misbehaving or looping client should be contained before it reaches shared
capacity.

Trading traffic is bursty by nature. A strategy repricing a book sends many orders in a few
milliseconds and then goes quiet. A limiter that cannot distinguish bursts from sustained load
either rejects legitimate repricing or has to be set so high that it stops limiting anything.

## Decision

A token bucket per account, applied in the request interceptor after authentication and before the
handler.

**Token bucket, not fixed window.** The bucket separates two things a fixed window conflates:
`burst` is how much short-term bunching is tolerated, `permitsPerSecond` is the sustained rate. A
fixed window also allows up to twice the intended rate across a window boundary, which the bucket
does not.

**Per account, not per connection or per IP.** The account is the unit that risk, idempotency and
order ownership are already scoped to, and it is the unit an operator would reason about. A client
cannot buy more capacity by opening more connections.

**Lazy refill.** Buckets refill on access from a nanosecond clock rather than from a timer thread,
so an idle account costs nothing and there is no background work proportional to the number of
accounts.

**After authentication, before idempotency.** The limit is per account, so identity must be
established first. It is charged before idempotency on purpose: a client retrying in a tight loop
should be throttled rather than allowed to hammer the gateway simply because its identifier is
already known.

**Throttling does not consume an order id.** A 429 is raised before any registration or reservation
exists, so the client may retry the same clientOrderId. This is covered by a test.

**429 carries `Retry-After`.** The bucket knows exactly how long until the next permit, so the
client is told rather than left to guess.

## Consequences

* One account exhausting its budget has no effect on any other account.
* The limit is a single global configuration applied identically to every account. Per-account
  tiers need a control-plane configuration source and are not Stage 3 work.
* Buckets are created on first use and never evicted, so the map grows with the number of distinct
  accounts seen. Accounts come from a fixed configured key set, so this is bounded today; it stops
  being bounded once accounts are dynamic.
* State is per gateway instance. Two instances would each allow the full rate, so the effective
  limit scales with instance count. A shared limiter would need coordination, which is a latency
  cost this stage does not want to pay before it has been measured.
* The limiter counts requests, not risk. A client within its rate can still send its maximum
  permitted notional every time; that is what the pre-trade limits in ADR-003 are for.
* Rate limiting and queue saturation are deliberately different responses: 429 means "you are
  sending too fast", 503 means "the system is full". Conflating them would hide which one is
  happening.

## Alternatives considered

* **Fixed or sliding window counters:** simpler, but either reject legitimate bursts or permit
  double the rate at a boundary.
* **Leaky bucket with queueing:** smooths output, but holding orders back adds latency invisibly.
  For trading, refusing quickly is more useful than delaying silently.
* **Rely on the bounded worker queues alone:** already present, but shared, so one client's overload
  becomes everyone's.
* **Limit per IP or per connection:** trivially defeated by opening more connections, and not the
  unit anything else in the system is scoped to.
* **A distributed limiter in Redis:** consistent across instances, but adds a network round trip to
  the front of every order for a limit that is approximate by nature.
