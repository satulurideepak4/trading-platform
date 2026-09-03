# ADR-004: clientOrderId idempotency through a shared registration

* Status: Accepted
* Date: 2026-08-18

## Context

A trading client that does not get a response cannot tell whether its order was accepted. Its only
safe reaction is to resend. If the gateway treats the resend as a new order, the client ends up with
double the intended position, which is the most expensive failure mode an order gateway has.

A naive "have I seen this clientOrderId before?" check does not solve it. Two threads carrying the
same clientOrderId can both find nothing and both submit. Nor does it solve the harder case: a
retry arriving while the first attempt is still in flight has nothing to look up yet.

## Decision

**The key is `(accountId, clientOrderId)`.** Clients only guarantee uniqueness within their own
account, and one client must not be able to collide with, or probe for, another's identifiers.

**One registration per key, created before any work.** `OrderRegistry.reserve` uses
`ConcurrentHashMap.putIfAbsent` to create a registration holding an allocated order id and a
`CompletableFuture` for the outcome. Exactly one caller wins. Losers get the same registration and
wait on the same future. There is no window in which two callers both believe they are first.

**Outcomes are stored, not just identifiers.** The future is completed with the final outcome —
accepted, or rejected by risk or by the engine — so a retry replays exactly what the first attempt
produced. A retried risk rejection is still a rejection with the same reason and the same order id.

**Order ids are allocated by the gateway.** The client never supplies one. Allocation is optimistic:
a caller that loses the race discards its id, leaving a gap. Ids need to be unique and increasing,
not contiguous.

**Reuse with different content is a conflict, not a new order.** The registration stores the order's
symbol, side, type, quantity and price. A repeat with different content returns 409 rather than
guessing which of the two the client meant.

**Settlement is driven by the router's completion, not by the HTTP thread.** This is what makes the
timeout case correct. A request that times out gets 504 and keeps its registration; the worker still
completes the command and completes the registration, so the client's retry receives the real
outcome instead of creating a second order.

**Withdrawal is distinguished from failure.** A command that provably never entered a queue —
queue-full, and only queue-full — withdraws its registration and releases its risk reservation, so
the client may retry the same clientOrderId cleanly. Anything that may have reached a worker keeps
its registration.

**Namespacing at the engine boundary.** The matching engine treats clientOrderId as platform-wide.
The gateway sends `accountId:clientOrderId`, which is why the client-supplied identifier is capped
at 32 characters and account ids at 31, keeping the combined form inside the engine's 64-character
field.

## Consequences

* Sixteen simultaneous identical submissions produce exactly one order: one 201 and fifteen 200s
  marked `duplicate: true`. This is covered by a test.
* A client can tell whether its retry was the request that traded, because a created order returns
  201 and a replay returns 200.
* Registrations are never evicted, so the map grows for the life of the process. At Stage 3 volumes
  this is bounded by test and demo traffic; a production gateway needs a retention window (a day is
  the usual choice, matching how long a clientOrderId is meaningful) and eviction. This is a known
  limitation, not an oversight.
* Duplicate detection exists in two places: the gateway keys on `(account, clientOrderId)` and the
  matching engine keeps its own platform-wide set. The engine's copy is the backstop that makes the
  invariant hold even if the gateway is bypassed or restarted.
* Because registrations are in memory, a gateway restart forgets them. A retry that spans a restart
  can create a second order. Durable idempotency depends on the same durable execution record that
  ADR-003 needs.
* A settlement bug would fail the registration without removing it, so the client sees an error and
  a retry sees the same error, rather than silently duplicating a live order.

## Alternatives considered

* **Global clientOrderId keyspace:** removes the namespacing step, but lets one client's identifier
  choices break another's and leaks the existence of other clients' orders.
* **Storing only the identifier, not the outcome:** cheaper, but a retry cannot be answered with
  what actually happened, only with "seen before".
* **Letting clients supply order ids:** removes an allocation, but makes the platform's primary key
  a value an untrusted party controls.
* **Abandoning the registration on timeout:** simpler, but converts a slow matching worker into
  duplicate orders, which is the exact failure this ADR exists to prevent.
* **Persisting registrations to Redis or PostgreSQL:** survives restart, but puts a storage round
  trip in front of every order. The durable answer belongs on the execution event stream instead.
