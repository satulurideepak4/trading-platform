# ADR-003: In-memory pre-trade risk with reserve-and-settle

* Status: Accepted
* Date: 2026-08-18

## Context

Every order must pass maximum order quantity, maximum order notional, allowed instrument, maximum
open orders and position limit checks before it is allowed to reach a matching worker. These checks
run on every submission and every replacement, so their cost is part of order latency.

Two properties are easy to get wrong:

1. **Checking is not enough.** If a check only reads current exposure, a burst of concurrent orders
   from one account all observe the same pre-burst state and all pass, and the account ends up past
   its limit.
2. **Filled quantity is not the exposure.** An account holding no position but working a 10,000-lot
   buy is one fill away from a 10,000-lot position. A limit checked against realised position only
   is not a limit.

We also need to decide where the state lives. PostgreSQL is the obvious home for authoritative risk
configuration, but a database round trip per rule would place storage latency and availability in
the order path.

## Decision

Risk state is held in memory in a dedicated `risk-engine` module with no Spring, no database and no
messaging dependency.

**Reserve and settle.** `PreTradeRiskEngine.reserve` checks the limits and, in the same critical
section, records the order as working at its full quantity. The command is only routed after that.
Once the matching engine reports an outcome, the reservation is settled against what is actually
still working. A rejected or unroutable command releases its reservation.

**Worst-case position.** The position limit is checked against the largest position the account
could reach, computed as `max(|position + workingBuys|, |position - workingSells|)`. Working buys
and sells do not net against each other, because only one side may fill.

**Per-account locking.** Each account's state is one object with synchronized methods. The critical
section spans check-and-reserve. Accounts never contend with each other.

**Absolute settlement.** Settlement sets the remaining reserved quantity rather than decrementing
it, so the same outcome may be reported more than once without corrupting the reservation.

**Both sides of a fill.** An execution changes the resting order's owner too, and that account is
not waiting on any request. The gateway looks up the owner of each side from its order registry and
applies the fill to both.

**Market order sizing.** A market order has no price, so its notional is estimated from an
instrument reference price seeded from configuration and updated from observed trades. An
instrument with no reference price rejects market orders rather than skipping the notional check.

## Consequences

* No storage system is in the order path, and risk stays available when a database is not.
* Limits hold under concurrency: a test fires 200 simultaneous orders at a 500-lot limit and
  exactly 50 ten-lot orders are approved.
* Reservations are conservative. An account can be refused for exposure it would only reach in the
  worst fill sequence. That is the intended direction to be wrong in.
* Risk state does not survive a restart. Nothing rebuilds it from execution history yet, so a
  restarted gateway starts from flat positions and zero open orders. This is the single largest gap
  in Stage 3 and is Stage 5 work.
* Limits are global rather than per account. Differentiated limits need a per-account configuration
  source, which is a control-plane feature, not an ingress one.
* Because the gateway is the only writer of risk state and holds it in one process, a second
  gateway instance would have its own independent view. Stage 3 is deliberately single-instance.

## How authoritative state and persistence would coexist

The intended shape, which Stages 4 and 5 build:

* **Execution history in Kafka and PostgreSQL is the durable record of truth.**
* **In-memory risk state is a derived projection** of that record, and is authoritative only for
  the admission decision, because that is the decision that must be fast.
* **On startup**, the projection is rebuilt by replaying execution history, and the gateway does not
  accept orders until the replay has caught up.
* **While running**, the projection is advanced by the execution event stream rather than by the
  synchronous command result, which also removes the gateway's current assumption that it issued
  every order it sees a fill for.

## Alternatives considered

* **Check against the database per rule:** authoritative and durable, but puts storage latency and
  availability into every order and still needs in-memory reservation to be correct under
  concurrency.
* **Check without reserving:** simpler, but permits a concurrent burst to breach any limit.
* **Net working buys against working sells:** allows more trading, but is not a limit, because the
  buy can fill while the sell does not.
* **One global lock over all accounts:** simpler than per-account state, but makes unrelated
  accounts contend on the busiest path in the system.
