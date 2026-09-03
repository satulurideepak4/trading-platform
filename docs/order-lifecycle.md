# Order lifecycle

Stage 2 does not change the domain lifecycle below. It adds an admission boundary before command
processing: a queue-full command is a routing rejection and never enters the lifecycle, whereas an
admitted command receives the same deterministic accepted or rejected `CommandResult` as Stage 1.

## Commands and identity

The engine accepts `SubmitOrder`, `CancelOrder`, and `ReplaceOrder` commands synchronously. A
positive `orderId` and a non-blank `clientOrderId` identify a submission. Both identifiers are
unique for the lifetime of an engine instance, including after a fill or cancellation. Valid
identifiers are reserved even when later content validation rejects the submission; allowing a
rejected identity to be reused would make replay and audit interpretation ambiguous.

Commands carry caller-supplied timestamps. The engine does not read a wall clock. This is required
for an ordered command replay to reproduce identical snapshots and execution events. Book priority
comes from an engine-assigned monotonic sequence, not timestamps, so equal or non-monotonic external
timestamps cannot change arrival ordering.

## States

```text
submit valid, no fill ────────────────> NEW
submit/replace, some quantity filled ─> PARTIALLY_FILLED
remaining quantity reaches zero ─────> FILLED
cancel active limit order ────────────> CANCELLED
market remainder after matching ──────> CANCELLED
invalid command ──────────────────────> REJECTED result
```

`FILLED`, `CANCELLED`, and a submission's `REJECTED` result are terminal. A partially executed
market order ends as `CANCELLED` with both its executed and remaining quantities visible in its
snapshot. Market orders never rest on the book.

Rejected cancel and replace commands return `REJECTED` as the command result and include the
unchanged existing order snapshot when one exists. They do not change the existing order's state.

The maintained quantity invariants are:

```text
0 <= remainingQuantity <= quantity
executedQuantity = quantity - remainingQuantity
executedQuantity <= quantity
```

## Cancellation

Only an active (`NEW` or `PARTIALLY_FILLED`) resting limit order can be cancelled. Cancellation
removes it from its price level before changing its state. Filled and already-cancelled orders are
rejected with `ORDER_NOT_ACTIVE` and cannot execute again.

## Replacement

Stage 1 replacements apply only to active limit orders and retain the original `orderId`,
`clientOrderId`, symbol, side, type, and creation timestamp. The replacement supplies a new total
quantity and limit price. Its total quantity cannot be below the already-executed quantity.

Every accepted replacement is modeled as cancel/re-enter: it receives a new priority sequence and
may immediately take liquidity at its new price. This intentionally simple rule avoids ambiguous
queue-retention policies. A future venue-rules stage may permit same-price quantity reductions to
retain priority if that behavior is explicitly specified and tested.
