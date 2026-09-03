# ADR-009: Bounded retry, then dead-letter

* Status: Accepted
* Date: 2026-08-18

## Context

A consumer that throws blocks its partition. Kafka does not skip a record, so the same one is
redelivered until it succeeds. If the failure is permanent — a malformed payload, a negative
quantity, an event type on the wrong topic — the partition stops forever, and every well-formed
record behind it stops with it. One bad record takes out an instrument.

Retrying is still right for the other kind of failure. A momentary blip is worth another attempt,
and giving up on it would lose a real trade.

The two need different treatment, so the design has to be able to tell them apart.

## Decision

**Classify first, retry second.**

Failures that cannot succeed on a retry skip the backoff entirely and are dead-lettered on the first
attempt:

* `DeserializationException` — the bytes are not a parseable event
* `InvalidEventException` — it parsed, but describes something impossible (negative quantity,
  missing account, an execution on the wrong topic)

Everything else gets a finite retry budget: four attempts with exponential backoff from 200ms,
doubling, capped at 5s. **Finite is the point.** An unbounded retry is not resilience — it is a
partition that has stopped with no alarm attached.

**Deserialization failures must not kill the container.** Both key and value deserializers are
wrapped in `ErrorHandlingDeserializer`, which turns unparseable bytes into a failed record the error
handler can route, instead of an exception that stops the consumer.

**Dead-lettered records keep their partition index.** The recoverer publishes to
`<topic>.dlq` at the same partition, so a key's failures stay together and the DLQ can be read
alongside the topic it came from.

**The DLQ producer serializes by runtime type.** A dead-lettered record arrives in one of two
shapes: a record that failed to deserialize has only its original bytes, while a record that parsed
and was then refused still holds a real event. The key is independent of that — a value can be
unparseable while its key parsed cleanly. A `DelegatingByTypeSerializer` on both key and value
handles every combination. This was found by a test rather than by inspection: an earlier version
used a byte-only DLQ template, and dead-lettering failed on the String key, which put the error
handler back into an unbounded retry and blocked the partition — the exact failure this ADR exists
to prevent.

**Give-ups are counted where they happen.** `trading.consumer.dead.lettered` is incremented in the
recoverer, which runs once after the budget is spent, rather than in the error handler, which runs
on every failed attempt.

## Consequences

* A poison record costs its partition a few hundred milliseconds and then stops mattering. An
  integration test publishes malformed JSON followed by a valid execution and asserts the valid one
  still lands.
* Nothing is silently discarded. Every give-up is on a durable topic, counted, and logged with the
  topic, partition, offset and cause.
* **Nothing consumes the DLQ.** Records accumulate there until someone looks. That is deliberate for
  Stage 4 — automatic reprocessing of records that already failed is a good way to loop — but it
  means the DLQ needs an alert on non-zero depth to be useful, which is Stage 8.
* Retries happen in the listener thread, so a retrying record delays everything behind it on that
  partition by up to the retry budget. Bounded at roughly 11 seconds worst case, which is why the
  cap exists.
* The classification is only as good as the exception types. A transient failure that surfaces as
  `InvalidEventException` would be dead-lettered rather than retried, so validation must be strict
  about only rejecting things that genuinely cannot become valid.
* Dead-lettering an execution means downstream state is permanently missing that trade. Nothing
  reconciles that gap yet; detecting it belongs with Stage 5's reconciliation from execution
  history.

## Alternatives considered

* **Retry forever:** never loses a record, and stops the instrument on the first malformed one.
* **Skip on failure:** keeps the partition moving and silently loses trades, which is worse than
  stopping.
* **Retry topics with increasing delays:** avoids blocking the partition during backoff, at the cost
  of losing per-key ordering — the retried record is re-consumed after later records for the same
  key. Not worth it for a budget measured in seconds.
* **One shared retry policy for everything:** simpler, but spends the full budget on failures that
  are certain to fail again, and delays the partition for no benefit.
