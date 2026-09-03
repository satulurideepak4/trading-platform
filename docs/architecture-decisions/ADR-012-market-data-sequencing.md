# ADR-012: Per-symbol sequence classification, not a dedup window

* Status: Accepted
* Date: 2026-08-19

## Context

A real exchange feed is not a clean stream. Packets duplicate, arrive out of order, and go missing.
`MarketDataProcessor` has to turn that into state a strategy or a mark-price consumer can trust:
never move a symbol's book backwards in time, but also never silently pretend the feed was perfect.

The exchange assigns each symbol its own sequence number. The question is what to keep on this side
to classify an incoming one, and how much memory that costs across many symbols over a long-running
process.

## Decision

**Keep one number per symbol — the last sequence that was actually applied — and classify every
incoming sequence against only that number.**

`SymbolSequenceTracker` (`market-data/.../process/SymbolSequenceTracker.java`) does the whole thing
in four comparisons:

| Incoming vs. last applied | Outcome | State advances? |
| --- | --- | --- |
| no prior sequence for this symbol | `IN_ORDER` | yes |
| `== last` | `DUPLICATE` | no |
| `< last` | `OUT_OF_ORDER` | no |
| `> last + 1` | `GAP` (flagged with the skip count) | yes |
| `== last + 1` | `IN_ORDER` | yes |

`GAP` still advances state — there is nothing to reconstruct the missing updates from, and refusing
to move forward would mean one lost packet freezes a symbol forever. It is flagged rather than
silently applied so a subscriber knows this symbol's state skipped intermediate values.

**No history window.** `DUPLICATE` and `OUT_OF_ORDER` are told apart only by whether the incoming
sequence equals or is strictly less than what was last applied — not by checking a set of
previously seen sequences. A record that is a true duplicate of something *older* than the last
applied value and a record that is genuinely new but arrived late both land in `OUT_OF_ORDER`, and
both get the same treatment: not applied. This is a deliberate trade: the alternative is a
per-symbol window of recently seen sequences that grows with the feed's reorder depth and never
shrinks on its own, for a distinction (which of the two actually happened) that state advancement
does not need — either way the record must not move the book backwards.

**Global processor sequence, separate from the exchange one.** Every `NormalizedUpdate` also carries
a `processorSequence`, a single counter incremented across every symbol. Exchange sequences are
per-symbol and untrusted; this is the number a subscriber can use to detect a gap in what *this
processor* published, independent of upstream feed quality.

## Consequences

* Memory is one `long` per tracked symbol, forever. A feed running for days across thousands of
  symbols costs kilobytes, not an ever-growing dedup structure.
* Classification is entirely local to one symbol and touches nothing else, so it costs the same
  whether the process is tracking one symbol or ten thousand.
* A gap is never retroactively filled. If the missing updates arrive later — genuinely out of order
  rather than lost — they are classified `OUT_OF_ORDER` and dropped, because by then a later
  sequence has already been applied. This is consistent with "state only moves forward," but it does
  mean a reordering feed permanently loses whatever arrived between the gap and its detection.
* `SymbolSequenceTracker` assumes one writer per process — `MarketDataProcessor` is fed by a single
  generator thread, the same single-writer discipline the matching engine uses per order book (see
  ADR-002). Concurrent callers on the same symbol are not supported and would need external
  synchronization.
* Tested directly against `MarketSimulator`'s injected duplicate/out-of-order/gap rates
  (`FeedImperfectionPolicy`), so the classification is exercised by every run of the CLI smoke test
  and the integration suite, not just unit tests written in isolation.

## Alternatives considered

* **A sliding window of recently seen sequences per symbol:** can distinguish a true duplicate from
  a late-arriving new record, but costs unbounded memory relative to how far the feed reorders, for
  a distinction that does not change what the processor does with either case.
* **Reject anything not exactly `last + 1`:** simplest possible rule, but treats every gap as fatal
  and stalls the symbol until a human intervenes — unacceptable for a feed that is expected to drop
  packets occasionally.
* **Buffer and reorder before applying:** would let genuinely out-of-order records still be applied
  in the right sequence, at the cost of a latency buffer on every symbol and unbounded worst-case
  delay if a gap never closes. Not attempted here; see `docs/market-data.md` for where this would
  fit if a future stage needs it.
