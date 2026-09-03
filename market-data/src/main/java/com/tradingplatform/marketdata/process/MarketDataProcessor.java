package com.tradingplatform.marketdata.process;

import com.tradingplatform.marketdata.event.MarketEventType;
import com.tradingplatform.marketdata.event.RawMarketEvent;
import com.tradingplatform.marketdata.publish.MarketDataSink;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a raw exchange feed into validated, sequenced, normalized state and events.
 *
 * <p>Ingests, validates, sequences, normalizes, maintains latest per-symbol state, and publishes —
 * the five things Stage 6 asks a market-data processor to do, deliberately kept in that order in
 * {@link #ingest}. Nothing here talks to a WebSocket or a database; that is the caller's job, kept
 * separate the same way the matching engine knows nothing about Kafka.
 *
 * <p><b>Single writer.</b> {@code MarketSimulator} drives {@link #ingest} from one thread, the same
 * discipline the matching engine uses per order book. {@link SymbolSequenceTracker} and the
 * processor sequence counter are not synchronized against concurrent callers because of it; the
 * published state map is a {@link ConcurrentHashMap} so readers — the REST snapshot endpoint,
 * {@code MarkPriceUpdater}, a newly subscribing WebSocket client — can safely read it from any
 * thread regardless.
 */
public final class MarketDataProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataProcessor.class);

    private final SymbolSequenceTracker sequenceTracker = new SymbolSequenceTracker();
    private final Map<String, SymbolSnapshot> states = new ConcurrentHashMap<>();
    private final AtomicLong processorSequence = new AtomicLong();
    private final MarketDataSink sink;
    private final MarketDataMetrics metrics;

    public MarketDataProcessor(MarketDataSink sink, MarketDataMetrics metrics) {
        this.sink = sink;
        this.metrics = metrics;
    }

    public void ingest(RawMarketEvent event) {
        metrics.recordReceived();
        try {
            validate(event);
        } catch (MarketDataValidationException invalid) {
            metrics.recordInvalid();
            LOG.debug("event=marketdata_rejected symbol={} reason={}", event.symbol(), invalid.getMessage());
            return;
        }

        SymbolSequenceTracker.Classification classification =
                sequenceTracker.classify(event.symbol(), event.exchangeSequence());
        metrics.record(classification.outcome(), classification.gapSize());
        if (classification.outcome() == SequenceOutcome.GAP) {
            LOG.warn(
                    "event=marketdata_gap_detected symbol={} exchangeSequence={} skipped={}",
                    event.symbol(),
                    event.exchangeSequence(),
                    classification.gapSize());
        }

        NormalizedUpdate update = normalize(event, classification);
        if (classification.advancesState()) {
            states.merge(event.symbol(), toSnapshot(event, update), (ignoredOld, fresh) -> fresh);
        }
        sink.publish(update);
    }

    public Optional<SymbolSnapshot> snapshot(String symbol) {
        return Optional.ofNullable(states.get(symbol));
    }

    public Set<String> symbols() {
        return Set.copyOf(states.keySet());
    }

    private static void validate(RawMarketEvent event) {
        if (event.symbol() == null || event.symbol().isBlank()) {
            throw new MarketDataValidationException("symbol must not be blank");
        }
        long price = priceOf(event);
        long quantity = quantityOf(event);
        if (price <= 0) {
            throw new MarketDataValidationException("price must be positive, was " + price);
        }
        if (quantity <= 0) {
            throw new MarketDataValidationException("quantity must be positive, was " + quantity);
        }
    }

    private NormalizedUpdate normalize(RawMarketEvent event, SymbolSequenceTracker.Classification c) {
        return new NormalizedUpdate(
                event.symbol(),
                MarketEventType.of(event),
                priceOf(event),
                quantityOf(event),
                event.exchangeSequence(),
                event.exchangeTimestamp(),
                processorSequence.incrementAndGet(),
                Instant.now(),
                c.outcome(),
                c.gapSize());
    }

    /** Folds one accepted update onto the symbol's previous snapshot; other sides are untouched. */
    private SymbolSnapshot toSnapshot(RawMarketEvent event, NormalizedUpdate update) {
        SymbolSnapshot previous = states.getOrDefault(event.symbol(), SymbolSnapshot.empty(event.symbol()));
        return switch (event) {
            case RawMarketEvent.BidUpdate bid -> new SymbolSnapshot(
                    previous.symbol(),
                    bid.price(),
                    bid.quantity(),
                    previous.bestAskPrice(),
                    previous.bestAskQuantity(),
                    previous.lastTradePrice(),
                    previous.lastTradeQuantity(),
                    update.exchangeSequence(),
                    update.exchangeTimestamp());
            case RawMarketEvent.AskUpdate ask -> new SymbolSnapshot(
                    previous.symbol(),
                    previous.bestBidPrice(),
                    previous.bestBidQuantity(),
                    ask.price(),
                    ask.quantity(),
                    previous.lastTradePrice(),
                    previous.lastTradeQuantity(),
                    update.exchangeSequence(),
                    update.exchangeTimestamp());
            case RawMarketEvent.Trade trade -> new SymbolSnapshot(
                    previous.symbol(),
                    previous.bestBidPrice(),
                    previous.bestBidQuantity(),
                    previous.bestAskPrice(),
                    previous.bestAskQuantity(),
                    trade.price(),
                    trade.quantity(),
                    update.exchangeSequence(),
                    update.exchangeTimestamp());
        };
    }

    private static long priceOf(RawMarketEvent event) {
        return switch (event) {
            case RawMarketEvent.BidUpdate bid -> bid.price();
            case RawMarketEvent.AskUpdate ask -> ask.price();
            case RawMarketEvent.Trade trade -> trade.price();
        };
    }

    private static long quantityOf(RawMarketEvent event) {
        return switch (event) {
            case RawMarketEvent.BidUpdate bid -> bid.quantity();
            case RawMarketEvent.AskUpdate ask -> ask.quantity();
            case RawMarketEvent.Trade trade -> trade.quantity();
        };
    }
}
