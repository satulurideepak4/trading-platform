package com.tradingplatform.marketdata.simulator;

import com.tradingplatform.marketdata.event.RawMarketEvent;
import com.tradingplatform.marketdata.process.MarketDataProcessor;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plays the part of an outside exchange: a single generator thread producing bid/ask/trade updates
 * across configured symbols at a target aggregate rate, feeding them straight into a {@link
 * MarketDataProcessor}.
 *
 * <p>Single-threaded by design, the same choice {@code OrderFlowSimulator} makes for order flow:
 * generation is CPU-only and one thread comfortably clears the rates this demo needs, so there is
 * no concurrency problem here worth solving before Stage 9 asks for a controlled benchmark.
 * Everything is derived from one seeded {@link SplittableRandom}, so a run is exactly reproducible.
 *
 * <p>Per symbol, a mid-price random-walks within a bounded range and bid/ask/trade prices are
 * derived from it, which keeps the book two-sided and roughly realistic without claiming to model
 * any real instrument's actual volatility.
 */
public final class MarketSimulator {
    private static final Logger LOG = LoggerFactory.getLogger(MarketSimulator.class);
    private static final long MIN_PRICE = 100;
    private static final long SPREAD = 4;
    private static final long MAX_STEP = 20;

    private final List<String> symbols;
    private final int eventsPerSecond;
    private final FeedImperfectionPolicy imperfections;
    private final MarketDataProcessor processor;
    private final SplittableRandom random;

    private final Map<String, Long> midPrices = new HashMap<>();
    private final Map<String, Long> exchangeSequences = new HashMap<>();
    private int nextSymbolIndex;

    private volatile boolean running;
    private Thread generatorThread;

    public MarketSimulator(
            List<String> symbols,
            int eventsPerSecond,
            long seed,
            FeedImperfectionPolicy imperfections,
            MarketDataProcessor processor) {
        if (symbols.isEmpty()) {
            throw new IllegalArgumentException("at least one symbol is required");
        }
        if (eventsPerSecond <= 0) {
            throw new IllegalArgumentException("eventsPerSecond must be positive");
        }
        this.symbols = List.copyOf(symbols);
        this.eventsPerSecond = eventsPerSecond;
        this.imperfections = imperfections;
        this.processor = processor;
        this.random = new SplittableRandom(seed);
        this.symbols.forEach(symbol -> {
            midPrices.put(symbol, MIN_PRICE + random.nextLong(1_000));
            exchangeSequences.put(symbol, 0L);
        });
    }

    /** Runs synchronously until exactly {@code eventCount} events have been generated. */
    public void runFor(long eventCount) {
        long nanosPerEvent = 1_000_000_000L / eventsPerSecond;
        for (long i = 0; i < eventCount; i++) {
            long startedAt = System.nanoTime();
            processor.ingest(generateNext());
            pace(startedAt, nanosPerEvent);
        }
    }

    /** Starts a background generator thread that runs until {@link #stop()}. */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        generatorThread = Thread.ofPlatform().name("marketdata-simulator").daemon(true).start(() -> {
            long nanosPerEvent = 1_000_000_000L / eventsPerSecond;
            while (running) {
                long startedAt = System.nanoTime();
                try {
                    processor.ingest(generateNext());
                } catch (RuntimeException failure) {
                    LOG.error("event=marketdata_simulator_generation_failed", failure);
                }
                pace(startedAt, nanosPerEvent);
            }
        });
        LOG.info(
                "event=marketdata_simulator_started symbols={} eventsPerSecond={}",
                symbols.size(),
                eventsPerSecond);
    }

    public synchronized void stop() {
        running = false;
        if (generatorThread != null) {
            generatorThread.interrupt();
            generatorThread = null;
        }
    }

    private static void pace(long startedAtNanos, long nanosPerEvent) {
        long remaining = nanosPerEvent - (System.nanoTime() - startedAtNanos);
        if (remaining > 0) {
            LockSupport.parkNanos(remaining);
        }
    }

    private RawMarketEvent generateNext() {
        String symbol = symbols.get(nextSymbolIndex);
        nextSymbolIndex = (nextSymbolIndex + 1) % symbols.size();

        long mid = walk(symbol);
        long sequence = nextSequence(symbol);
        Instant now = Instant.now();

        int kind = random.nextInt(10);
        if (kind < 4) {
            return new RawMarketEvent.BidUpdate(symbol, sequence, mid - SPREAD, quantity(), now);
        }
        if (kind < 8) {
            return new RawMarketEvent.AskUpdate(symbol, sequence, mid + SPREAD, quantity(), now);
        }
        return new RawMarketEvent.Trade(symbol, sequence, mid, quantity(), now);
    }

    private long walk(String symbol) {
        long step = random.nextLong(-MAX_STEP, MAX_STEP + 1);
        long updated = Math.max(MIN_PRICE, midPrices.get(symbol) + step);
        midPrices.put(symbol, updated);
        return updated;
    }

    private long quantity() {
        return random.nextLong(1, 501);
    }

    /**
     * The true next sequence for this symbol always advances by one; what is actually emitted may
     * differ, on purpose. See {@link FeedImperfectionPolicy}.
     */
    private long nextSequence(String symbol) {
        long current = exchangeSequences.merge(symbol, 1L, Long::sum);
        return switch (imperfections.next(random)) {
            case NONE -> current;
            case DUPLICATE -> {
                // Re-emit the previous sequence instead of the fresh one; the counter still
                // advanced, so the next normal event resumes from where it left off.
                yield Math.max(1, current - 1);
            }
            case OUT_OF_ORDER -> {
                long delayed = current - random.nextLong(2, 5);
                yield Math.max(1, delayed);
            }
            case GAP -> {
                long skipped = random.nextLong(2, 5);
                long jumped = current + skipped;
                exchangeSequences.put(symbol, jumped);
                yield jumped;
            }
        };
    }
}
