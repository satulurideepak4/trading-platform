package com.tradingplatform.benchmark.jmh;

import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.MatchingEngine;
import java.time.Instant;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Pure insertion cost: every order is a BUY at a random price in a fixed band, so nothing ever has
 * a counter-side order to cross against. This isolates the order book's admission path — validate,
 * assign a priority sequence, insert into a price level — from any matching work at all, which is
 * what {@link MatchingEngineCrossingOrderBenchmark} measures instead.
 *
 * <p>Price is drawn from a bounded 2,000-tick band, not a single fixed price: real order flow does
 * not park everything on one price level, and a single level's insertion is the easy case. The book
 * is reset once per iteration ({@link Level#Iteration}), not per invocation — resetting every call
 * would add setup cost to every measured sample; resetting per iteration bounds how large the book
 * grows within a single ~1s measurement window without touching what's being timed.
 *
 * <p>Run with {@code java -jar target/benchmarks.jar MatchingEngineRestingOrderBenchmark}. See
 * docs/benchmark-methodology.md for the exact command and this environment's hardware/JDK.
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class MatchingEngineRestingOrderBenchmark {
    private static final Instant BASE_TIME = Instant.parse("2025-01-01T00:00:00Z");
    private static final String SYMBOL = "AAPL";

    private MatchingEngine engine;
    private SplittableRandom random;
    private long nextOrderId;

    @Setup(Level.Iteration)
    public void setUp() {
        engine = new MatchingEngine();
        random = new SplittableRandom(7_921);
        nextOrderId = 1;
    }

    @Benchmark
    public CommandResult submitRestingBuyOrder() {
        long orderId = nextOrderId++;
        long price = 9_000 + random.nextLong(2_000);
        SubmitOrder command = SubmitOrder.limit(
                orderId, "bench-" + orderId, SYMBOL, Side.BUY, 10, price, BASE_TIME.plusNanos(orderId));
        return engine.submit(command);
    }
}
