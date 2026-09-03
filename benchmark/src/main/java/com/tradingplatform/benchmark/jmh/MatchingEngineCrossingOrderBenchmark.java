package com.tradingplatform.benchmark.jmh;

import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.MatchingEngine;
import java.time.Instant;
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
 * Pure matching cost: a resting SELL is placed immediately before every timed call
 * ({@link Level#Invocation}), at the exact price and quantity the timed BUY will submit, so every
 * measured operation fully crosses one resting order and leaves the book empty afterward — no book
 * growth across the run, and every sample does real matching work rather than a mix of matching and
 * plain insertion.
 *
 * <p>{@code @Setup(Level.Invocation)} is the deliberate, JMH-documented tool for "this benchmark
 * needs fresh state before every single call" — and the honest caveat that comes with it: at the
 * sub-microsecond scale a single {@code submit()} runs at, the setup call's own cost is not free,
 * so this benchmark's absolute numbers include a fixed per-invocation setup overhead that
 * {@link MatchingEngineRestingOrderBenchmark} does not pay. Read the two relative to each other —
 * "how much more does crossing cost than resting insertion, plus this benchmark's own admitted
 * setup tax" — not as two independent absolute costs. See docs/benchmark-methodology.md.
 *
 * <p>Run with {@code java -jar target/benchmarks.jar MatchingEngineCrossingOrderBenchmark}.
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class MatchingEngineCrossingOrderBenchmark {
    private static final Instant BASE_TIME = Instant.parse("2025-01-01T00:00:00Z");
    private static final String SYMBOL = "AAPL";
    private static final long PRICE = 10_000;
    private static final long QUANTITY = 10;

    private MatchingEngine engine;
    private long nextOrderId;

    @Setup(Level.Iteration)
    public void setUpIteration() {
        engine = new MatchingEngine();
        nextOrderId = 1;
    }

    @Setup(Level.Invocation)
    public void restOneCounterOrder() {
        long orderId = nextOrderId++;
        engine.submit(SubmitOrder.limit(
                orderId, "rest-" + orderId, SYMBOL, Side.SELL, QUANTITY, PRICE, BASE_TIME.plusNanos(orderId)));
    }

    @Benchmark
    public CommandResult crossAgainstRestingOrder() {
        long orderId = nextOrderId++;
        SubmitOrder command = SubmitOrder.limit(
                orderId, "cross-" + orderId, SYMBOL, Side.BUY, QUANTITY, PRICE, BASE_TIME.plusNanos(orderId));
        return engine.submit(command);
    }
}
