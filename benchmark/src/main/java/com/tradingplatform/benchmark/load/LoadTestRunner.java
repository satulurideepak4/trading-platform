package com.tradingplatform.benchmark.load;

import com.tradingplatform.benchmark.report.ReportWriter;
import com.tradingplatform.benchmark.report.RunResult;
import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.routing.OrderRouter;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Drives one {@link Workload} against one {@link SubmissionTarget}, records producer-observed
 * wall-clock latency and queue depth for the whole run, and writes an automatically generated
 * report into {@code benchmarks/results/}. This is the macro/systems half of Stage 9's tooling —
 * the JMH benchmarks in {@code com.tradingplatform.benchmark.jmh} are the micro half. See
 * docs/benchmark-methodology.md for why the split.
 *
 * <pre>
 * mvn -pl benchmark -am package -DskipTests
 * java -cp benchmark/target/classes:matching-engine/target/classes:trading-domain/target/classes \
 *   com.tradingplatform.benchmark.load.LoadTestRunner --workload=steady --target=router
 * </pre>
 */
public final class LoadTestRunner {
    private static final Instant BASE_TIME = Instant.parse("2026-01-01T00:00:00Z");
    private static final long REFERENCE_PRICE = 10_000;
    private static final long QUEUE_SAMPLE_INTERVAL_MILLIS = 20;
    /** How many of a producer's own recent orders it keeps around to cancel later. */
    private static final int CANCEL_TRACKING_WINDOW = 64;
    /** The gateway's own default configured instrument set (see application.yml/.env.example) —
     * network targets trade against a real, running gateway, which only accepts symbols it was
     * actually configured with, unlike the in-process targets' synthetic SYM0..SYMn universe. */
    private static final List<String> GATEWAY_SYMBOLS = List.of("AAPL", "MSFT", "NVDA", "TSLA");

    private LoadTestRunner() {}

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        Workload workload = arguments.workload();
        List<String> symbols =
                arguments.isNetworkTarget() ? GATEWAY_SYMBOLS : symbols(workload.symbolCount());
        // Only matters against a real, persistent gateway (a network target): its idempotency
        // registry remembers every clientOrderId for the life of the process, not just this run, so
        // two separate LoadTestRunner invocations - even the same workload run twice, or two
        // different targets run back to back against the same live gateway - would otherwise both
        // generate "load-1".."load-N" and collide. A real end-to-end run caught exactly this: a
        // rerun with the identical clientOrderId space hit ClientOrderIdConflictException rather
        // than a harmless idempotent replay (a separate real bug - inconsistent strategyId
        // resolution between the REST and Protobuf paths, fixed in ProtobufOrderMapper - meant the
        // "identical" resubmission was not byte-for-byte identical after all). In-process targets
        // are unaffected either way - each gets a fresh engine/registry per run.
        String runId = Long.toString(System.nanoTime(), 36);

        try (SubmissionTarget target = arguments.buildTarget(workload)) {
            RunResult result = run(
                    workload, arguments.targetName(), arguments.producerCount(), symbols, target, runId);
            System.out.println(result.toConsoleSummary());
            String path = ReportWriter.write(result);
            System.out.println("event=report_written path=" + path);
        }
    }

    private static RunResult run(
            Workload workload,
            String targetName,
            int producerCount,
            List<String> symbols,
            SubmissionTarget target,
            String runId)
            throws InterruptedException {
        LatencyRecorder latency = new LatencyRecorder(workload.orderCount());
        LongAdder admitted = new LongAdder();
        LongAdder saturated = new LongAdder();
        LongAdder accepted = new LongAdder();
        LongAdder rejected = new LongAdder();
        LongAdder executions = new LongAdder();
        LongAdder cancelsSent = new LongAdder();
        CountDownLatch completed = new CountDownLatch(workload.orderCount());
        AtomicLong pacingSlot = new AtomicLong();
        AtomicInteger maxQueueDepth = new AtomicInteger();
        LongAdder queueDepthSum = new LongAdder();
        LongAdder queueDepthSamples = new LongAdder();

        Thread sampler = startQueueDepthSampler(target, maxQueueDepth, queueDepthSum, queueDepthSamples);
        long startedAt = System.nanoTime();
        try (ExecutorService producers = Executors.newFixedThreadPool(producerCount)) {
            List<Future<?>> tasks = new java.util.ArrayList<>(producerCount);
            for (int producerId = 0; producerId < producerCount; producerId++) {
                int id = producerId;
                tasks.add(producers.submit(() -> produce(
                        id, producerCount, workload, symbols, target, runId, startedAt, pacingSlot, latency,
                        admitted, saturated, accepted, rejected, executions, cancelsSent, completed)));
            }
            producers.shutdown();
            for (Future<?> task : tasks) {
                task.get();
            }
            completed.await();
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("producer thread failed", e);
        } finally {
            sampler.interrupt();
        }
        long elapsedNanos = System.nanoTime() - startedAt;

        return new RunResult(
                workload,
                targetName,
                producerCount,
                elapsedNanos,
                admitted.sum(),
                saturated.sum(),
                accepted.sum(),
                rejected.sum(),
                executions.sum(),
                cancelsSent.sum(),
                latency.percentiles(),
                maxQueueDepth.get(),
                queueDepthSamples.sum() == 0 ? 0.0 : (double) queueDepthSum.sum() / queueDepthSamples.sum());
    }

    private static Thread startQueueDepthSampler(
            SubmissionTarget target, AtomicInteger max, LongAdder sum, LongAdder samples) {
        Thread sampler = Thread.ofPlatform().daemon(true).name("queue-depth-sampler").start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int depth = target.queueDepth();
                max.accumulateAndGet(depth, Math::max);
                sum.add(depth);
                samples.increment();
                try {
                    TimeUnit.MILLISECONDS.sleep(QUEUE_SAMPLE_INTERVAL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        return sampler;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private static void produce(
            int producerId,
            int producerCount,
            Workload workload,
            List<String> symbols,
            SubmissionTarget target,
            String runId,
            long startedAt,
            AtomicLong pacingSlot,
            LatencyRecorder latency,
            LongAdder admitted,
            LongAdder saturated,
            LongAdder accepted,
            LongAdder rejected,
            LongAdder executions,
            LongAdder cancelsSent,
            CountDownLatch completed) {
        SplittableRandom random = new SplittableRandom(workload.seed() + 104_729L * producerId);
        Deque<Long> ownOpenOrders = new ArrayDeque<>(CANCEL_TRACKING_WINDOW);

        for (long index = producerId; index < workload.orderCount(); index += producerCount) {
            pace(workload.ordersPerSecond(), startedAt, pacingSlot.getAndIncrement());
            long orderId = index + 1;
            boolean sendCancel = workload.cancelRatio() > 0
                    && !ownOpenOrders.isEmpty()
                    && random.nextDouble() < workload.cancelRatio();

            long submittedAt = System.nanoTime();
            try {
                if (sendCancel) {
                    long targetOrderId = ownOpenOrders.pollFirst();
                    cancelsSent.increment();
                    target.cancel(new CancelOrder(targetOrderId, BASE_TIME.plusNanos(orderId)))
                            .whenComplete((outcome, failure) -> onComplete(
                                    submittedAt, latency, outcome, failure, accepted, rejected, executions,
                                    completed));
                } else {
                    String symbol = symbols.get(random.nextInt(symbols.size()));
                    long price = REFERENCE_PRICE
                            + random.nextLong(-workload.matchBandTicks(), workload.matchBandTicks() + 1);
                    Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
                    // runId namespaces clientOrderId per process invocation - see the comment on its
                    // declaration in main(). "load-" + runId + "-" + orderId stays well under
                    // SubmitOrderRequest's 32-char clientOrderId limit (runId is nanoTime in base 36,
                    // ~9 chars; orderId is at most 7-8 digits for any realistic workload size).
                    SubmitOrder command = SubmitOrder.limit(
                            orderId, "load-" + runId + "-" + orderId, symbol, side, random.nextLong(1, 101),
                            Math.max(1, price), BASE_TIME.plusNanos(orderId));
                    target.submit(command)
                            .whenComplete((outcome, failure) -> onComplete(
                                    submittedAt, latency, outcome, failure, accepted, rejected, executions,
                                    completed));
                    if (ownOpenOrders.size() >= CANCEL_TRACKING_WINDOW) {
                        ownOpenOrders.pollFirst();
                    }
                    ownOpenOrders.addLast(orderId);
                }
                admitted.increment();
            } catch (RuntimeException saturation) {
                saturated.increment();
                completed.countDown();
            }
        }
    }

    private static void onComplete(
            long submittedAt,
            LatencyRecorder latency,
            SubmissionOutcome outcome,
            Throwable failure,
            LongAdder accepted,
            LongAdder rejected,
            LongAdder executions,
            CountDownLatch completed) {
        latency.record(System.nanoTime() - submittedAt);
        if (failure == null && outcome != null) {
            if (outcome.accepted()) {
                accepted.increment();
                executions.add(outcome.executionCount());
            } else {
                rejected.increment();
            }
        } else {
            rejected.increment();
        }
        completed.countDown();
    }

    private static void pace(long ordersPerSecond, long startedAt, long slot) {
        if (ordersPerSecond <= 0) {
            return;
        }
        long target = startedAt + (long) (slot * (1_000_000_000.0 / ordersPerSecond));
        long remaining;
        while ((remaining = target - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
        }
    }

    private static List<String> symbols(int count) {
        return java.util.stream.IntStream.range(0, count).mapToObj(i -> "SYM" + i).toList();
    }

    private static final java.util.Set<String> NETWORK_TARGETS =
            java.util.Set.of("rest-json", "rest-protobuf", "tcp-binary");

    // Package-private (not private) specifically so WorkloadTest/LoadTestRunnerArgumentsTest in this
    // package can exercise the parser directly instead of shelling out to main().
    record Arguments(
            String workloadName, String targetName, int producerCount, Integer orderCountOverride,
            Integer workerCountOverride, Long seedOverride, String host, int httpPort, int tcpPort,
            String apiKey) {

        boolean isNetworkTarget() {
            return NETWORK_TARGETS.contains(targetName);
        }

        Workload workload() {
            Workload base = Workload.byName(workloadName);
            int orderCount = orderCountOverride != null ? orderCountOverride : base.orderCount();
            int workerCount = workerCountOverride != null ? workerCountOverride : base.workerCount();
            long seed = seedOverride != null ? seedOverride : base.seed();
            return new Workload(
                    base.name(), base.symbolCount(), base.ordersPerSecond(), base.cancelRatio(),
                    base.matchBandTicks(), orderCount, workerCount, base.queueCapacity(), seed);
        }

        SubmissionTarget buildTarget(Workload workload) {
            return switch (targetName) {
                case "router" -> new OrderRouterTarget(
                        new OrderRouter(workload.workerCount(), workload.queueCapacity()));
                case "coarse-lock" -> new CoarseLockedMatchingHarness();
                case "queue-array" -> new QueueComparisonHarness(
                        workload.workerCount(), workload.queueCapacity(), ArrayBlockingQueue::new);
                case "queue-linked" -> new QueueComparisonHarness(
                        workload.workerCount(), workload.queueCapacity(), LinkedBlockingQueue::new);
                case "rest-json" -> new RestJsonSubmissionTarget(host, httpPort, apiKey);
                case "rest-protobuf" -> new RestProtobufSubmissionTarget(host, httpPort, apiKey);
                case "tcp-binary" -> new TcpBinarySubmissionTarget(host, tcpPort, apiKey);
                default -> throw new IllegalArgumentException("unknown target: " + targetName);
            };
        }

        static Arguments parse(String[] args) {
            String workloadName = "steady";
            String targetName = "router";
            int producerCount = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors()));
            Integer orderCountOverride = null;
            Integer workerCountOverride = null;
            Long seedOverride = null;
            String host = "localhost";
            int httpPort = 8080;
            int tcpPort = 9090;
            String apiKey = "local-dev-key";
            for (String argument : args) {
                String[] pair = argument.split("=", 2);
                if (pair.length != 2 || !pair[0].startsWith("--")) {
                    throw new IllegalArgumentException("expected --name=value but got: " + argument);
                }
                switch (pair[0]) {
                    case "--workload" -> workloadName = pair[1];
                    case "--target" -> targetName = pair[1];
                    case "--producers" -> producerCount = Integer.parseInt(pair[1]);
                    case "--orders" -> orderCountOverride = Integer.parseInt(pair[1]);
                    case "--workers" -> workerCountOverride = Integer.parseInt(pair[1]);
                    case "--seed" -> seedOverride = Long.parseLong(pair[1]);
                    case "--host" -> host = pair[1];
                    case "--http-port" -> httpPort = Integer.parseInt(pair[1]);
                    case "--tcp-port" -> tcpPort = Integer.parseInt(pair[1]);
                    case "--api-key" -> apiKey = pair[1];
                    default -> throw new IllegalArgumentException("unknown option: " + pair[0]);
                }
            }
            return new Arguments(
                    workloadName, targetName, producerCount, orderCountOverride, workerCountOverride,
                    seedOverride, host, httpPort, tcpPort, apiKey);
        }
    }
}
