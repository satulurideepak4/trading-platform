package com.tradingplatform.simulator;

import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.routing.OrderRouter;
import com.tradingplatform.matching.routing.OrderRoutingRejectedException;
import com.tradingplatform.matching.routing.RouterMetricsSnapshot;
import com.tradingplatform.matching.routing.WorkerMetricsSnapshot;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/** Configurable Stage 2 stress workload. Its timings are not controlled benchmark evidence. */
public final class ConcurrentOrderFlowSimulator {
    private static final Instant BASE_TIME = Instant.parse("2026-01-01T00:00:00Z");

    private ConcurrentOrderFlowSimulator() {}

    public static void main(String[] args) throws Exception {
        Configuration configuration = Configuration.parse(args);
        List<String> symbols = symbols(configuration.symbolCount());
        LongAdder admitted = new LongAdder();
        LongAdder saturated = new LongAdder();
        LongAdder accepted = new LongAdder();
        LongAdder rejected = new LongAdder();
        LongAdder executions = new LongAdder();
        LongAdder failed = new LongAdder();
        CountDownLatch completed = new CountDownLatch(configuration.orderCount());
        AtomicLong pacingSlot = new AtomicLong();
        long workloadStartedAt = System.nanoTime();

        try (OrderRouter router =
                        new OrderRouter(configuration.workerCount(), configuration.queueCapacity());
                ExecutorService producers =
                        Executors.newFixedThreadPool(configuration.producerCount())) {
            List<Future<?>> producerTasks = new ArrayList<>(configuration.producerCount());
            for (int producerId = 0; producerId < configuration.producerCount(); producerId++) {
                int assignedProducerId = producerId;
                producerTasks.add(producers.submit(() -> produce(
                        assignedProducerId,
                        configuration,
                        symbols,
                        router,
                        workloadStartedAt,
                        pacingSlot,
                        admitted,
                        saturated,
                        accepted,
                        rejected,
                        executions,
                        failed,
                        completed)));
            }
            producers.shutdown();
            for (Future<?> task : producerTasks) {
                task.get();
            }
            completed.await();

            long elapsedNanos = System.nanoTime() - workloadStartedAt;
            printSummary(
                    configuration,
                    router.metrics(),
                    elapsedNanos,
                    admitted.sum(),
                    saturated.sum(),
                    accepted.sum(),
                    rejected.sum(),
                    executions.sum(),
                    failed.sum());
        }
    }

    private static void produce(
            int producerId,
            Configuration configuration,
            List<String> symbols,
            OrderRouter router,
            long workloadStartedAt,
            AtomicLong pacingSlot,
            LongAdder admitted,
            LongAdder saturated,
            LongAdder accepted,
            LongAdder rejected,
            LongAdder executions,
            LongAdder failed,
            CountDownLatch completed) {
        SplittableRandom random =
                new SplittableRandom(configuration.seed() + 104_729L * producerId);
        for (long index = producerId;
                index < configuration.orderCount();
                index += configuration.producerCount()) {
            pace(configuration.ordersPerSecond(), workloadStartedAt, pacingSlot.getAndIncrement());
            long orderId = index + 1;
            SubmitOrder command = generatedOrder(orderId, symbols, random);
            try {
                router.submit(command).whenComplete((routed, failure) -> {
                    if (failure != null) {
                        failed.increment();
                    } else {
                        CommandResult result = routed.value();
                        if (result.accepted()) {
                            accepted.increment();
                            executions.add(result.executions().size());
                        } else {
                            rejected.increment();
                        }
                    }
                    completed.countDown();
                });
                admitted.increment();
            } catch (OrderRoutingRejectedException queueRejection) {
                saturated.increment();
                completed.countDown();
            }
        }
    }

    private static void pace(long ordersPerSecond, long startedAt, long slot) {
        if (ordersPerSecond == 0) {
            return;
        }
        long target = startedAt + (long) (slot * (1_000_000_000.0 / ordersPerSecond));
        long remaining;
        while ((remaining = target - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
        }
    }

    private static SubmitOrder generatedOrder(
            long orderId, List<String> symbols, SplittableRandom random) {
        String symbol = symbols.get(random.nextInt(symbols.size()));
        Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
        long quantity = random.nextLong(1, 101);
        Instant timestamp = BASE_TIME.plusNanos(orderId);
        if (random.nextInt(20) == 0) {
            return SubmitOrder.market(
                    orderId, "stress-" + orderId, symbol, side, quantity, timestamp);
        }
        return SubmitOrder.limit(
                orderId,
                "stress-" + orderId,
                symbol,
                side,
                quantity,
                random.nextLong(9_900, 10_101),
                timestamp);
    }

    private static List<String> symbols(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(index -> "SYM" + index)
                .toList();
    }

    private static void printSummary(
            Configuration configuration,
            RouterMetricsSnapshot metrics,
            long elapsedNanos,
            long admitted,
            long saturated,
            long accepted,
            long rejected,
            long executions,
            long failed) {
        System.out.printf(
                Locale.ROOT,
                "event=concurrent_simulation_complete symbols=%d producers=%d workers=%d "
                        + "orders=%d targetOrdersPerSecond=%d admitted=%d saturated=%d "
                        + "accepted=%d rejected=%d failed=%d executions=%d elapsedMs=%.3f%n",
                configuration.symbolCount(),
                configuration.producerCount(),
                configuration.workerCount(),
                configuration.orderCount(),
                configuration.ordersPerSecond(),
                admitted,
                saturated,
                accepted,
                rejected,
                failed,
                executions,
                elapsedNanos / 1_000_000.0);
        for (WorkerMetricsSnapshot worker : metrics.workers()) {
            System.out.printf(
                    Locale.ROOT,
                    "event=worker_metrics worker=%d admitted=%d saturated=%d processed=%d "
                            + "queueDepth=%d avgProcessingNanos=%.1f maxProcessingNanos=%d%n",
                    worker.workerId(),
                    worker.admittedCommands(),
                    worker.saturatedCommands(),
                    worker.processedCommands(),
                    worker.queueDepth(),
                    worker.averageProcessingNanos(),
                    worker.maximumProcessingNanos());
        }
        System.out.println(
                "note=This is a stress/smoke workload, not a controlled benchmark result.");
    }

    private record Configuration(
            int symbolCount,
            int producerCount,
            int orderCount,
            long ordersPerSecond,
            int workerCount,
            int queueCapacity,
            long seed) {

        private static Configuration parse(String[] args) {
            int symbolCount = 16;
            int producerCount = 4;
            int orderCount = 100_000;
            long ordersPerSecond = 0;
            int workerCount = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors()));
            int queueCapacity = 8_192;
            long seed = 7_921;

            for (String argument : args) {
                String[] pair = argument.split("=", 2);
                if (pair.length != 2 || !pair[0].startsWith("--")) {
                    throw new IllegalArgumentException("expected --name=value but got: " + argument);
                }
                switch (pair[0]) {
                    case "--symbols" -> symbolCount = Integer.parseInt(pair[1]);
                    case "--producers" -> producerCount = Integer.parseInt(pair[1]);
                    case "--orders" -> orderCount = Integer.parseInt(pair[1]);
                    case "--orders-per-second" -> ordersPerSecond = Long.parseLong(pair[1]);
                    case "--workers" -> workerCount = Integer.parseInt(pair[1]);
                    case "--queue-capacity" -> queueCapacity = Integer.parseInt(pair[1]);
                    case "--seed" -> seed = Long.parseLong(pair[1]);
                    default -> throw new IllegalArgumentException("unknown option: " + pair[0]);
                }
            }
            if (symbolCount <= 0
                    || producerCount <= 0
                    || orderCount <= 0
                    || ordersPerSecond < 0
                    || workerCount <= 0
                    || queueCapacity <= 0) {
                throw new IllegalArgumentException(
                        "symbols, producers, orders, workers, and queue capacity must be positive; "
                                + "orders per second must be non-negative");
            }
            return new Configuration(
                    symbolCount,
                    producerCount,
                    orderCount,
                    ordersPerSecond,
                    workerCount,
                    queueCapacity,
                    seed);
        }
    }
}
