package com.tradingplatform.matching.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.OrderBookSnapshot;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class OrderRouterTest {
    private static final Instant BASE_TIME = Instant.parse("2026-01-01T09:30:00Z");

    @Test
    void simultaneousSubmissionsForOneSymbolHaveOneOwnerAndNoLostOrders() throws Exception {
        int producerCount = 8;
        int ordersPerProducer = 200;
        try (OrderRouter router = new OrderRouter(4, 4_096);
                ExecutorService producers = Executors.newFixedThreadPool(producerCount)) {
            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<RoutedResult<CommandResult>>> completions =
                    java.util.Collections.synchronizedList(new ArrayList<>());

            for (int producer = 0; producer < producerCount; producer++) {
                int producerId = producer;
                producers.submit(() -> {
                    start.await();
                    for (int index = 0; index < ordersPerProducer; index++) {
                        long orderId = producerId * ordersPerProducer + index + 1L;
                        completions.add(router.submit(limit(
                                orderId, "AAPL", Side.BUY, 1, 9_900, orderId)));
                    }
                    return null;
                });
            }

            start.countDown();
            producers.shutdown();
            assertTrue(producers.awaitTermination(10, TimeUnit.SECONDS));
            List<RoutedResult<CommandResult>> results = awaitAll(completions);

            assertEquals(producerCount * ordersPerProducer, results.size());
            assertEquals(
                    Set.of(router.workerIndex("AAPL")),
                    results.stream().map(RoutedResult::workerId).collect(java.util.stream.Collectors.toSet()));
            assertEquals(
                    producerCount * ordersPerProducer,
                    await(router.book("AAPL")).value().bids().size());
        }
    }

    @Test
    void concurrentArrivalOrderBecomesFifoTimePriority() throws Exception {
        int orderCount = 300;
        try (OrderRouter router = new OrderRouter(3, 1_024);
                ExecutorService producers = Executors.newFixedThreadPool(6)) {
            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<RoutedResult<CommandResult>>> submissions =
                    java.util.Collections.synchronizedList(new ArrayList<>());

            for (int index = 0; index < orderCount; index++) {
                long orderId = index + 1L;
                producers.submit(() -> {
                    start.await();
                    submissions.add(router.submit(
                            limit(orderId, "MSFT", Side.SELL, 1, 10_000, orderId)));
                    return null;
                });
            }
            start.countDown();
            producers.shutdown();
            assertTrue(producers.awaitTermination(10, TimeUnit.SECONDS));

            List<Long> expectedMakerOrder = awaitAll(submissions).stream()
                    .sorted(Comparator.comparingLong(RoutedResult::processingSequence))
                    .map(result -> result.value().orderId())
                    .toList();
            RoutedResult<CommandResult> sweep = await(router.submit(
                    SubmitOrder.market(10_000, "sweep", "MSFT", Side.BUY, orderCount, at(10_000))));

            assertEquals(
                    expectedMakerOrder,
                    sweep.value().executions().stream().map(Execution::makerOrderId).toList());
        }
    }

    @Test
    void differentSymbolsAreOwnedByDifferentWorkersAndExecutionIdsDoNotCollide() {
        try (OrderRouter router = new OrderRouter(4, 64)) {
            String firstSymbol = symbolForWorker(router, 0);
            String secondSymbol = symbolForWorker(router, 1);
            assertNotEquals(router.workerIndex(firstSymbol), router.workerIndex(secondSymbol));

            await(router.submit(limit(1, firstSymbol, Side.SELL, 2, 10_000, 1)));
            await(router.submit(limit(2, secondSymbol, Side.SELL, 2, 20_000, 2)));
            RoutedResult<CommandResult> firstFill = await(router.submit(
                    SubmitOrder.market(3, "client-3", firstSymbol, Side.BUY, 2, at(3))));
            RoutedResult<CommandResult> secondFill = await(router.submit(
                    SubmitOrder.market(4, "client-4", secondSymbol, Side.BUY, 2, at(4))));

            assertEquals(0, firstFill.workerId());
            assertEquals(1, secondFill.workerId());
            Set<Long> executionIds = Set.of(
                    firstFill.value().executions().getFirst().executionId(),
                    secondFill.value().executions().getFirst().executionId());
            assertEquals(2, executionIds.size());
        }
    }

    @Test
    void cancellationIsOrderedWithTrafficForTheSameSymbol() {
        try (OrderRouter router = new OrderRouter(2, 512)) {
            await(router.submit(limit(1, "NVDA", Side.BUY, 1, 10_000, 1)));
            List<CompletableFuture<RoutedResult<CommandResult>>> traffic = IntStream.range(0, 100)
                    .mapToObj(index -> router.submit(limit(
                            index + 2L, "NVDA", Side.BUY, 1, 9_900, index + 2L)))
                    .toList();
            CompletableFuture<RoutedResult<CommandResult>> cancellation =
                    router.cancel(new CancelOrder(1, at(200)));
            CompletableFuture<RoutedResult<CommandResult>> sweep = router.submit(
                    SubmitOrder.market(500, "sweep", "NVDA", Side.SELL, 1, at(201)));

            awaitAll(traffic);
            assertEquals(OrderStatus.CANCELLED, await(cancellation).value().resultingStatus());
            assertFalse(await(sweep).value().executions().stream()
                    .anyMatch(execution -> execution.makerOrderId() == 1));
        }
    }

    @Test
    void fullQueueRejectsImmediatelyAndDoesNotReserveIdentifiers() {
        try (OrderRouter router = new OrderRouter(1, 2, false)) {
            CompletableFuture<RoutedResult<CommandResult>> first =
                    router.submit(limit(1, "AAPL", Side.BUY, 1, 10_000, 1));
            CompletableFuture<RoutedResult<CommandResult>> second =
                    router.submit(limit(2, "AAPL", Side.BUY, 1, 10_000, 2));

            OrderRoutingRejectedException rejection = assertThrows(
                    OrderRoutingRejectedException.class,
                    () -> router.submit(limit(3, "AAPL", Side.BUY, 1, 10_000, 3)));
            assertEquals(RoutingRejectionReason.QUEUE_FULL, rejection.reason());
            assertEquals(1, router.metrics().saturatedCommands());
            assertEquals(2, router.metrics().queueDepth());

            router.startWorkers();
            assertTrue(await(first).value().accepted());
            assertTrue(await(second).value().accepted());
            assertTrue(await(router.submit(limit(3, "AAPL", Side.BUY, 1, 10_000, 3)))
                    .value()
                    .accepted());
        }
    }

    @Test
    void duplicateIdsRemainGlobalAcrossWorkerShards() {
        try (OrderRouter router = new OrderRouter(4, 64)) {
            String firstSymbol = symbolForWorker(router, 0);
            String secondSymbol = symbolForWorker(router, 1);
            await(router.submit(limit(1, firstSymbol, Side.BUY, 1, 10_000, 1)));

            RoutedResult<CommandResult> duplicate = await(router.submit(
                    SubmitOrder.limit(
                            1, "different-client", secondSymbol, Side.BUY, 1, 10_000, at(2))));

            assertFalse(duplicate.value().accepted());
            assertEquals(0, duplicate.workerId());
        }
    }

    @Test
    void orderedReplayProducesIdenticalDomainResultsAndState() {
        ReplayOutcome first = replayScenario();
        ReplayOutcome second = replayScenario();

        assertEquals(first.results(), second.results());
        assertEquals(first.processingMetadata(), second.processingMetadata());
        assertEquals(first.book(), second.book());
    }

    @Test
    void metricsReportQueueAndProcessingMeasurements() {
        try (OrderRouter router = new OrderRouter(2, 32)) {
            RoutedResult<CommandResult> result =
                    await(router.submit(limit(1, "AAPL", Side.BUY, 1, 10_000, 1)));
            RouterMetricsSnapshot metrics = router.metrics();

            assertTrue(result.queueWaitNanos() >= 0);
            assertTrue(result.processingNanos() >= 0);
            assertEquals(1, metrics.admittedCommands());
            assertEquals(1, metrics.processedCommands());
            WorkerMetricsSnapshot worker = metrics.workers().get(result.workerId());
            assertTrue(worker.totalProcessingNanos() >= result.processingNanos());
        }
    }

    @Test
    void closedRouterRejectsNewCommands() {
        OrderRouter router = new OrderRouter(1, 8);
        router.close();

        OrderRoutingRejectedException rejection = assertThrows(
                OrderRoutingRejectedException.class,
                () -> router.submit(limit(1, "AAPL", Side.BUY, 1, 10_000, 1)));
        assertEquals(RoutingRejectionReason.ROUTER_CLOSED, rejection.reason());
    }

    private static ReplayOutcome replayScenario() {
        try (OrderRouter router = new OrderRouter(3, 64)) {
            List<RoutedResult<CommandResult>> routed = List.of(
                    await(router.submit(limit(1, "AAPL", Side.SELL, 7, 10_100, 1))),
                    await(router.submit(limit(2, "AAPL", Side.SELL, 8, 10_000, 2))),
                    await(router.submit(limit(3, "AAPL", Side.BUY, 10, 10_100, 3))),
                    await(router.replace(new ReplaceOrder(1, 9, 10_050, at(4)))),
                    await(router.submit(
                            SubmitOrder.market(4, "client-4", "AAPL", Side.BUY, 2, at(5)))),
                    await(router.submit(limit(5, "MSFT", Side.BUY, 3, 9_900, 6))),
                    await(router.cancel(new CancelOrder(5, at(7)))));
            List<CommandResult> results = routed.stream().map(RoutedResult::value).toList();
            List<List<Long>> processingMetadata = routed.stream()
                    .map(result -> List.of(
                            result.routingSequence(),
                            result.processingSequence(),
                            (long) result.workerId()))
                    .toList();
            return new ReplayOutcome(results, processingMetadata, await(router.book("AAPL")).value());
        }
    }

    private static String symbolForWorker(OrderRouter router, int workerId) {
        return IntStream.range(0, 10_000)
                .mapToObj(index -> "SYM" + index)
                .filter(symbol -> router.workerIndex(symbol) == workerId)
                .findFirst()
                .orElseThrow();
    }

    private static SubmitOrder limit(
            long orderId,
            String symbol,
            Side side,
            long quantity,
            long price,
            long timestampOffset) {
        return SubmitOrder.limit(
                orderId,
                "client-" + orderId,
                symbol,
                side,
                quantity,
                price,
                at(timestampOffset));
    }

    private static Instant at(long offset) {
        return BASE_TIME.plusNanos(offset);
    }

    private static <T> RoutedResult<T> await(CompletableFuture<RoutedResult<T>> completion) {
        return completion.orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static <T> List<RoutedResult<T>> awaitAll(
            List<CompletableFuture<RoutedResult<T>>> completions) {
        CompletableFuture.allOf(completions.toArray(CompletableFuture[]::new))
                .orTimeout(10, TimeUnit.SECONDS)
                .join();
        return completions.stream().map(CompletableFuture::join).toList();
    }

    private record ReplayOutcome(
            List<CommandResult> results,
            List<List<Long>> processingMetadata,
            OrderBookSnapshot book) {}
}
