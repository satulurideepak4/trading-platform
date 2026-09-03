package com.tradingplatform.benchmark.load;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.matching.routing.OrderRouter;
import com.tradingplatform.matching.routing.RoutedResult;
import java.util.concurrent.CompletableFuture;

/**
 * Adapts the real, production {@code OrderRouter} to {@link SubmissionTarget}. The adapter lives
 * here, in the benchmark module, rather than {@code OrderRouter} implementing this interface
 * itself — {@code matching-engine} has no reason to know a benchmark harness exists.
 */
public final class OrderRouterTarget implements SubmissionTarget {
    private final OrderRouter router;

    public OrderRouterTarget(OrderRouter router) {
        this.router = router;
    }

    @Override
    public CompletableFuture<SubmissionOutcome> submit(SubmitOrder command) {
        return router.submit(command).thenApply(OrderRouterTarget::toOutcome);
    }

    @Override
    public CompletableFuture<SubmissionOutcome> cancel(CancelOrder command) {
        return router.cancel(command).thenApply(OrderRouterTarget::toOutcome);
    }

    @Override
    public int queueDepth() {
        return router.metrics().queueDepth();
    }

    @Override
    public void close() {
        router.close();
    }

    private static SubmissionOutcome toOutcome(RoutedResult<CommandResult> routed) {
        CommandResult result = routed.value();
        return new SubmissionOutcome(result.accepted(), result.executions().size());
    }
}
