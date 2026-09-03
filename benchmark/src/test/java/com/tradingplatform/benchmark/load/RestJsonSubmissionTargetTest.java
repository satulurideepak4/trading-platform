package com.tradingplatform.benchmark.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.gateway.api.OrderResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link RestJsonSubmissionTarget}'s own bookkeeping (the local order-id map) against a
 * minimal fake HTTP server, not a live gateway. {@link RestProtobufSubmissionTarget} follows the
 * identical shape — one {@code java.net.http.HttpClient} call, one response parsed, the same map
 * populated the same way — and is exercised for real against a live gateway by the actual benchmark
 * runs rather than duplicating this same test a second time for a different content type.
 */
class RestJsonSubmissionTargetTest {
    private static final Instant NOW = Instant.parse("2026-01-01T09:30:00Z");
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private HttpServer server;
    private final AtomicLong nextServerOrderId = new AtomicLong(1000);

    @BeforeEach
    void startFakeServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/orders", this::handle);
        server.start();
    }

    @AfterEach
    void stopFakeServer() {
        server.stop(0);
    }

    @Test
    void submitPopulatesTheLocalOrderIdMapAndCancelUsesIt() throws Exception {
        try (RestJsonSubmissionTarget target =
                new RestJsonSubmissionTarget("localhost", server.getAddress().getPort(), "any-key")) {
            SubmitOrder submit = SubmitOrder.limit(1, "client-1", "AAPL", Side.BUY, 10, 19_000, NOW);

            SubmissionOutcome submitOutcome = target.submit(submit).get(5, TimeUnit.SECONDS);
            assertTrue(submitOutcome.accepted());

            SubmissionOutcome cancelOutcome =
                    target.cancel(new CancelOrder(1, NOW)).get(5, TimeUnit.SECONDS);
            assertTrue(cancelOutcome.accepted());
        }
    }

    @Test
    void cancelWithNoPriorSubmitIsReportedAsUnaccepted() throws Exception {
        try (RestJsonSubmissionTarget target =
                new RestJsonSubmissionTarget("localhost", server.getAddress().getPort(), "any-key")) {
            SubmissionOutcome outcome = target.cancel(new CancelOrder(999, NOW)).get(5, TimeUnit.SECONDS);

            assertEquals(new SubmissionOutcome(false, 0), outcome);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            byte[] body = switch (exchange.getRequestMethod()) {
                case "POST" -> handleSubmit();
                case "DELETE" -> handleCancel();
                default -> throw new AssertionError("unexpected method " + exchange.getRequestMethod());
            };
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
        } finally {
            exchange.close();
        }
    }

    private byte[] handleSubmit() throws IOException {
        long serverOrderId = nextServerOrderId.incrementAndGet();
        OrderResponse response = new OrderResponse(
                serverOrderId, "client-1", "AAPL", Side.BUY, OrderType.LIMIT, OrderStatus.NEW, 10, 10,
                0, 19_000, NOW, NOW, List.of(), false);
        return MAPPER.writeValueAsBytes(response);
    }

    private byte[] handleCancel() throws IOException {
        OrderResponse response = new OrderResponse(
                1001, "client-1", "AAPL", Side.BUY, OrderType.LIMIT, OrderStatus.CANCELLED, 10, 0, 10,
                19_000, NOW, NOW, List.of(), false);
        return MAPPER.writeValueAsBytes(response);
    }
}
