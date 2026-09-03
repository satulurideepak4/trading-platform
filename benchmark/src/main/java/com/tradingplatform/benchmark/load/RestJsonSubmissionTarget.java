package com.tradingplatform.benchmark.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.gateway.api.OrderResponse;
import com.tradingplatform.gateway.api.SubmitOrderRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stage 10's HTTP+JSON comparison target — the real {@code /orders} endpoint, exercised the way an
 * actual client would: real JSON bodies via the same {@link SubmitOrderRequest}/{@link
 * OrderResponse} DTOs the gateway ships, over a real {@link HttpClient} connection.
 *
 * <p>The gateway assigns its own order id on submit, distinct from the {@code orderId} {@link
 * SubmitOrder}/{@link CancelOrder} carry (which {@code LoadTestRunner}'s producers assign locally,
 * the same identifier space {@code OrderRouterTarget} uses in-process). {@link #serverOrderIds}
 * bridges the two, entirely contained here — neither {@link SubmissionTarget} nor {@link
 * SubmissionOutcome} needed to change to support a target where the server assigns identity.
 */
public final class RestJsonSubmissionTarget implements SubmissionTarget {
    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final URI ordersUri;
    private final String apiKey;
    private final Map<Long, Long> serverOrderIds = new ConcurrentHashMap<>();

    public RestJsonSubmissionTarget(String host, int port, String apiKey) {
        this.ordersUri = URI.create("http://" + host + ":" + port + "/orders");
        this.apiKey = apiKey;
    }

    @Override
    public CompletableFuture<SubmissionOutcome> submit(SubmitOrder command) {
        SubmitOrderRequest request = new SubmitOrderRequest(
                command.clientOrderId(), command.symbol(), command.side(), command.type(),
                command.quantity(), command.price(), null);
        HttpRequest httpRequest = HttpRequest.newBuilder(ordersUri)
                .header("X-Api-Key", apiKey)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(writeValue(request)))
                .build();
        long localOrderId = command.orderId();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> handleSubmitResponse(localOrderId, response));
    }

    @Override
    public CompletableFuture<SubmissionOutcome> cancel(CancelOrder command) {
        Long serverOrderId = serverOrderIds.get(command.orderId());
        if (serverOrderId == null) {
            // The submit that would have populated this either hasn't completed yet or was itself
            // rejected - nothing to cancel; a saturated-style rejection is the honest report.
            return CompletableFuture.completedFuture(new SubmissionOutcome(false, 0));
        }
        HttpRequest httpRequest = HttpRequest.newBuilder(ordersUri.resolve("/orders/" + serverOrderId))
                .header("X-Api-Key", apiKey)
                .DELETE()
                .build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
                .thenApply(response -> new SubmissionOutcome(response.statusCode() == 200, 0));
    }

    private SubmissionOutcome handleSubmitResponse(long localOrderId, HttpResponse<byte[]> response) {
        if (response.statusCode() != 200 && response.statusCode() != 201) {
            return new SubmissionOutcome(false, 0);
        }
        OrderResponse orderResponse = readValue(response.body());
        serverOrderIds.put(localOrderId, orderResponse.orderId());
        return new SubmissionOutcome(true, orderResponse.executions().size());
    }

    private byte[] writeValue(Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private OrderResponse readValue(byte[] body) {
        try {
            return mapper.readValue(body, OrderResponse.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** No queue concept from an HTTP client's perspective - each request is its own connection-
     * level exchange, mirroring {@code CoarseLockedMatchingHarness}'s "no queue" targets. */
    @Override
    public int queueDepth() {
        return 0;
    }

    @Override
    public void close() {
        client.close();
    }
}
