package com.tradingplatform.benchmark.load;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderAckProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderTypeProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SideProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SubmitOrderProto;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stage 10's HTTP+Protobuf comparison target — {@code /protobuf/orders}, the same ingress pipeline
 * as {@link RestJsonSubmissionTarget} and {@code OrderRouterTarget}, encoded with the generated
 * {@code OrderEntryProto} types instead of JSON. See {@link RestJsonSubmissionTarget}'s Javadoc for
 * why {@link #serverOrderIds} exists.
 */
public final class RestProtobufSubmissionTarget implements SubmissionTarget {
    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

    private final HttpClient client = HttpClient.newHttpClient();
    private final URI ordersUri;
    private final String apiKey;
    private final Map<Long, Long> serverOrderIds = new ConcurrentHashMap<>();

    public RestProtobufSubmissionTarget(String host, int port, String apiKey) {
        this.ordersUri = URI.create("http://" + host + ":" + port + "/protobuf/orders");
        this.apiKey = apiKey;
    }

    @Override
    public CompletableFuture<SubmissionOutcome> submit(SubmitOrder command) {
        SubmitOrderProto request = SubmitOrderProto.newBuilder()
                .setClientOrderId(command.clientOrderId())
                .setSymbol(command.symbol())
                .setSide(command.side() == Side.BUY ? SideProto.BUY : SideProto.SELL)
                .setType(command.type() == OrderType.LIMIT ? OrderTypeProto.LIMIT : OrderTypeProto.MARKET)
                .setQuantity(command.quantity())
                .setPrice(command.price())
                .build();
        HttpRequest httpRequest = HttpRequest.newBuilder(ordersUri)
                .header("X-Api-Key", apiKey)
                .header("Content-Type", PROTOBUF_CONTENT_TYPE)
                .header("Accept", PROTOBUF_CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(request.toByteArray()))
                .build();
        long localOrderId = command.orderId();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> handleAck(localOrderId, response));
    }

    @Override
    public CompletableFuture<SubmissionOutcome> cancel(CancelOrder command) {
        Long serverOrderId = serverOrderIds.get(command.orderId());
        if (serverOrderId == null) {
            return CompletableFuture.completedFuture(new SubmissionOutcome(false, 0));
        }
        HttpRequest httpRequest = HttpRequest.newBuilder(
                        ordersUri.resolve("/protobuf/orders/" + serverOrderId))
                .header("X-Api-Key", apiKey)
                .header("Accept", PROTOBUF_CONTENT_TYPE)
                .DELETE()
                .build();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> handleAck(command.orderId(), response));
    }

    private SubmissionOutcome handleAck(long localOrderId, HttpResponse<byte[]> response) {
        if (response.statusCode() != 200) {
            return new SubmissionOutcome(false, 0);
        }
        OrderAckProto ack = parse(response.body());
        if (ack.getAccepted()) {
            serverOrderIds.put(localOrderId, ack.getOrderId());
        }
        return new SubmissionOutcome(ack.getAccepted(), ack.getExecutionsCount());
    }

    private static OrderAckProto parse(byte[] body) {
        try {
            return OrderAckProto.parseFrom(body);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("malformed OrderAckProto response", e);
        }
    }

    @Override
    public int queueDepth() {
        return 0;
    }

    @Override
    public void close() {
        client.close();
    }
}
