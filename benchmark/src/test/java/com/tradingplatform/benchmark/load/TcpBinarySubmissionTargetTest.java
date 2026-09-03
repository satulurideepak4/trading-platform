package com.tradingplatform.benchmark.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import com.tradingplatform.gateway.tcp.protocol.ClientMessage;
import com.tradingplatform.gateway.tcp.protocol.FrameIO;
import com.tradingplatform.gateway.tcp.protocol.OrderEntryCodec;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link TcpBinarySubmissionTarget}'s own bookkeeping (request/response correlation,
 * the local order-id map) against a minimal fake server speaking the real protocol classes, not a
 * live gateway — this is exactly the seam {@code OrderEntryCodec}/{@code FrameIO} exist to let a
 * test stand in on either side of.
 */
class TcpBinarySubmissionTargetTest {
    private static final Instant NOW = Instant.parse("2026-01-01T09:30:00Z");

    private FakeServer server;

    @BeforeEach
    void startFakeServer() throws IOException {
        server = new FakeServer();
        server.start();
    }

    @AfterEach
    void stopFakeServer() throws IOException {
        server.stop();
    }

    @Test
    void submitPopulatesTheLocalOrderIdMapAndCancelUsesIt() throws Exception {
        try (TcpBinarySubmissionTarget target =
                new TcpBinarySubmissionTarget("localhost", server.port(), "any-key")) {
            SubmitOrder submit = SubmitOrder.limit(1, "client-1", "AAPL", Side.BUY, 10, 19_000, NOW);

            SubmissionOutcome submitOutcome = get(target.submit(submit));
            assertTrue(submitOutcome.accepted());

            // The fake server maps requestId -> serverOrderId as requestId + 1000, deliberately
            // different from the local orderId (1) LoadTestRunner would track it under - proving
            // the target is really translating ids, not just echoing them back.
            SubmissionOutcome cancelOutcome = get(target.cancel(new CancelOrder(1, NOW)));
            assertTrue(cancelOutcome.accepted());
            assertEquals(1001L, server.lastCancelledServerOrderId());
        }
    }

    @Test
    void twoPipelinedSubmitsBothResolveCorrectlyEvenWhenTheServerAnswersOutOfOrder() throws Exception {
        server.respondToSubmitsInReverseOrder();
        try (TcpBinarySubmissionTarget target =
                new TcpBinarySubmissionTarget("localhost", server.port(), "any-key")) {
            SubmitOrder first = SubmitOrder.limit(1, "client-1", "AAPL", Side.BUY, 10, 19_000, NOW);
            SubmitOrder second = SubmitOrder.limit(2, "client-2", "AAPL", Side.SELL, 5, 19_100, NOW);

            CompletableFuture<SubmissionOutcome> firstFuture = target.submit(first);
            CompletableFuture<SubmissionOutcome> secondFuture = target.submit(second);

            assertTrue(get(firstFuture).accepted());
            assertTrue(get(secondFuture).accepted());
            // Each local orderId must map to its own request's server-assigned id, not whichever
            // response happened to arrive first.
            SubmissionOutcome firstCancel = get(target.cancel(new CancelOrder(1, NOW)));
            SubmissionOutcome secondCancel = get(target.cancel(new CancelOrder(2, NOW)));
            assertTrue(firstCancel.accepted());
            assertTrue(secondCancel.accepted());
        }
    }

    private static SubmissionOutcome get(CompletableFuture<SubmissionOutcome> future)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(5, TimeUnit.SECONDS);
    }

    /** Speaks just enough of the real protocol to drive the target's own logic, not the ingress
     * pipeline it would normally sit in front of. */
    private static final class FakeServer {
        private ServerSocket serverSocket;
        private Thread acceptThread;
        private volatile boolean reverseSubmitOrder;
        private volatile long lastCancelledServerOrderId;

        void start() throws IOException {
            serverSocket = new ServerSocket(0);
            acceptThread = Thread.ofPlatform().name("fake-tcp-server").start(this::acceptOneConnection);
        }

        void respondToSubmitsInReverseOrder() {
            reverseSubmitOrder = true;
        }

        long lastCancelledServerOrderId() {
            return lastCancelledServerOrderId;
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void stop() throws IOException {
            serverSocket.close();
        }

        private void acceptOneConnection() {
            try (Socket socket = serverSocket.accept()) {
                DataInputStream in = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                ClientMessage auth = OrderEntryCodec.decodeClientMessage(
                        FrameIO.readFrame(in, 65_536).orElseThrow());
                if (!(auth instanceof ClientMessage.Auth)) {
                    throw new AssertionError("expected AUTH first, got " + auth);
                }
                FrameIO.writeFrame(out, OrderEntryCodec.encode(new ServerMessage.AuthAck("ACC-TEST")));

                if (reverseSubmitOrder) {
                    ClientMessage firstSubmit = readOneMessage(in);
                    ClientMessage secondSubmit = readOneMessage(in);
                    respond(out, secondSubmit);
                    respond(out, firstSubmit);
                    while (true) {
                        respond(out, readOneMessage(in));
                    }
                } else {
                    while (true) {
                        respond(out, readOneMessage(in));
                    }
                }
            } catch (IOException | RuntimeException endOfTest) {
                // The test closing the socket at teardown ends this loop; nothing more to do.
            }
        }

        private ClientMessage readOneMessage(DataInputStream in) throws IOException {
            return OrderEntryCodec.decodeClientMessage(FrameIO.readFrame(in, 65_536).orElseThrow());
        }

        private void respond(DataOutputStream out, ClientMessage message) throws IOException {
            ServerMessage response = switch (message) {
                case ClientMessage.Submit submit -> ack(submit.requestId(), submit.requestId() + 1000);
                case ClientMessage.Cancel cancel -> {
                    lastCancelledServerOrderId = cancel.orderId();
                    yield ackForCancel(cancel.requestId(), cancel.orderId());
                }
                default -> throw new AssertionError("unexpected message: " + message);
            };
            FrameIO.writeFrame(out, OrderEntryCodec.encode(response));
        }

        private static ServerMessage.Ack ack(long requestId, long serverOrderId) {
            OrderSnapshot order = new OrderSnapshot(
                    serverOrderId, "client-" + requestId, "AAPL", Side.BUY, OrderType.LIMIT,
                    OrderStatus.NEW, 10, 10, 19_000, NOW, NOW, 0);
            OrderOutcome outcome = OrderOutcome.accepted("client-" + requestId, order, java.util.List.of());
            return ServerMessage.Ack.from(requestId, outcome);
        }

        private static ServerMessage.Ack ackForCancel(long requestId, long serverOrderId) {
            OrderSnapshot order = new OrderSnapshot(
                    serverOrderId, "client", "AAPL", Side.BUY, OrderType.LIMIT, OrderStatus.CANCELLED,
                    10, 0, 19_000, NOW, NOW, 0);
            OrderOutcome outcome = OrderOutcome.accepted("client", order, java.util.List.of());
            return ServerMessage.Ack.from(requestId, outcome);
        }
    }
}
