package com.tradingplatform.gateway.tcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.tcp.protocol.ClientMessage;
import com.tradingplatform.gateway.tcp.protocol.FrameIO;
import com.tradingplatform.gateway.tcp.protocol.OrderEntryCodec;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Drives the real {@link TcpServer} over real sockets — the same protocol a genuine client would
 * speak, not a shortcut through {@code TcpConnection}'s internals. Reuses the "test" profile's
 * accounts/instruments ({@code trading-gateway/src/test/resources/application-test.yml}) with TCP
 * turned on and bound to an ephemeral port, the same override {@code
 * TcpAwareTradingEventPublisherWiringTest} already established works cleanly.
 */
@SpringBootTest(properties = {
    "trading.tcp.enabled=true", "trading.tcp.port=0", "trading.tcp.idle-timeout=1s",
    "trading.tcp.auth-timeout=1s", "trading.tcp.max-frame-length=64"
})
@ActiveProfiles("test")
class TcpConnectionIntegrationTest {
    private static final String API_KEY = "key-one";
    private static final AtomicLong REQUEST_IDS = new AtomicLong();
    private static final AtomicLong CLIENT_ORDER_IDS = new AtomicLong();

    @Autowired private TcpServer server;

    @Test
    void authenticatesThenSubmitsAndReceivesAnAcceptedAck() throws Exception {
        try (TestClient client = TestClient.connectAndAuthenticate(server.port(), API_KEY)) {
            ServerMessage.Ack ack = client.submit("AAPL", Side.BUY, 10, 19_000);

            assertTrue(ack.accepted());
            assertTrue(ack.orderId() > 0);
            assertEquals(OrderType.LIMIT, ack.order().orElseThrow().type());
        }
    }

    @Test
    void aBadApiKeyIsRejectedAndTheConnectionCloses() throws Exception {
        try (Socket socket = new Socket("localhost", server.port())) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            FrameIO.writeFrame(out, OrderEntryCodec.encode(new ClientMessage.Auth("not-a-real-key")));

            ServerMessage response =
                    OrderEntryCodec.decodeServerMessage(FrameIO.readFrame(in, 4096).orElseThrow());

            assertInstanceOf(ServerMessage.AuthReject.class, response);
            // The server closes its side after rejecting; the next read reaching a clean EOF (empty)
            // rather than blocking forever is the proof, not just that a reject message arrived.
            assertEquals(java.util.Optional.empty(), FrameIO.readFrame(in, 4096));
        }
    }

    @Test
    void cancelRoundTripsAgainstAnOrderJustSubmittedOnTheSameConnection() throws Exception {
        try (TestClient client = TestClient.connectAndAuthenticate(server.port(), API_KEY)) {
            ServerMessage.Ack submitAck = client.submit("MSFT", Side.SELL, 5, 42_500);
            ServerMessage.Ack cancelAck = client.cancel(submitAck.orderId());

            assertTrue(cancelAck.accepted());
            assertEquals(
                    com.tradingplatform.domain.OrderStatus.CANCELLED,
                    cancelAck.order().orElseThrow().status());
        }
    }

    @Test
    void replaceRoundTripsAgainstAnOrderJustSubmittedOnTheSameConnection() throws Exception {
        try (TestClient client = TestClient.connectAndAuthenticate(server.port(), API_KEY)) {
            ServerMessage.Ack submitAck = client.submit("MSFT", Side.SELL, 5, 42_500);
            ServerMessage.Ack replaceAck = client.replace(submitAck.orderId(), 3, 42_600);

            assertTrue(replaceAck.accepted());
            assertEquals(3, replaceAck.order().orElseThrow().quantity());
        }
    }

    @Test
    void anOversizedFrameGetsAProtocolErrorRatherThanHangingTheConnection() throws Exception {
        try (TestClient client = TestClient.connectAndAuthenticate(server.port(), API_KEY)) {
            // max-frame-length is 64 for this test; force a declared length larger than that.
            client.out.writeInt(10_000);
            client.out.write(new byte[10]);
            client.out.flush();

            ServerMessage response = client.readOneServerMessage();
            assertInstanceOf(ServerMessage.Error.class, response);
        }
    }

    @Test
    void anIdleConnectionIsDisconnectedAfterTheConfiguredTimeout() throws Exception {
        // idle-timeout is 1s for this test (see the class's @SpringBootTest properties).
        try (TestClient client = TestClient.connectAndAuthenticate(server.port(), API_KEY)) {
            // Sends nothing and just waits: SO_TIMEOUT firing server-side is what closes this.
            java.util.Optional<byte[]> frame = FrameIO.readFrame(client.in, 65_536);
            assertEquals(java.util.Optional.empty(), frame);
        }
    }

    @Test
    void twoConnectionsOnOppositeSidesOfATradeEachGetTheirOwnExecutionPush() throws Exception {
        // A fresh, uncrossed price level so this test's resting order isn't hit by leftover state
        // from another test sharing the same class-wide context.
        long price = 12_345 + (System.nanoTime() % 100);
        try (TestClient resting = TestClient.connectAndAuthenticate(server.port(), "key-two");
                TestClient aggressor = TestClient.connectAndAuthenticate(server.port(), "key-three")) {
            ServerMessage.Ack restingAck = resting.submit("AAPL", Side.SELL, 4, price);
            assertTrue(restingAck.accepted());

            ServerMessage.Ack aggressorAck = aggressor.submit("AAPL", Side.BUY, 4, price);
            assertTrue(aggressorAck.accepted());
            assertEquals(1, aggressorAck.executions().size());

            ServerMessage push = resting.readOneServerMessage();
            ServerMessage.ExecutionPush executionPush = assertInstanceOf(ServerMessage.ExecutionPush.class, push);
            assertEquals(restingAck.orderId(), executionPush.orderId());
            assertTrue(executionPush.maker());
            assertEquals(0, executionPush.remainingQuantity());
        }
    }

    /** A minimal, protocol-faithful client used only by this test — real socket, real framing, no
     * shortcut through server internals. */
    private static final class TestClient implements AutoCloseable {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;

        private TestClient(Socket socket) throws IOException {
            this.socket = socket;
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
        }

        static TestClient connectAndAuthenticate(int port, String apiKey) throws IOException {
            TestClient client = new TestClient(new Socket("localhost", port));
            client.send(new ClientMessage.Auth(apiKey));
            ServerMessage response = client.readOneServerMessage();
            if (!(response instanceof ServerMessage.AuthAck)) {
                throw new AssertionError("expected AuthAck, got " + response);
            }
            return client;
        }

        ServerMessage.Ack submit(String symbol, Side side, long quantity, long price) throws IOException {
            long requestId = REQUEST_IDS.incrementAndGet();
            send(new ClientMessage.Submit(
                    requestId, "client-" + CLIENT_ORDER_IDS.incrementAndGet(), symbol, side,
                    OrderType.LIMIT, quantity, price, ""));
            return (ServerMessage.Ack) readOneServerMessage();
        }

        ServerMessage.Ack cancel(long orderId) throws IOException {
            send(new ClientMessage.Cancel(REQUEST_IDS.incrementAndGet(), orderId));
            return (ServerMessage.Ack) readOneServerMessage();
        }

        ServerMessage.Ack replace(long orderId, long quantity, long price) throws IOException {
            send(new ClientMessage.Replace(REQUEST_IDS.incrementAndGet(), orderId, quantity, price));
            return (ServerMessage.Ack) readOneServerMessage();
        }

        private void send(ClientMessage message) throws IOException {
            FrameIO.writeFrame(out, OrderEntryCodec.encode(message));
        }

        ServerMessage readOneServerMessage() throws IOException {
            byte[] frame = FrameIO.readFrame(in, 65_536).orElseThrow(
                    () -> new AssertionError("connection closed before a response arrived"));
            return OrderEntryCodec.decodeServerMessage(frame);
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
