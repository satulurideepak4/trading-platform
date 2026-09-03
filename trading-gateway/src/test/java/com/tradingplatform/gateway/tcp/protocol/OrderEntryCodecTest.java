package com.tradingplatform.gateway.tcp.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import com.tradingplatform.gateway.ingress.RejectionSource;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderEntryCodecTest {
    private static final Instant NOW = Instant.parse("2026-01-01T09:30:00.123456789Z");

    @Test
    void authRoundTrips() {
        ClientMessage.Auth original = new ClientMessage.Auth("local-dev-key");

        ClientMessage decoded = OrderEntryCodec.decodeClientMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void submitRoundTripsIncludingEmptyStrategyId() {
        ClientMessage.Submit original = new ClientMessage.Submit(
                42L, "client-1", "AAPL", Side.BUY, OrderType.LIMIT, 100, 19_000, "");

        ClientMessage decoded = OrderEntryCodec.decodeClientMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void cancelRoundTrips() {
        ClientMessage.Cancel original = new ClientMessage.Cancel(7L, 123L);

        ClientMessage decoded = OrderEntryCodec.decodeClientMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void replaceRoundTrips() {
        ClientMessage.Replace original = new ClientMessage.Replace(8L, 123L, 50, 19_500);

        ClientMessage decoded = OrderEntryCodec.decodeClientMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void authAckRoundTrips() {
        ServerMessage.AuthAck original = new ServerMessage.AuthAck("ACC-1");

        ServerMessage decoded = OrderEntryCodec.decodeServerMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void authRejectRoundTrips() {
        ServerMessage.AuthReject original = new ServerMessage.AuthReject("unknown api key");

        ServerMessage decoded = OrderEntryCodec.decodeServerMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void acceptedAckWithNoExecutionsRoundTrips() {
        OrderSnapshot order = new OrderSnapshot(
                1, "client-1", "AAPL", Side.BUY, OrderType.LIMIT, OrderStatus.NEW, 100, 100,
                19_000, NOW, NOW, 0);
        OrderOutcome outcome = OrderOutcome.accepted("client-1", order, List.of());
        ServerMessage.Ack original = ServerMessage.Ack.from(99L, outcome);

        ServerMessage decoded = OrderEntryCodec.decodeServerMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void acceptedAckWithExecutionsRoundTripsAndOmitsCounterpartyIdentity() {
        OrderSnapshot order = new OrderSnapshot(
                1, "client-1", "AAPL", Side.BUY, OrderType.LIMIT, OrderStatus.FILLED, 100, 0,
                19_000, NOW, NOW.plusSeconds(1), 0);
        // orderId 1 is the taker here (makerOrderId=2), which the round-trip must preserve as
        // maker=false without ever encoding the counterparty's order id 2 at all.
        Execution execution = new Execution(
                555, "AAPL", 19_000, 100, 1, "client-1", 2, "client-2", 2, 1, 0, 0, NOW);
        OrderOutcome outcome = OrderOutcome.accepted("client-1", order, List.of(execution));
        ServerMessage.Ack original = ServerMessage.Ack.from(100L, outcome);

        ServerMessage.Ack decoded =
                (ServerMessage.Ack) OrderEntryCodec.decodeServerMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
        assertEquals(1, decoded.executions().size());
        assertEquals(555, decoded.executions().get(0).executionId());
        assertEquals(false, decoded.executions().get(0).maker());
    }

    @Test
    void rejectedAckRoundTrips() {
        OrderOutcome outcome = OrderOutcome.rejected(
                1, "client-1", RejectionSource.RISK, "MAX_ORDER_QUANTITY_EXCEEDED", "too big");
        ServerMessage.Ack original = ServerMessage.Ack.from(101L, outcome);

        ServerMessage decoded = OrderEntryCodec.decodeServerMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void executionPushRoundTrips() {
        ServerMessage.ExecutionPush original =
                new ServerMessage.ExecutionPush(10L, 777L, "MSFT", 42_000, 5, 3, true, NOW);

        ServerMessage decoded = OrderEntryCodec.decodeServerMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void errorRoundTrips() {
        ServerMessage.Error original = new ServerMessage.Error("frame too large");

        ServerMessage decoded = OrderEntryCodec.decodeServerMessage(OrderEntryCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void stringsLongerThan255BytesAreRejectedAtEncodeTime() {
        String tooLong = "x".repeat(256);
        ClientMessage.Auth message = new ClientMessage.Auth(tooLong);

        assertThrows(TcpProtocolException.class, () -> OrderEntryCodec.encode(message));
    }

    @Test
    void anUnrecognizedMessageTypeIsAProtocolException() {
        byte[] frame = {(byte) 123};

        assertThrows(TcpProtocolException.class, () -> OrderEntryCodec.decodeClientMessage(frame));
    }
}
