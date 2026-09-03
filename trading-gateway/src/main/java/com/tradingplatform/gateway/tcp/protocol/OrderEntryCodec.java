package com.tradingplatform.gateway.tcp.protocol;

import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.ingress.RejectionSource;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage.Ack.AcceptedOrder;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage.Ack.AckExecution;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage.Ack.Rejection;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The order-entry wire format: one {@code byte} {@link MessageType} tag, then a fixed layout of
 * fields specific to that type. Framed by {@link FrameIO}, which is what makes a torn read
 * detectable and recoverable in the first place — nothing here assumes a payload arrived whole.
 *
 * <pre>
 * AUTH           string  apiKey
 * AUTH_ACK       string  accountId
 * AUTH_REJECT    string  reason
 * SUBMIT         long    requestId
 *                string  clientOrderId
 *                string  symbol
 *                byte    side           (WireCodes)
 *                byte    orderType      (WireCodes)
 *                long    quantity
 *                long    price
 *                string  strategyId     (empty string means "use the account default")
 * CANCEL         long    requestId
 *                long    orderId
 * REPLACE        long    requestId
 *                long    orderId
 *                long    quantity
 *                long    price
 * ACK            long    requestId
 *                long    orderId
 *                string  clientOrderId
 *                byte    duplicate      (0/1)
 *                byte    accepted       (0/1)
 *                -- accepted only --
 *                string  symbol
 *                byte    side           (WireCodes)
 *                byte    orderType      (WireCodes)
 *                byte    status         (WireCodes)
 *                long    quantity
 *                long    remainingQuantity
 *                long    price
 *                long    createdAt epoch second
 *                int     createdAt nano-of-second
 *                long    updatedAt epoch second
 *                int     updatedAt nano-of-second
 *                int     executionCount
 *                          [executionCount times]:
 *                          long executionId, long price, long quantity,
 *                          byte liquidity (WireCodes), long timestamp epoch second, int nano
 *                -- rejected only --
 *                byte    rejectionSource (WireCodes)
 *                string  reason
 *                string  detail
 * EXECUTION_PUSH long    orderId
 *                long    executionId
 *                string  symbol
 *                long    price
 *                long    quantity
 *                long    remainingQuantity
 *                byte    liquidity (WireCodes)
 *                long    timestamp epoch second
 *                int     timestamp nano-of-second
 * ERROR          string  message
 * </pre>
 *
 * Strings are one length byte (0-255, unsigned) followed by that many UTF-8 bytes — the same
 * convention {@code matching-engine}'s {@code JournalCodec} uses, for the same reason: no field
 * this protocol carries is ever long enough to need more.
 *
 * <p>Neither an {@code ACK}'s executions nor an {@code EXECUTION_PUSH} carry the counterparty's
 * order id, client order id or account — only this account's own side. See {@link ServerMessage}'s
 * class Javadoc.
 */
public final class OrderEntryCodec {
    private static final int MAX_STRING_BYTES = 255;

    private OrderEntryCodec() {}

    // ---- client -> server ----

    public static byte[] encode(ClientMessage message) {
        return withPayload(payload -> {
            switch (message) {
                case ClientMessage.Auth auth -> {
                    payload.writeByte(MessageType.AUTH);
                    writeString(payload, auth.apiKey());
                }
                case ClientMessage.Submit submit -> {
                    payload.writeByte(MessageType.SUBMIT);
                    payload.writeLong(submit.requestId());
                    writeString(payload, submit.clientOrderId());
                    writeString(payload, submit.symbol());
                    payload.writeByte(WireCodes.side(submit.side()));
                    payload.writeByte(WireCodes.orderType(submit.type()));
                    payload.writeLong(submit.quantity());
                    payload.writeLong(submit.price());
                    writeString(payload, submit.strategyId() == null ? "" : submit.strategyId());
                }
                case ClientMessage.Cancel cancel -> {
                    payload.writeByte(MessageType.CANCEL);
                    payload.writeLong(cancel.requestId());
                    payload.writeLong(cancel.orderId());
                }
                case ClientMessage.Replace replace -> {
                    payload.writeByte(MessageType.REPLACE);
                    payload.writeLong(replace.requestId());
                    payload.writeLong(replace.orderId());
                    payload.writeLong(replace.quantity());
                    payload.writeLong(replace.price());
                }
            }
        });
    }

    public static ClientMessage decodeClientMessage(byte[] frame) {
        DataInputStream in = reader(frame);
        try {
            byte type = in.readByte();
            return switch (type) {
                case MessageType.AUTH -> new ClientMessage.Auth(readString(in));
                case MessageType.SUBMIT -> new ClientMessage.Submit(
                        in.readLong(),
                        readString(in),
                        readString(in),
                        WireCodes.side(in.readByte()),
                        WireCodes.orderType(in.readByte()),
                        in.readLong(),
                        in.readLong(),
                        readString(in));
                case MessageType.CANCEL -> new ClientMessage.Cancel(in.readLong(), in.readLong());
                case MessageType.REPLACE -> new ClientMessage.Replace(
                        in.readLong(), in.readLong(), in.readLong(), in.readLong());
                default -> throw new TcpProtocolException("unexpected client message type " + type);
            };
        } catch (IOException e) {
            throw new TcpProtocolException("malformed client frame", e);
        }
    }

    // ---- server -> client ----

    public static byte[] encode(ServerMessage message) {
        return withPayload(payload -> {
            switch (message) {
                case ServerMessage.AuthAck ack -> {
                    payload.writeByte(MessageType.AUTH_ACK);
                    writeString(payload, ack.accountId());
                }
                case ServerMessage.AuthReject reject -> {
                    payload.writeByte(MessageType.AUTH_REJECT);
                    writeString(payload, reject.reason());
                }
                case ServerMessage.Ack ack -> {
                    payload.writeByte(MessageType.ACK);
                    writeAck(payload, ack);
                }
                case ServerMessage.ExecutionPush push -> {
                    payload.writeByte(MessageType.EXECUTION_PUSH);
                    writeExecutionPush(payload, push);
                }
                case ServerMessage.Error error -> {
                    payload.writeByte(MessageType.ERROR);
                    writeString(payload, error.message());
                }
            }
        });
    }

    public static ServerMessage decodeServerMessage(byte[] frame) {
        DataInputStream in = reader(frame);
        try {
            byte type = in.readByte();
            return switch (type) {
                case MessageType.AUTH_ACK -> new ServerMessage.AuthAck(readString(in));
                case MessageType.AUTH_REJECT -> new ServerMessage.AuthReject(readString(in));
                case MessageType.ACK -> readAck(in);
                case MessageType.EXECUTION_PUSH -> readExecutionPush(in);
                case MessageType.ERROR -> new ServerMessage.Error(readString(in));
                default -> throw new TcpProtocolException("unexpected server message type " + type);
            };
        } catch (IOException e) {
            throw new TcpProtocolException("malformed server frame", e);
        }
    }

    private static void writeAck(DataOutputStream out, ServerMessage.Ack ack) throws IOException {
        out.writeLong(ack.requestId());
        out.writeLong(ack.orderId());
        writeString(out, ack.clientOrderId());
        out.writeByte(ack.duplicate() ? 1 : 0);
        out.writeByte(ack.accepted() ? 1 : 0);
        if (ack.accepted()) {
            AcceptedOrder order = ack.order().orElseThrow(
                    () -> new IllegalStateException("accepted acks always carry an order"));
            writeString(out, order.symbol());
            out.writeByte(WireCodes.side(order.side()));
            out.writeByte(WireCodes.orderType(order.type()));
            out.writeByte(WireCodes.orderStatus(order.status()));
            out.writeLong(order.quantity());
            out.writeLong(order.remainingQuantity());
            out.writeLong(order.price());
            writeInstant(out, order.createdAt());
            writeInstant(out, order.updatedAt());
            List<AckExecution> executions = ack.executions();
            out.writeInt(executions.size());
            for (AckExecution execution : executions) {
                out.writeLong(execution.executionId());
                out.writeLong(execution.price());
                out.writeLong(execution.quantity());
                out.writeByte(WireCodes.liquidity(execution.maker()));
                writeInstant(out, execution.timestamp());
            }
        } else {
            Rejection rejection = ack.rejection().orElseThrow(
                    () -> new IllegalStateException("rejected acks always carry a rejection"));
            out.writeByte(WireCodes.rejectionSource(rejection.source()));
            writeString(out, rejection.reason());
            writeString(out, rejection.detail());
        }
    }

    private static ServerMessage.Ack readAck(DataInputStream in) throws IOException {
        long requestId = in.readLong();
        long orderId = in.readLong();
        String clientOrderId = readString(in);
        boolean duplicate = in.readByte() != 0;
        boolean accepted = in.readByte() != 0;
        Optional<AcceptedOrder> order = Optional.empty();
        List<AckExecution> executions = List.of();
        Optional<Rejection> rejection = Optional.empty();
        if (accepted) {
            String symbol = readString(in);
            Side side = WireCodes.side(in.readByte());
            OrderType type = WireCodes.orderType(in.readByte());
            OrderStatus status = WireCodes.orderStatus(in.readByte());
            long quantity = in.readLong();
            long remainingQuantity = in.readLong();
            long price = in.readLong();
            Instant createdAt = readInstant(in);
            Instant updatedAt = readInstant(in);
            int executionCount = in.readInt();
            List<AckExecution> decoded = new ArrayList<>(executionCount);
            for (int i = 0; i < executionCount; i++) {
                long executionId = in.readLong();
                long executionPrice = in.readLong();
                long executionQuantity = in.readLong();
                boolean maker = WireCodes.liquidity(in.readByte());
                Instant timestamp = readInstant(in);
                decoded.add(new AckExecution(executionId, executionPrice, executionQuantity, maker, timestamp));
            }
            order = Optional.of(new AcceptedOrder(
                    symbol, side, type, status, quantity, remainingQuantity, price, createdAt, updatedAt));
            executions = decoded;
        } else {
            RejectionSource source = WireCodes.rejectionSource(in.readByte());
            String reason = readString(in);
            String detail = readString(in);
            rejection = Optional.of(new Rejection(source, reason, detail));
        }
        return new ServerMessage.Ack(
                requestId, orderId, clientOrderId, duplicate, accepted, order, executions, rejection);
    }

    private static void writeExecutionPush(DataOutputStream out, ServerMessage.ExecutionPush push)
            throws IOException {
        out.writeLong(push.orderId());
        out.writeLong(push.executionId());
        writeString(out, push.symbol());
        out.writeLong(push.price());
        out.writeLong(push.quantity());
        out.writeLong(push.remainingQuantity());
        out.writeByte(WireCodes.liquidity(push.maker()));
        writeInstant(out, push.timestamp());
    }

    private static ServerMessage.ExecutionPush readExecutionPush(DataInputStream in) throws IOException {
        long orderId = in.readLong();
        long executionId = in.readLong();
        String symbol = readString(in);
        long price = in.readLong();
        long quantity = in.readLong();
        long remainingQuantity = in.readLong();
        boolean maker = WireCodes.liquidity(in.readByte());
        Instant timestamp = readInstant(in);
        return new ServerMessage.ExecutionPush(
                orderId, executionId, symbol, price, quantity, remainingQuantity, maker, timestamp);
    }

    private static void writeInstant(DataOutputStream out, Instant instant) throws IOException {
        out.writeLong(instant.getEpochSecond());
        out.writeInt(instant.getNano());
    }

    private static Instant readInstant(DataInputStream in) throws IOException {
        return Instant.ofEpochSecond(in.readLong(), in.readInt());
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new TcpProtocolException(
                    "value too long to encode (" + bytes.length + " bytes): " + value);
        }
        out.writeByte(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readUnsignedByte();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static DataInputStream reader(byte[] frame) {
        return new DataInputStream(new java.io.ByteArrayInputStream(frame));
    }

    private static byte[] withPayload(IOConsumer<DataOutputStream> writer) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            writer.accept(out);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode a frame in-memory - unreachable", e);
        }
        return buffer.toByteArray();
    }

    @FunctionalInterface
    private interface IOConsumer<T> {
        void accept(T value) throws IOException;
    }
}
