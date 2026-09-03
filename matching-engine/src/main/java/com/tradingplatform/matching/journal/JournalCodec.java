package com.tradingplatform.matching.journal;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * The journal's on-disk record shape, shared by {@link FileCommandJournal} (write) and {@link
 * JournalReader} (read) so the two can never drift apart.
 *
 * <p>Hand-rolled fixed layout, the same choice and for the same reasons as
 * {@code market-data}'s {@code BinaryMarketDataCodec} (see ADR-014 there, ADR-015 here): three flat
 * command records, no schema-evolution need, no reason to add a dependency this module doesn't
 * otherwise have.
 *
 * <pre>
 * byte    type          (0 = SUBMIT, 1 = CANCEL, 2 = REPLACE)
 * long    journalSequence
 * int     workerId
 * long    orderId
 * long    timestamp epoch second
 * int     timestamp nano-of-second (full precision, not truncated to millis)
 * -- SUBMIT only --
 * byte    clientOrderId length, then that many UTF-8 bytes
 * byte    symbol length, then that many UTF-8 bytes
 * byte    side ordinal
 * byte    order type ordinal
 * long    quantity
 * long    price
 * -- REPLACE only --
 * long    newQuantity
 * long    newPrice
 * </pre>
 *
 * Callers frame each encoded record with a 4-byte length prefix; see {@link FileCommandJournal} and
 * {@link JournalReader}. That framing, not anything in this class, is what makes a torn trailing
 * write detectable.
 */
final class JournalCodec {
    private static final byte TYPE_SUBMIT = 0;
    private static final byte TYPE_CANCEL = 1;
    private static final byte TYPE_REPLACE = 2;

    // type + journalSequence + workerId + orderId + epochSecond + nano.
    private static final int HEADER_LENGTH = Byte.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES
            + Long.BYTES + Integer.BYTES;

    private JournalCodec() {}

    static byte[] encode(JournalRecord record) {
        return switch (record) {
            case JournalRecord.Submit submit -> encodeSubmit(submit);
            case JournalRecord.Cancel cancel -> encodeCancel(cancel);
            case JournalRecord.Replace replace -> encodeReplace(replace);
        };
    }

    private static byte[] encodeSubmit(JournalRecord.Submit journaled) {
        SubmitOrder command = journaled.command();
        byte[] clientOrderIdBytes = utf8(command.clientOrderId());
        byte[] symbolBytes = utf8(command.symbol());
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH
                + 1 + clientOrderIdBytes.length + 1 + symbolBytes.length + 1 + 1 + Long.BYTES + Long.BYTES);
        buffer.put(TYPE_SUBMIT);
        putHeader(buffer, journaled.journalSequence(), journaled.workerId(), command.orderId(), command.timestamp());
        putLengthPrefixed(buffer, clientOrderIdBytes);
        putLengthPrefixed(buffer, symbolBytes);
        buffer.put((byte) command.side().ordinal());
        buffer.put((byte) command.type().ordinal());
        buffer.putLong(command.quantity());
        buffer.putLong(command.price());
        return buffer.array();
    }

    private static byte[] encodeCancel(JournalRecord.Cancel journaled) {
        CancelOrder command = journaled.command();
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH);
        buffer.put(TYPE_CANCEL);
        putHeader(buffer, journaled.journalSequence(), journaled.workerId(), command.orderId(), command.timestamp());
        return buffer.array();
    }

    private static byte[] encodeReplace(JournalRecord.Replace journaled) {
        ReplaceOrder command = journaled.command();
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH + Long.BYTES + Long.BYTES);
        buffer.put(TYPE_REPLACE);
        putHeader(buffer, journaled.journalSequence(), journaled.workerId(), command.orderId(), command.timestamp());
        buffer.putLong(command.newQuantity());
        buffer.putLong(command.newPrice());
        return buffer.array();
    }

    static JournalRecord decode(byte[] bytes) {
        // A structurally-intact record (the length prefix framing it was correct, so this is not
        // the torn-trailing-write case JournalReader already tells apart) can still have corrupted
        // field bytes: a bad enum ordinal, a length prefix that overruns the record. Every one of
        // those throws some raw JDK exception (ArrayIndexOutOfBoundsException,
        // BufferUnderflowException...) from deep inside this method rather than the
        // JournalCorruptionException every caller is documented to expect from a corrupt journal -
        // caught and rethrown as one here rather than leaking the JDK exception type.
        try {
            return decodeFields(bytes);
        } catch (JournalCorruptionException alreadyCorrect) {
            throw alreadyCorrect;
        } catch (RuntimeException malformed) {
            throw new JournalCorruptionException(
                    "journal record failed to decode (" + bytes.length + " bytes)", malformed);
        }
    }

    private static JournalRecord decodeFields(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte type = buffer.get();
        long journalSequence = buffer.getLong();
        int workerId = buffer.getInt();
        long orderId = buffer.getLong();
        Instant timestamp = Instant.ofEpochSecond(buffer.getLong(), buffer.getInt());
        return switch (type) {
            case TYPE_SUBMIT -> {
                String clientOrderId = getLengthPrefixed(buffer);
                String symbol = getLengthPrefixed(buffer);
                Side side = Side.values()[buffer.get()];
                OrderType orderType = OrderType.values()[buffer.get()];
                long quantity = buffer.getLong();
                long price = buffer.getLong();
                yield new JournalRecord.Submit(
                        journalSequence,
                        workerId,
                        new SubmitOrder(
                                orderId, clientOrderId, symbol, side, orderType, quantity, price, timestamp));
            }
            case TYPE_CANCEL -> new JournalRecord.Cancel(
                    journalSequence, workerId, new CancelOrder(orderId, timestamp));
            case TYPE_REPLACE -> {
                long newQuantity = buffer.getLong();
                long newPrice = buffer.getLong();
                yield new JournalRecord.Replace(
                        journalSequence, workerId, new ReplaceOrder(orderId, newQuantity, newPrice, timestamp));
            }
            default -> throw new JournalCorruptionException("unknown journal record type " + type);
        };
    }

    private static void putHeader(
            ByteBuffer buffer, long journalSequence, int workerId, long orderId, Instant timestamp) {
        buffer.putLong(journalSequence);
        buffer.putInt(workerId);
        buffer.putLong(orderId);
        buffer.putLong(timestamp.getEpochSecond());
        buffer.putInt(timestamp.getNano());
    }

    private static void putLengthPrefixed(ByteBuffer buffer, byte[] bytes) {
        buffer.put((byte) bytes.length);
        buffer.put(bytes);
    }

    private static String getLengthPrefixed(ByteBuffer buffer) {
        int length = Byte.toUnsignedInt(buffer.get());
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 255) {
            throw new IllegalArgumentException("value too long to journal: " + value);
        }
        return bytes;
    }
}
