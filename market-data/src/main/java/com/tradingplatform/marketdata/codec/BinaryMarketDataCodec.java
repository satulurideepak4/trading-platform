package com.tradingplatform.marketdata.codec;

import com.tradingplatform.marketdata.event.MarketEventType;
import com.tradingplatform.marketdata.process.NormalizedUpdate;
import com.tradingplatform.marketdata.process.SequenceOutcome;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * A compact fixed-layout binary encoding, hand-rolled rather than generated. See ADR-014 for why:
 * no Protobuf toolchain exists in this build yet, and adding a code generator for one message type
 * is more machinery than the message needs. The wire shape below is exactly what {@link
 * NormalizedUpdate} needs and nothing more.
 *
 * <pre>
 * byte    symbolLength
 * byte[]  symbol (UTF-8, symbolLength bytes)
 * byte    type ordinal
 * byte    outcome ordinal
 * long    price
 * long    quantity
 * long    exchangeSequence
 * long    exchangeTimestamp (epoch millis)
 * long    processorSequence
 * long    receivedAt (epoch millis)
 * int     gapSize
 * </pre>
 */
public final class BinaryMarketDataCodec implements MarketDataCodec {
    private static final int FIXED_LENGTH = 1 + 1 + 1 + 8 + 8 + 8 + 8 + 8 + 8 + 4;

    @Override
    public byte[] encode(NormalizedUpdate update) {
        byte[] symbolBytes = update.symbol().getBytes(StandardCharsets.UTF_8);
        if (symbolBytes.length > 255) {
            throw new IllegalArgumentException("symbol too long for binary codec: " + update.symbol());
        }
        ByteBuffer buffer = ByteBuffer.allocate(FIXED_LENGTH + symbolBytes.length);
        buffer.put((byte) symbolBytes.length);
        buffer.put(symbolBytes);
        buffer.put((byte) update.type().ordinal());
        buffer.put((byte) update.outcome().ordinal());
        buffer.putLong(update.price());
        buffer.putLong(update.quantity());
        buffer.putLong(update.exchangeSequence());
        buffer.putLong(update.exchangeTimestamp().toEpochMilli());
        buffer.putLong(update.processorSequence());
        buffer.putLong(update.receivedAt().toEpochMilli());
        buffer.putInt(update.gapSize());
        return buffer.array();
    }

    @Override
    public NormalizedUpdate decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int symbolLength = Byte.toUnsignedInt(buffer.get());
        byte[] symbolBytes = new byte[symbolLength];
        buffer.get(symbolBytes);
        String symbol = new String(symbolBytes, StandardCharsets.UTF_8);
        MarketEventType type = MarketEventType.values()[buffer.get()];
        SequenceOutcome outcome = SequenceOutcome.values()[buffer.get()];
        long price = buffer.getLong();
        long quantity = buffer.getLong();
        long exchangeSequence = buffer.getLong();
        Instant exchangeTimestamp = Instant.ofEpochMilli(buffer.getLong());
        long processorSequence = buffer.getLong();
        Instant receivedAt = Instant.ofEpochMilli(buffer.getLong());
        int gapSize = buffer.getInt();
        return new NormalizedUpdate(
                symbol,
                type,
                price,
                quantity,
                exchangeSequence,
                exchangeTimestamp,
                processorSequence,
                receivedAt,
                outcome,
                gapSize);
    }

    @Override
    public boolean isText() {
        return false;
    }
}
