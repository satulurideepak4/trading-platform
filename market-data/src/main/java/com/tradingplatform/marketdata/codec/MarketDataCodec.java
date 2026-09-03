package com.tradingplatform.marketdata.codec;

import com.tradingplatform.marketdata.process.NormalizedUpdate;

/**
 * Wire format for a {@link NormalizedUpdate}, chosen independently of how it is transported.
 *
 * <p>Two implementations exist today: {@link JsonMarketDataCodec}, human-readable and the default,
 * and {@link BinaryMarketDataCodec}, a compact fixed-layout encoding. Selecting between them is one
 * configuration property; nothing else in the processing or publishing path knows which is in use.
 * See ADR-014 for why binary here is hand-rolled rather than Protobuf, and what would change if
 * that decision were revisited.
 */
public interface MarketDataCodec {
    byte[] encode(NormalizedUpdate update);

    NormalizedUpdate decode(byte[] bytes);

    /** Whether {@link #encode} produces text safe to send as a WebSocket text frame. */
    boolean isText();
}
