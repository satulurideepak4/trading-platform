package com.tradingplatform.marketdata.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingplatform.marketdata.process.NormalizedUpdate;

/** The default codec: readable on the wire, and decodable by any client with a JSON parser. */
public final class JsonMarketDataCodec implements MarketDataCodec {
    private final ObjectMapper mapper;

    public JsonMarketDataCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] encode(NormalizedUpdate update) {
        try {
            return mapper.writeValueAsBytes(update);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException("could not encode " + update, e);
        }
    }

    @Override
    public NormalizedUpdate decode(byte[] bytes) {
        try {
            return mapper.readValue(bytes, NormalizedUpdate.class);
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("could not decode market data payload", e);
        }
    }

    @Override
    public boolean isText() {
        return true;
    }
}
