package com.tradingplatform.marketdata.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradingplatform.marketdata.event.MarketEventType;
import com.tradingplatform.marketdata.process.NormalizedUpdate;
import com.tradingplatform.marketdata.process.SequenceOutcome;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Both codecs must agree with each other on meaning even though their bytes differ completely —
 * that is the whole point of {@link MarketDataCodec} being pluggable. See ADR-014.
 */
class MarketDataCodecTest {
    private static final NormalizedUpdate SAMPLE = new NormalizedUpdate(
            "AAPL",
            MarketEventType.BID,
            19_050,
            125,
            42,
            Instant.parse("2025-01-01T09:30:00.123Z"),
            9_001,
            Instant.parse("2025-01-01T09:30:00.456Z"),
            SequenceOutcome.GAP,
            3);

    static List<MarketDataCodec> codecs() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return List.of(new JsonMarketDataCodec(mapper), new BinaryMarketDataCodec());
    }

    @ParameterizedTest
    @MethodSource("codecs")
    void decodeReversesEncodeExactly(MarketDataCodec codec) {
        byte[] encoded = codec.encode(SAMPLE);
        NormalizedUpdate decoded = codec.decode(encoded);

        assertEquals(SAMPLE, decoded);
    }

    @Test
    void jsonCodecIsText() {
        assertTrue(codecs().get(0).isText());
    }

    @Test
    void binaryCodecIsNotText() {
        assertFalse(codecs().get(1).isText());
    }

    @Test
    void binaryEncodingIsSmallerThanJsonForTheSameUpdate() {
        MarketDataCodec json = codecs().get(0);
        MarketDataCodec binary = codecs().get(1);

        assertTrue(binary.encode(SAMPLE).length < json.encode(SAMPLE).length);
    }
}
