package com.tradingplatform.benchmark.jmh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.api.SubmitOrderRequest;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderTypeProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SideProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SubmitOrderProto;
import com.tradingplatform.gateway.tcp.protocol.ClientMessage;
import com.tradingplatform.gateway.tcp.protocol.OrderEntryCodec;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Pure encode/decode cost for one representative SUBMIT order message, JSON vs. Protobuf vs. the
 * hand-rolled binary codec — the "serialization overhead" figure {@code
 * docs/networking-comparison.md} needs a real number for rather than an assertion. No network, no
 * matching engine, no gateway: just the same logical content run through each encoding, so the
 * difference measured here is exactly the encoding cost and nothing else.
 *
 * <p>The three representations are deliberately not identical Java objects — {@code
 * SubmitOrderRequest}, the generated {@code SubmitOrderProto}, and {@code ClientMessage.Submit} —
 * because that is exactly what each of the three order-entry paths actually sends; testing three
 * encodings of one shared type would measure something none of them do in production.
 *
 * <p>Run with {@code java -jar target/benchmarks.jar SerializationBenchmark}. See
 * docs/benchmark-methodology.md for the exact command and this environment's hardware/JDK.
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class SerializationBenchmark {
    private ObjectMapper jsonMapper;
    private SubmitOrderRequest jsonMessage;
    private byte[] jsonBytes;

    private SubmitOrderProto protobufMessage;
    private byte[] protobufBytes;

    private ClientMessage.Submit binaryMessage;
    private byte[] binaryBytes;

    @Setup(Level.Trial)
    public void setUp() {
        jsonMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        jsonMessage = new SubmitOrderRequest(
                "bench-1", "AAPL", Side.BUY, OrderType.LIMIT, 10, 19_000, "DEFAULT");
        jsonBytes = writeJson(jsonMessage);

        protobufMessage = SubmitOrderProto.newBuilder()
                .setClientOrderId("bench-1")
                .setSymbol("AAPL")
                .setSide(SideProto.BUY)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(10)
                .setPrice(19_000)
                .setStrategyId("DEFAULT")
                .build();
        protobufBytes = protobufMessage.toByteArray();

        binaryMessage = new ClientMessage.Submit(
                1L, "bench-1", "AAPL", Side.BUY, OrderType.LIMIT, 10, 19_000, "DEFAULT");
        binaryBytes = OrderEntryCodec.encode(binaryMessage);
    }

    @Benchmark
    public byte[] jsonEncode() {
        return writeJson(jsonMessage);
    }

    @Benchmark
    public SubmitOrderRequest jsonDecode() throws Exception {
        return jsonMapper.readValue(jsonBytes, SubmitOrderRequest.class);
    }

    @Benchmark
    public byte[] protobufEncode() {
        return protobufMessage.toByteArray();
    }

    @Benchmark
    public SubmitOrderProto protobufDecode() throws Exception {
        return SubmitOrderProto.parseFrom(protobufBytes);
    }

    @Benchmark
    public byte[] binaryEncode() {
        return OrderEntryCodec.encode(binaryMessage);
    }

    @Benchmark
    public ClientMessage binaryDecode() {
        return OrderEntryCodec.decodeClientMessage(binaryBytes);
    }

    private byte[] writeJson(SubmitOrderRequest message) {
        try {
            return jsonMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
