package com.tradingplatform.gateway.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderAckProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderStatusProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderTypeProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.ReplaceOrderProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SideProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SubmitOrderProto;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * Mirrors {@link OrderApiTest}'s coverage of the JSON path, over the Protobuf one — same ingress
 * pipeline, same accounts/instruments (the "test" profile), a different {@code Content-Type}.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OrderProtobufControllerTest {
    private static final MediaType PROTOBUF = MediaType.parseMediaType("application/x-protobuf");
    private static final String API_KEY = "key-one";
    private static final AtomicInteger CLIENT_ORDER_IDS = new AtomicInteger();

    @Autowired private MockMvc mockMvc;

    @Test
    void acceptsALimitOrderAndReturnsItsAllocatedIdentity() throws Exception {
        SubmitOrderProto request = SubmitOrderProto.newBuilder()
                .setClientOrderId(nextClientOrderId())
                .setSymbol("AAPL")
                .setSide(SideProto.BUY)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(10)
                .setPrice(18_000)
                .build();

        OrderAckProto ack = submit(request);

        assertTrue(ack.getAccepted());
        assertTrue(ack.getOrderId() > 0);
        assertEquals(OrderStatusProto.NEW, ack.getStatus());
        assertEquals(10, ack.getRemainingQuantity());
        assertFalse(ack.getDuplicate());
    }

    @Test
    void matchesAgainstRestingLiquidityAndReportsTheFill() throws Exception {
        String symbol = "AAPL";
        OrderAckProto resting = submit(SubmitOrderProto.newBuilder()
                .setClientOrderId(nextClientOrderId())
                .setSymbol(symbol)
                .setSide(SideProto.SELL)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(6)
                .setPrice(18_100)
                .build());
        assertTrue(resting.getAccepted());

        OrderAckProto taker = submit(SubmitOrderProto.newBuilder()
                .setClientOrderId(nextClientOrderId())
                .setSymbol(symbol)
                .setSide(SideProto.BUY)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(6)
                .setPrice(18_100)
                .build());

        assertTrue(taker.getAccepted());
        assertEquals(OrderStatusProto.FILLED, taker.getStatus());
        assertEquals(1, taker.getExecutionsCount());
        assertEquals("TAKER", taker.getExecutions(0).getLiquidity());
    }

    @Test
    void rejectsOrdersAboveTheRiskLimitWithoutCreatingThem() throws Exception {
        OrderAckProto ack = submit(SubmitOrderProto.newBuilder()
                .setClientOrderId(nextClientOrderId())
                .setSymbol("AAPL")
                .setSide(SideProto.BUY)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(100_000) // above the "test" profile's max-order-quantity of 1000
                .setPrice(18_000)
                .build());

        assertFalse(ack.getAccepted());
        assertEquals("RISK", ack.getRejection().getSource());
        // A risk rejection still carries the orderId the gateway allocated before running the risk
        // check - the same behavior OrderResponse/ErrorResponse.forOrder already has on the JSON
        // path, kept consistent here rather than asserted away.
        assertTrue(ack.getOrderId() > 0);
    }

    @Test
    void cancelsAnOrderJustSubmitted() throws Exception {
        OrderAckProto submitAck = submit(SubmitOrderProto.newBuilder()
                .setClientOrderId(nextClientOrderId())
                .setSymbol("AAPL")
                .setSide(SideProto.SELL)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(4)
                .setPrice(18_500)
                .build());

        MvcResult result = mockMvc.perform(authenticated(delete("/protobuf/orders/" + submitAck.getOrderId())))
                .andExpect(status().isOk())
                .andReturn();
        OrderAckProto cancelAck = OrderAckProto.parseFrom(result.getResponse().getContentAsByteArray());

        assertTrue(cancelAck.getAccepted());
        assertEquals(OrderStatusProto.CANCELLED, cancelAck.getStatus());
    }

    @Test
    void replacesAnOrderJustSubmitted() throws Exception {
        OrderAckProto submitAck = submit(SubmitOrderProto.newBuilder()
                .setClientOrderId(nextClientOrderId())
                .setSymbol("AAPL")
                .setSide(SideProto.SELL)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(4)
                .setPrice(18_500)
                .build());
        ReplaceOrderProto replace = ReplaceOrderProto.newBuilder().setQuantity(2).setPrice(18_600).build();

        MvcResult result = mockMvc.perform(
                        authenticated(put("/protobuf/orders/" + submitAck.getOrderId()))
                                .contentType(PROTOBUF)
                                .content(replace.toByteArray()))
                .andExpect(status().isOk())
                .andReturn();
        OrderAckProto replaceAck = OrderAckProto.parseFrom(result.getResponse().getContentAsByteArray());

        assertTrue(replaceAck.getAccepted());
        assertEquals(2, replaceAck.getQuantity());
        assertEquals(18_600, replaceAck.getPrice());
    }

    /**
     * A real bug this exact scenario caught: {@code ProtobufOrderMapper} once passed proto3's
     * empty-string default straight through as {@code strategyId} instead of resolving it to
     * {@code DEFAULT} the way {@link SubmitOrderRequest#toOrderContent()} already does for JSON —
     * so the identical logical request (same clientOrderId, no strategy given) produced two
     * different {@code OrderContent} values depending only on which encoding submitted it first,
     * and resubmitting on the other encoding was rejected as {@code
     * ClientOrderIdConflictException} instead of recognised as the same request.
     */
    @Test
    void omittingStrategyIdResolvesToTheSameDefaultOnBothEncodings() throws Exception {
        String clientOrderId = nextClientOrderId();
        String jsonBody = """
                {"clientOrderId":"%s","symbol":"AAPL","side":"BUY","type":"LIMIT","quantity":10,"price":18000}
                """.formatted(clientOrderId);
        mockMvc.perform(post("/orders")
                        .header("X-Api-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonBody))
                .andExpect(status().isCreated());

        // No .setStrategyId(...) call at all - proto3's own unset-field default, mirroring what a
        // real Protobuf client that never learned about strategies would send.
        SubmitOrderProto sameRequestAgainViaProtobuf = SubmitOrderProto.newBuilder()
                .setClientOrderId(clientOrderId)
                .setSymbol("AAPL")
                .setSide(SideProto.BUY)
                .setType(OrderTypeProto.LIMIT)
                .setQuantity(10)
                .setPrice(18_000)
                .build();

        OrderAckProto ack = submit(sameRequestAgainViaProtobuf);

        assertTrue(ack.getAccepted(), () -> "expected a clean idempotent replay, got: " + ack);
        assertTrue(ack.getDuplicate());
    }

    @Test
    void anOrderNotFoundExceptionStillComesBackAsProtobufNotJson() throws Exception {
        MvcResult result = mockMvc.perform(authenticated(delete("/protobuf/orders/999999999")))
                .andExpect(status().isOk())
                .andReturn();

        assertTrue(result.getResponse().getContentType().contains("protobuf"));
        OrderAckProto ack = OrderAckProto.parseFrom(result.getResponse().getContentAsByteArray());
        assertFalse(ack.getAccepted());
        assertEquals("OrderNotFoundException", ack.getRejection().getReason());
    }

    private OrderAckProto submit(SubmitOrderProto request) throws Exception {
        MvcResult result = mockMvc.perform(
                        authenticated(post("/protobuf/orders"))
                                .contentType(PROTOBUF)
                                .content(request.toByteArray()))
                .andExpect(status().isOk())
                .andReturn();
        return OrderAckProto.parseFrom(result.getResponse().getContentAsByteArray());
    }

    private static MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder builder) {
        return builder.header("X-Api-Key", API_KEY).accept(PROTOBUF);
    }

    private static String nextClientOrderId() {
        return "protobuf-" + CLIENT_ORDER_IDS.incrementAndGet();
    }
}
