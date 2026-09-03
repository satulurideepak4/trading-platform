package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.auth.AuthenticatedClient;
import com.tradingplatform.gateway.ingress.OrderIngressService;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import com.tradingplatform.gateway.ingress.RejectionSource;
import com.tradingplatform.gateway.protobuf.ProtobufOrderMapper;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.OrderAckProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.ReplaceOrderProto;
import com.tradingplatform.gateway.protobuf.generated.OrderEntryProto.SubmitOrderProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The Protobuf arm of Stage 10's networking comparison — same ingress pipeline, auth and risk
 * checks as {@link OrderController}'s JSON path (see {@code OrderIngressService}), a different wire
 * encoding. A separate path rather than content-negotiating {@code /orders}: the two paths' request
 * and response object graphs are necessarily different Java types ({@code SubmitOrderRequest}/
 * {@code OrderResponse} vs. generated {@code Message} subtypes), so there is no method body to
 * usefully share, and a separate path avoids any ambiguity in how Spring MVC would otherwise have to
 * pick a converter for one route serving two encodings.
 *
 * <p>Always returns one self-contained {@link OrderAckProto} — accepted and rejected outcomes are
 * both that same message type, never two different ones — see {@link ProtobufOrderMapper#toAck}.
 * Unlike the JSON path, HTTP status is always 200 here: the accepted/rejected distinction and every
 * detail a client needs is in the body, and this stage is about comparing encoding cost, not
 * re-litigating REST status-code conventions for a second, parallel API surface.
 */
@RestController
@RequestMapping("/protobuf/orders")
public class OrderProtobufController {
    private static final Logger LOG = LoggerFactory.getLogger(OrderProtobufController.class);

    private final OrderIngressService ingress;

    public OrderProtobufController(OrderIngressService ingress) {
        this.ingress = ingress;
    }

    @PostMapping
    public OrderAckProto submit(AuthenticatedClient client, @RequestBody SubmitOrderProto request) {
        OrderOutcome outcome = ingress.submit(
                client.accountId(), request.getClientOrderId(), ProtobufOrderMapper.toOrderContent(request));
        return ProtobufOrderMapper.toAck(outcome);
    }

    @DeleteMapping("/{orderId}")
    public OrderAckProto cancel(AuthenticatedClient client, @PathVariable long orderId) {
        return ProtobufOrderMapper.toAck(ingress.cancel(client.accountId(), orderId));
    }

    @PutMapping("/{orderId}")
    public OrderAckProto replace(
            AuthenticatedClient client,
            @PathVariable long orderId,
            @RequestBody ReplaceOrderProto request) {
        OrderOutcome outcome =
                ingress.replace(client.accountId(), orderId, request.getQuantity(), request.getPrice());
        return ProtobufOrderMapper.toAck(outcome);
    }

    /**
     * {@code GatewayExceptionHandler}'s {@code @RestControllerAdvice} handlers all return the JSON
     * {@code ErrorResponse} type — correct for {@code /orders}, wrong here: a client that negotiated
     * {@code application/x-protobuf} would get a 406 (no converter can serialize a JSON-only type as
     * protobuf) or a body in a different encoding than the one it asked for, either way breaking the
     * one guarantee this path exists to keep. An {@code @ExceptionHandler} declared directly on a
     * controller takes precedence over a {@code @RestControllerAdvice} for that controller's own
     * methods, which is what keeps every response from here — success or failure — the same
     * encoding. Mirrors {@code TcpConnection.handle()}'s equivalent catch-all for the same reason.
     */
    @ExceptionHandler(RuntimeException.class)
    public OrderAckProto onFailure(RuntimeException failure) {
        LOG.info("event=protobuf_command_failed reason=\"{}\"", failure.getMessage());
        OrderOutcome synthetic = OrderOutcome.rejected(
                0, "", RejectionSource.MATCHING_ENGINE, failure.getClass().getSimpleName(),
                String.valueOf(failure.getMessage()));
        return ProtobufOrderMapper.toAck(synthetic);
    }
}
