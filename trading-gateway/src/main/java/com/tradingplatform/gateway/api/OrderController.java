package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.auth.AuthenticatedClient;
import com.tradingplatform.gateway.ingress.OrderIngressService;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderIngressService ingress;

    public OrderController(OrderIngressService ingress) {
        this.ingress = ingress;
    }

    /**
     * Returns 201 for an order this request created and 200 when it is replaying the outcome of an
     * earlier identical request, so a client can tell whether its retry was the one that traded.
     */
    @PostMapping
    public ResponseEntity<?> submit(
            AuthenticatedClient client, @Valid @RequestBody SubmitOrderRequest request) {
        OrderOutcome outcome = ingress.submit(
                client.accountId(), request.clientOrderId(), request.toOrderContent());
        if (!outcome.accepted()) {
            return RejectionResponses.toResponseEntity(outcome);
        }
        HttpStatus status = outcome.duplicate() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(OrderResponse.from(outcome));
    }

    @DeleteMapping("/{orderId}")
    public ResponseEntity<?> cancel(AuthenticatedClient client, @PathVariable long orderId) {
        return respond(ingress.cancel(client.accountId(), orderId));
    }

    @PutMapping("/{orderId}")
    public ResponseEntity<?> replace(
            AuthenticatedClient client,
            @PathVariable long orderId,
            @Valid @RequestBody ReplaceOrderRequest request) {
        return respond(
                ingress.replace(client.accountId(), orderId, request.quantity(), request.price()));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<?> find(AuthenticatedClient client, @PathVariable long orderId) {
        return respond(ingress.findOrder(client.accountId(), orderId));
    }

    private static ResponseEntity<?> respond(OrderOutcome outcome) {
        return outcome.accepted()
                ? ResponseEntity.ok(OrderResponse.from(outcome))
                : RejectionResponses.toResponseEntity(outcome);
    }
}
