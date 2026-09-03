package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.ingress.OrderIngressService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Positive;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Validated
public class OrderBookController {
    private final OrderIngressService ingress;

    public OrderBookController(OrderIngressService ingress) {
        this.ingress = ingress;
    }

    /**
     * The book is read through the same worker queue as trading commands, so the snapshot is taken
     * between two commands rather than across them. Depth is capped because the request occupies a
     * matching worker for as long as it takes to build the snapshot.
     */
    @GetMapping("/orderbook/{symbol}")
    public OrderBookResponse book(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "10") @Positive @Max(100) int depth) {
        return OrderBookResponse.from(ingress.book(symbol), depth);
    }
}
