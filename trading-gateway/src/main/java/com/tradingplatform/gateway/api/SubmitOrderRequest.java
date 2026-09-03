package com.tradingplatform.gateway.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderContent;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

/**
 * @param clientOrderId the client's own identifier for this order; repeating it is what makes a
 *     retry safe. Bounded to 32 characters so that the account-qualified form the matching engine
 *     stores stays within its 64-character field.
 * @param price integer ticks. Must be omitted or zero for market orders, which have no price.
 * @param strategyId optional attribution for position and P&L reporting; defaults to DEFAULT
 */
public record SubmitOrderRequest(
        @NotBlank @Size(max = 32) @Pattern(regexp = "[A-Za-z0-9._-]+") String clientOrderId,
        @NotBlank @Size(max = 16) @Pattern(regexp = "[A-Z0-9][A-Z0-9._-]*") String symbol,
        @NotNull Side side,
        @NotNull OrderType type,
        @Positive long quantity,
        @PositiveOrZero long price,
        @Size(max = 32) @Pattern(regexp = "[A-Za-z0-9._-]*") String strategyId) {

    /** Clients that do not separate books still get one, so every position has an attribution. */
    public static final String DEFAULT_STRATEGY = "DEFAULT";

    // JsonIgnore because Jackson's default bean introspection otherwise treats this isXxx()
    // Bean Validation method as a serializable "priceConsistentWithType" property - invisible as
    // long as nothing ever serializes SubmitOrderRequest outbound (Spring only ever deserializes a
    // client's JSON into it), which is exactly why this went unnoticed until Stage 10's benchmark
    // client became the first thing in this codebase to serialize one: the server then rejected
    // its own DTO's JSON as unparseable, since @JsonIgnore was missing on the client side it needed
    // to be present here instead, once and for all, rather than worked around per caller.
    @JsonIgnore
    @AssertTrue(message = "limit orders require a positive price and market orders must not carry one")
    public boolean isPriceConsistentWithType() {
        if (type == null) {
            return true; // @NotNull already reports this.
        }
        return type == OrderType.LIMIT ? price > 0 : price == 0;
    }

    public OrderContent toOrderContent() {
        return new OrderContent(symbol, side, type, quantity, price, resolvedStrategyId());
    }

    private String resolvedStrategyId() {
        return strategyId == null || strategyId.isBlank() ? DEFAULT_STRATEGY : strategyId;
    }
}
