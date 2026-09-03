package com.tradingplatform.gateway.api;

import com.tradingplatform.domain.RejectionReason;
import com.tradingplatform.gateway.CorrelationId;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Maps a rejection onto an HTTP status.
 *
 * <p>The distinction that matters to a client is whether the request was malformed, whether it was
 * well-formed but not permitted, or whether it conflicts with the current state of the order. Risk
 * rejections are 422 rather than 400 because nothing about the request was wrong: the account was
 * simply not allowed to send it.
 */
final class RejectionResponses {

    private RejectionResponses() {}

    static ResponseEntity<ErrorResponse> toResponseEntity(OrderOutcome outcome) {
        OrderOutcome.Rejection rejection = outcome.rejection().orElseThrow();
        HttpStatus status = switch (rejection.source()) {
            case RISK -> HttpStatus.UNPROCESSABLE_ENTITY;
            case MATCHING_ENGINE -> matchingEngineStatus(rejection.reason());
        };
        return ResponseEntity.status(status)
                .body(ErrorResponse.forOrder(
                        rejection.source().name(),
                        rejection.reason(),
                        rejection.detail(),
                        outcome.orderId(),
                        CorrelationId.current()));
    }

    private static HttpStatus matchingEngineStatus(String reason) {
        RejectionReason parsed;
        try {
            parsed = RejectionReason.valueOf(reason);
        } catch (IllegalArgumentException unknown) {
            return HttpStatus.BAD_REQUEST;
        }
        return switch (parsed) {
            case ORDER_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case ORDER_NOT_ACTIVE, DUPLICATE_ORDER_ID, DUPLICATE_CLIENT_ORDER_ID ->
                    HttpStatus.CONFLICT;
            // The remaining reasons all describe content the gateway should have caught first.
            // Reaching them means gateway validation and engine validation have drifted apart.
            default -> HttpStatus.BAD_REQUEST;
        };
    }
}
