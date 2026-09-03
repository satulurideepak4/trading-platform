package com.tradingplatform.gateway.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * @param source which layer refused the request, so a client can tell a permanent rejection from
 *     something worth retrying
 * @param orderId present whenever an order id was already allocated, so a rejected submission can
 *     still be reconciled
 * @param details field-level messages for validation failures
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
        String source,
        String reason,
        String message,
        Long orderId,
        String correlationId,
        List<String> details) {

    public static ErrorResponse of(
            String source, String reason, String message, String correlationId) {
        return new ErrorResponse(source, reason, message, null, correlationId, null);
    }

    public static ErrorResponse forOrder(
            String source, String reason, String message, long orderId, String correlationId) {
        return new ErrorResponse(source, reason, message, orderId, correlationId, null);
    }

    public static ErrorResponse validation(
            String message, List<String> details, String correlationId) {
        return new ErrorResponse(
                "VALIDATION", "INVALID_REQUEST", message, null, correlationId, details);
    }
}
