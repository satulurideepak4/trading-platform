package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.CorrelationId;
import com.tradingplatform.gateway.auth.UnauthenticatedClientException;
import com.tradingplatform.gateway.ingress.ClientOrderIdConflictException;
import com.tradingplatform.gateway.ingress.InstrumentNotFoundException;
import com.tradingplatform.gateway.ingress.MatchingTimeoutException;
import com.tradingplatform.gateway.ingress.OrderNotFoundException;
import com.tradingplatform.gateway.marketdata.SymbolNotTrackedException;
import com.tradingplatform.gateway.ratelimit.RateLimitExceededException;
import com.tradingplatform.matching.routing.OrderRoutingRejectedException;
import jakarta.validation.ConstraintViolationException;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GatewayExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> onInvalidBody(MethodArgumentNotValidException failure) {
        List<String> details = failure.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .toList();
        List<String> objectErrors = failure.getBindingResult().getGlobalErrors().stream()
                .map(error -> error.getDefaultMessage())
                .toList();
        return ResponseEntity.badRequest()
                .body(ErrorResponse.validation(
                        "the request body failed validation",
                        concat(details, objectErrors),
                        CorrelationId.current()));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> onInvalidParameter(ConstraintViolationException failure) {
        List<String> details = failure.getConstraintViolations().stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .toList();
        return ResponseEntity.badRequest()
                .body(ErrorResponse.validation(
                        "a request parameter failed validation", details, CorrelationId.current()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> onUnreadableBody(HttpMessageNotReadableException failure) {
        // The parser message can quote the payload, so it is not echoed back to the caller.
        LOG.debug("event=unreadable_request_body", failure);
        return ResponseEntity.badRequest()
                .body(ErrorResponse.of(
                        "VALIDATION",
                        "MALFORMED_REQUEST",
                        "the request body could not be parsed",
                        CorrelationId.current()));
    }

    @ExceptionHandler(UnauthenticatedClientException.class)
    public ResponseEntity<ErrorResponse> onUnauthenticated(
            UnauthenticatedClientException failure) {
        LOG.info("event=authentication_failed reason=\"{}\"", failure.getMessage());
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(ErrorResponse.of(
                        "AUTHENTICATION",
                        "UNAUTHENTICATED",
                        failure.getMessage(),
                        CorrelationId.current()));
    }

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ErrorResponse> onRateLimited(RateLimitExceededException failure) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, retryAfterSeconds(failure.retryAfterMillis()))
                .body(ErrorResponse.of(
                        "RATE_LIMIT",
                        "RATE_LIMIT_EXCEEDED",
                        failure.getMessage(),
                        CorrelationId.current()));
    }

    @ExceptionHandler(ClientOrderIdConflictException.class)
    public ResponseEntity<ErrorResponse> onClientOrderIdConflict(
            ClientOrderIdConflictException failure) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ErrorResponse.forOrder(
                        "IDEMPOTENCY",
                        "CLIENT_ORDER_ID_REUSED",
                        failure.getMessage(),
                        failure.existingOrderId(),
                        CorrelationId.current()));
    }

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> onOrderNotFound(OrderNotFoundException failure) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.of(
                        "GATEWAY", "ORDER_NOT_FOUND", failure.getMessage(),
                        CorrelationId.current()));
    }

    @ExceptionHandler(InstrumentNotFoundException.class)
    public ResponseEntity<ErrorResponse> onInstrumentNotFound(
            InstrumentNotFoundException failure) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.of(
                        "GATEWAY", "INSTRUMENT_NOT_FOUND", failure.getMessage(),
                        CorrelationId.current()));
    }

    @ExceptionHandler(SymbolNotTrackedException.class)
    public ResponseEntity<ErrorResponse> onSymbolNotTracked(SymbolNotTrackedException failure) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.of(
                        "MARKET_DATA", "SYMBOL_NOT_TRACKED", failure.getMessage(),
                        CorrelationId.current()));
    }

    /**
     * Queue saturation is backpressure, not an error: the command was never admitted, so the client
     * can safely resend the same clientOrderId once the queue drains.
     */
    @ExceptionHandler(OrderRoutingRejectedException.class)
    public ResponseEntity<ErrorResponse> onRoutingRejected(
            OrderRoutingRejectedException failure) {
        LOG.warn(
                "event=routing_rejected reason={} workerId={}",
                failure.reason(),
                failure.workerId());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(ErrorResponse.of(
                        "BACKPRESSURE",
                        failure.reason().name(),
                        failure.getMessage(),
                        CorrelationId.current()));
    }

    /**
     * A timeout says nothing about whether the command was applied, so the response deliberately
     * does not claim failure. Retrying the same clientOrderId is the safe action and will return
     * the real outcome.
     */
    @ExceptionHandler(MatchingTimeoutException.class)
    public ResponseEntity<ErrorResponse> onMatchingTimeout(MatchingTimeoutException failure) {
        LOG.error("event=matching_timeout orderId={}", failure.orderId());
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(ErrorResponse.forOrder(
                        "MATCHING_ENGINE",
                        "OUTCOME_UNKNOWN",
                        failure.getMessage() + "; retry the same clientOrderId to learn the outcome",
                        failure.orderId(),
                        CorrelationId.current()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> onUnexpectedFailure(Exception failure) {
        LOG.error("event=unhandled_gateway_failure", failure);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of(
                        "GATEWAY",
                        "INTERNAL_ERROR",
                        "the gateway failed to process the request",
                        CorrelationId.current()));
    }

    private static String retryAfterSeconds(long retryAfterMillis) {
        return Long.toString(Math.max(1, (retryAfterMillis + 999) / 1000));
    }

    private static List<String> concat(List<String> first, List<String> second) {
        return second.isEmpty() ? first : Stream.concat(first.stream(), second.stream()).toList();
    }
}
