package com.tradingplatform.gateway;

import org.slf4j.MDC;

/**
 * One identifier that ties a client request to every log line it produces, including the lines
 * written after the request thread has moved on.
 */
public final class CorrelationId {
    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    private CorrelationId() {}

    /** Returns the current request's identifier, or "none" when called outside a request. */
    public static String current() {
        String correlationId = MDC.get(MDC_KEY);
        return correlationId == null ? "none" : correlationId;
    }
}
