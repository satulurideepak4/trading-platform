package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.auth.AuthenticatedClient;
import com.tradingplatform.gateway.auth.ClientAuthenticator;
import com.tradingplatform.gateway.ratelimit.RateLimitDecision;
import com.tradingplatform.gateway.ratelimit.RateLimitExceededException;
import com.tradingplatform.gateway.ratelimit.TokenBucketRateLimiter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Authenticates the caller and charges the request against its rate limit before any handler runs.
 *
 * <p>Rate limiting comes after authentication because the limit is per account, and it comes before
 * idempotency on purpose: a client retrying in a tight loop should be throttled rather than allowed
 * to hammer the gateway simply because the identifier is already known.
 */
public class ClientAuthenticationInterceptor implements HandlerInterceptor {
    static final String API_KEY_HEADER = "X-Api-Key";
    static final String CLIENT_ATTRIBUTE = "tradingClient";
    private static final String ACCOUNT_MDC_KEY = "accountId";

    private final ClientAuthenticator authenticator;
    private final TokenBucketRateLimiter rateLimiter;

    public ClientAuthenticationInterceptor(
            ClientAuthenticator authenticator, TokenBucketRateLimiter rateLimiter) {
        this.authenticator = authenticator;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public boolean preHandle(
            HttpServletRequest request, HttpServletResponse response, Object handler) {
        AuthenticatedClient client = authenticator.authenticate(request.getHeader(API_KEY_HEADER));
        request.setAttribute(CLIENT_ATTRIBUTE, client);
        MDC.put(ACCOUNT_MDC_KEY, client.accountId());

        RateLimitDecision decision = rateLimiter.tryAcquire(client.accountId());
        if (!decision.allowed()) {
            throw new RateLimitExceededException(client.accountId(), decision.retryAfterMillis());
        }
        return true;
    }

    @Override
    public void afterCompletion(
            HttpServletRequest request,
            HttpServletResponse response,
            Object handler,
            Exception failure) {
        MDC.remove(ACCOUNT_MDC_KEY);
    }
}
