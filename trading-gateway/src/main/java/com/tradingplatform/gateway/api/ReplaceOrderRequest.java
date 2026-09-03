package com.tradingplatform.gateway.api;

import jakarta.validation.constraints.Positive;

/**
 * @param quantity the replacement's total quantity, including anything already executed
 */
public record ReplaceOrderRequest(@Positive long quantity, @Positive long price) {}
