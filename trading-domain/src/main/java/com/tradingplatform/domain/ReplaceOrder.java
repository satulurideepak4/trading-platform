package com.tradingplatform.domain;

import java.time.Instant;

/** newQuantity is the replacement's total quantity, including any quantity already executed. */
public record ReplaceOrder(long orderId, long newQuantity, long newPrice, Instant timestamp) {}
