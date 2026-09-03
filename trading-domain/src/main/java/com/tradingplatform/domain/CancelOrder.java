package com.tradingplatform.domain;

import java.time.Instant;

public record CancelOrder(long orderId, Instant timestamp) {}
