package com.tradingplatform.gateway.api;

import jakarta.validation.constraints.Positive;

public record MarkPriceRequest(@Positive long price) {}
