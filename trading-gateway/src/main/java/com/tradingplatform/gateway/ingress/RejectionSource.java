package com.tradingplatform.gateway.ingress;

/**
 * Which layer refused the command. Clients need this to decide what to do next: a RISK rejection
 * will keep failing until the account changes, while BACKPRESSURE is worth retrying shortly.
 */
public enum RejectionSource {
    RISK,
    MATCHING_ENGINE
}
