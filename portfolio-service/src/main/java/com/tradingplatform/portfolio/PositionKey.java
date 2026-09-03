package com.tradingplatform.portfolio;

import java.util.Objects;

/**
 * Positions are held per account, strategy and instrument.
 *
 * <p>The symbol being part of the key matters more than it looks: Kafka partitions the execution
 * stream by symbol, so every execution touching a given position key arrives on one partition and
 * is therefore handled by one consumer thread. Two threads cannot contend for the same position
 * row through the normal path.
 */
public record PositionKey(String accountId, String strategyId, String symbol) {

    public PositionKey {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(strategyId, "strategyId");
        Objects.requireNonNull(symbol, "symbol");
    }
}
