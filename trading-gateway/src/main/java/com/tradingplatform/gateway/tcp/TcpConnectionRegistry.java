package com.tradingplatform.gateway.tcp;

import com.tradingplatform.gateway.tcp.protocol.ServerMessage;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Which authenticated TCP connections belong to which account, so an execution can be pushed to
 * whichever of them (there may be more than one, or none) currently represents that account.
 *
 * <p>Mirrors {@code market-data}'s {@code MarketDataHub}: a plain concurrent map from identity to
 * the connections interested in it, with a gauge for how many are open. Unlike the market-data
 * hub, the "interest" here is an account owning an order, not a subscription — an account is
 * registered the moment a connection authenticates, not on a per-symbol basis.
 */
public final class TcpConnectionRegistry {
    private final ConcurrentHashMap<String, Set<TcpConnection>> connectionsByAccount =
            new ConcurrentHashMap<>();
    private final AtomicInteger connectionCount = new AtomicInteger();

    public TcpConnectionRegistry(MeterRegistry meters) {
        Gauge.builder("trading.tcp.connections", connectionCount, AtomicInteger::get)
                .description("Open, authenticated TCP order-entry connections")
                .register(meters);
    }

    void register(String accountId, TcpConnection connection) {
        connectionsByAccount
                .computeIfAbsent(accountId, ignored -> ConcurrentHashMap.newKeySet())
                .add(connection);
        connectionCount.incrementAndGet();
    }

    void unregister(String accountId, TcpConnection connection) {
        connectionsByAccount.computeIfPresent(accountId, (ignored, connections) -> {
            connections.remove(connection);
            return connections.isEmpty() ? null : connections;
        });
        connectionCount.decrementAndGet();
    }

    /** Best-effort: pushes to every currently-open connection for this account, if any. */
    void pushToAccount(String accountId, ServerMessage message) {
        Set<TcpConnection> connections = connectionsByAccount.get(accountId);
        if (connections == null) {
            return;
        }
        for (TcpConnection connection : connections) {
            connection.push(message);
        }
    }

    int openConnectionCount() {
        return connectionCount.get();
    }
}
