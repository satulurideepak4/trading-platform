package com.tradingplatform.gateway.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param enabled when false, no TCP listener is started at all — the gateway trades exactly as it
 *     does without Stage 10, which is how every earlier stage's tests run with no TCP machinery
 *     present
 * @param port the listening port for the persistent order-entry protocol; see docs/tcp-protocol.md
 * @param maxConnections the acceptor refuses beyond this; each connection costs one reader thread,
 *     one writer thread and one outbound queue for the life of the connection
 * @param dispatchPoolSize shared across every connection — decoding a frame never blocks on
 *     ingress, but applying the decoded command does, so this bounds how many commands are being
 *     applied to the matching path at once regardless of how many connections are open
 * @param dispatchQueueCapacity the shared dispatch pool's own bounded queue; once full, the
 *     submitting connection's reader thread runs the command itself (see {@code TcpServer}'s
 *     {@code CallerRunsPolicy}), which is what turns overload into real, observable backpressure on
 *     that one connection's socket rather than an unbounded queue silently growing
 * @param outboundQueueCapacity per-connection; see {@code TcpConnection} and ADR-013's
 *     per-subscriber-queue pattern, which this mirrors
 * @param maxConsecutiveDrops consecutive dropped outbound messages before a stuck connection is
 *     disconnected
 * @param authTimeout how long a newly accepted connection has to send its first (AUTH) frame
 * @param idleTimeout how long an authenticated connection may go without sending anything before
 *     it is disconnected — {@code SO_TIMEOUT}, reset on every frame received
 * @param maxFrameLength a declared frame length above this is rejected before any payload bytes are
 *     read or a buffer for them is allocated; untrusted network input, unlike the matching-engine
 *     journal's own trusted files
 */
@ConfigurationProperties(prefix = "trading.tcp")
public record TcpProperties(
        boolean enabled,
        int port,
        int maxConnections,
        int dispatchPoolSize,
        int dispatchQueueCapacity,
        int outboundQueueCapacity,
        int maxConsecutiveDrops,
        Duration authTimeout,
        Duration idleTimeout,
        int maxFrameLength) {}
