package com.tradingplatform.gateway.tcp;

import com.tradingplatform.gateway.auth.ClientAuthenticator;
import com.tradingplatform.gateway.ingress.OrderIngressService;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts TCP connections for the persistent order-entry protocol; see {@code docs/tcp-protocol.md}
 * and ADR-016.
 *
 * <h2>The shared dispatch pool and why saturation is a feature, not a bug</h2>
 *
 * <p>Every connection hands its decoded commands to one bounded {@link ThreadPoolExecutor}, sized
 * independently of connection count — matching demand isn't "one thread per connection forever," it
 * is "how much concurrent work can actually be applied to the matching path at once." Its rejection
 * policy is {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}: when the pool and its
 * queue are both full, the connection's own reader thread runs the command itself instead of the
 * command being dropped or an unbounded queue growing without limit. That stops the offending
 * connection's reader from returning to {@code Socket.read()}, which stops it draining its TCP
 * receive buffer, which makes the client's own next write block — real, observable, kernel-level
 * backpressure produced by an actual mechanism, not asserted in a comment. See {@code
 * docs/networking-comparison.md}'s backpressure section for this traced end to end.
 */
public final class TcpServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TcpServer.class);

    private final int port;
    private final int maxConnections;
    private final Duration authTimeout;
    private final Duration idleTimeout;
    private final int maxFrameLength;
    private final int outboundQueueCapacity;
    private final int maxConsecutiveDrops;
    private final ClientAuthenticator authenticator;
    private final OrderIngressService ingress;
    private final TcpConnectionRegistry registry;
    private final ThreadPoolExecutor dispatchPool;
    private final Set<TcpConnection> connections = ConcurrentHashMap.newKeySet();

    private ServerSocket serverSocket;
    private Thread acceptorThread;
    private volatile boolean closed;

    public TcpServer(
            int port,
            int maxConnections,
            int dispatchPoolSize,
            int dispatchQueueCapacity,
            Duration authTimeout,
            Duration idleTimeout,
            int maxFrameLength,
            int outboundQueueCapacity,
            int maxConsecutiveDrops,
            ClientAuthenticator authenticator,
            OrderIngressService ingress,
            TcpConnectionRegistry registry) {
        this.port = port;
        this.maxConnections = maxConnections;
        this.authTimeout = authTimeout;
        this.idleTimeout = idleTimeout;
        this.maxFrameLength = maxFrameLength;
        this.outboundQueueCapacity = outboundQueueCapacity;
        this.maxConsecutiveDrops = maxConsecutiveDrops;
        this.authenticator = authenticator;
        this.ingress = ingress;
        this.registry = registry;
        this.dispatchPool = new ThreadPoolExecutor(
                dispatchPoolSize,
                dispatchPoolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(dispatchQueueCapacity),
                Thread.ofPlatform().name("tcp-dispatch-", 0).factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("could not bind TCP order-entry port " + port, e);
        }
        acceptorThread = Thread.ofPlatform().name("tcp-acceptor").start(this::acceptLoop);
        LOG.info("event=tcp_server_started port={}", port);
    }

    private void acceptLoop() {
        while (!closed) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException stoppedOrFailed) {
                if (!closed) {
                    LOG.warn("event=tcp_accept_failed", stoppedOrFailed);
                }
                continue;
            }
            if (connections.size() >= maxConnections) {
                LOG.warn(
                        "event=tcp_connection_refused reason=max_connections remote={}",
                        socket.getRemoteSocketAddress());
                closeQuietly(socket);
                continue;
            }
            acceptConnection(socket);
        }
    }

    private void acceptConnection(Socket socket) {
        try {
            TcpConnection connection = new TcpConnection(
                    socket,
                    authenticator,
                    ingress,
                    dispatchPool,
                    registry,
                    authTimeout,
                    idleTimeout,
                    maxFrameLength,
                    outboundQueueCapacity,
                    maxConsecutiveDrops);
            connections.add(connection);
            connection.start();
        } catch (IOException failedToWrapSocket) {
            LOG.warn("event=tcp_connection_setup_failed remote={}", socket.getRemoteSocketAddress());
            closeQuietly(socket);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // The connection was being refused anyway.
        }
    }

    public int openConnectionCount() {
        return registry.openConnectionCount();
    }

    /** The actual bound port — differs from the configured one when that was {@code 0}, letting
     * the OS assign an ephemeral port, which is what tests use to avoid a fixed-port conflict. */
    public int port() {
        return serverSocket.getLocalPort();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // Nothing useful to do with a failure to close a socket already being torn down.
        }
        if (acceptorThread != null) {
            acceptorThread.interrupt();
        }
        connections.forEach(TcpConnection::close);
        dispatchPool.shutdown();
        try {
            if (!dispatchPool.awaitTermination(5, TimeUnit.SECONDS)) {
                dispatchPool.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            dispatchPool.shutdownNow();
        }
        LOG.info("event=tcp_server_stopped");
    }
}
