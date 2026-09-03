package com.tradingplatform.gateway.tcp;

import com.tradingplatform.gateway.auth.AuthenticatedClient;
import com.tradingplatform.gateway.auth.ClientAuthenticator;
import com.tradingplatform.gateway.auth.UnauthenticatedClientException;
import com.tradingplatform.gateway.ingress.OrderIngressService;
import com.tradingplatform.gateway.ingress.OrderOutcome;
import com.tradingplatform.gateway.ingress.OrderRegistry.OrderContent;
import com.tradingplatform.gateway.ingress.RejectionSource;
import com.tradingplatform.gateway.tcp.protocol.ClientMessage;
import com.tradingplatform.gateway.tcp.protocol.FrameIO;
import com.tradingplatform.gateway.tcp.protocol.OrderEntryCodec;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage;
import com.tradingplatform.gateway.tcp.protocol.TcpProtocolException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * One accepted TCP socket: an authentication handshake, then a reader loop that decodes frames and
 * dispatches them to the shared ingress pipeline, plus this connection's own bounded outbound
 * queue and writer thread for everything the reader doesn't answer directly (an async {@code
 * ExecutionPush}).
 *
 * <h2>Why two threads, not one</h2>
 *
 * <p>The reader must never block waiting to write — a slow or absent reader on the client's side of
 * the socket must not stall the connection from continuing to receive and dispatch new commands
 * (real TCP backpressure comes from the socket's own send buffer filling, not from this connection
 * blocking itself). Every outbound message, from either the reader thread's own request/response
 * path or an async push arriving from a completely different connection's fill, funnels through one
 * queue and one writer thread — the same non-blocking-offer, drop-oldest-on-overflow,
 * disconnect-after-sustained-overflow shape {@code market-data}'s {@code
 * QueuedMarketDataSubscription} already uses (ADR-013) — so two completions can never interleave
 * their bytes on the wire and a stuck client's queue only ever costs this one connection.
 *
 * <h2>Why the reader never calls {@code OrderIngressService} itself</h2>
 *
 * <p>{@code OrderIngressService.submit/cancel/replace} block the calling thread until the matching
 * engine responds or a timeout elapses. If the reader thread made that call directly, this
 * connection could have at most one command in flight at a time — exactly what a persistent,
 * pipeline-capable connection is supposed to avoid. Every decoded command is instead handed to the
 * server's shared dispatch pool; see {@link TcpServer}.
 */
final class TcpConnection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TcpConnection.class);

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final ClientAuthenticator authenticator;
    private final OrderIngressService ingress;
    private final ExecutorService dispatchPool;
    private final TcpConnectionRegistry registry;
    private final Duration authTimeout;
    private final Duration idleTimeout;
    private final int maxFrameLength;
    private final int maxConsecutiveDrops;
    private final BlockingQueue<ServerMessage> outboundQueue;
    private final Thread readerThread;
    private final Thread writerThread;

    private volatile String accountId;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger consecutiveDrops = new AtomicInteger();

    TcpConnection(
            Socket socket,
            ClientAuthenticator authenticator,
            OrderIngressService ingress,
            ExecutorService dispatchPool,
            TcpConnectionRegistry registry,
            Duration authTimeout,
            Duration idleTimeout,
            int maxFrameLength,
            int outboundQueueCapacity,
            int maxConsecutiveDrops)
            throws IOException {
        // Left at the JDK default (Nagle's algorithm enabled), small ACK/SUBMIT writes can be
        // delayed tens of milliseconds waiting to coalesce with more outbound data that may never
        // come - exactly the kind of hidden cost that would make a "low-latency" path measure worse
        // than plain HTTP. See ADR-016.
        socket.setTcpNoDelay(true);
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.authenticator = authenticator;
        this.ingress = ingress;
        this.dispatchPool = dispatchPool;
        this.registry = registry;
        this.authTimeout = authTimeout;
        this.idleTimeout = idleTimeout;
        this.maxFrameLength = maxFrameLength;
        this.maxConsecutiveDrops = maxConsecutiveDrops;
        this.outboundQueue = new ArrayBlockingQueue<>(outboundQueueCapacity);
        String label = String.valueOf(socket.getRemoteSocketAddress());
        this.readerThread = Thread.ofPlatform().name("tcp-reader-" + label).unstarted(this::readerLoop);
        this.writerThread = Thread.ofPlatform().name("tcp-writer-" + label).unstarted(this::writerLoop);
    }

    void start() {
        writerThread.start();
        readerThread.start();
    }

    /** Non-blocking; see the class Javadoc for why a full queue drops rather than waits. */
    void push(ServerMessage message) {
        if (outboundQueue.offer(message)) {
            consecutiveDrops.set(0);
            return;
        }
        outboundQueue.poll();
        if (outboundQueue.offer(message) && consecutiveDrops.incrementAndGet() >= maxConsecutiveDrops) {
            LOG.warn(
                    "event=tcp_connection_disconnected account={} reason=sustained_outbound_overflow",
                    accountId);
            close();
        }
    }

    private void readerLoop() {
        try {
            if (!authenticate()) {
                return;
            }
            socket.setSoTimeout((int) idleTimeout.toMillis());
            while (!closed.get()) {
                Optional<byte[]> frame;
                try {
                    frame = FrameIO.readFrame(in, maxFrameLength);
                } catch (SocketTimeoutException idle) {
                    LOG.info("event=tcp_connection_idle_timeout account={}", accountId);
                    break;
                }
                if (frame.isEmpty()) {
                    break;
                }
                ClientMessage message = OrderEntryCodec.decodeClientMessage(frame.get());
                dispatch(message);
            }
        } catch (TcpProtocolException protocolFailure) {
            LOG.info(
                    "event=tcp_connection_protocol_error account={} reason=\"{}\"",
                    accountId,
                    protocolFailure.getMessage());
            // Written directly, not through push()/the outbound queue: this connection is about to
            // close in the finally block below, and a queued message racing that close against the
            // writer thread being interrupted is exactly the kind of delivery this diagnostic is
            // supposed to survive.
            tryWriteDirectly(new ServerMessage.Error(protocolFailure.getMessage()));
        } catch (IOException | RuntimeException failure) {
            if (!closed.get()) {
                LOG.info(
                        "event=tcp_connection_reader_failed account={} reason=\"{}\"",
                        accountId,
                        failure.getMessage());
            }
        } finally {
            close();
        }
    }

    /** @return true if authentication succeeded and the reader loop should continue */
    private boolean authenticate() throws IOException {
        socket.setSoTimeout((int) authTimeout.toMillis());
        Optional<byte[]> frame;
        try {
            frame = FrameIO.readFrame(in, maxFrameLength);
        } catch (SocketTimeoutException timedOut) {
            LOG.info("event=tcp_connection_auth_timeout");
            return false;
        }
        if (frame.isEmpty()) {
            return false;
        }
        ClientMessage first = OrderEntryCodec.decodeClientMessage(frame.get());
        if (!(first instanceof ClientMessage.Auth auth)) {
            writeDirectly(new ServerMessage.AuthReject("AUTH must be the first message"));
            return false;
        }
        try {
            AuthenticatedClient client = authenticator.authenticate(auth.apiKey());
            accountId = client.accountId();
        } catch (UnauthenticatedClientException rejected) {
            LOG.info("event=tcp_auth_rejected reason=\"{}\"", rejected.getMessage());
            writeDirectly(new ServerMessage.AuthReject(rejected.getMessage()));
            return false;
        }
        registry.register(accountId, this);
        writeDirectly(new ServerMessage.AuthAck(accountId));
        return true;
    }

    /** Only ever called from the reader thread, before the writer thread has anything competing to
     * write - the one exception to "everything goes through the outbound queue", made so a client
     * gets AUTH_ACK/AUTH_REJECT without waiting on a queue that has nothing else in it yet anyway. */
    private void writeDirectly(ServerMessage message) throws IOException {
        FrameIO.writeFrame(out, OrderEntryCodec.encode(message));
    }

    /** {@link #writeDirectly} for the handful of call sites already inside exception handling,
     * where a second failure trying to report the first must never stop the connection from still
     * reaching its {@code close()}. */
    private void tryWriteDirectly(ServerMessage message) {
        try {
            writeDirectly(message);
        } catch (IOException failed) {
            LOG.debug("event=tcp_final_message_write_failed account={}", accountId);
        }
    }

    private void dispatch(ClientMessage message) {
        try {
            dispatchPool.execute(() -> handle(message));
        } catch (RejectedExecutionException poolShutdown) {
            // Only happens while the server itself is shutting down.
            LOG.debug("event=tcp_dispatch_rejected_pool_shutdown account={}", accountId);
        }
    }

    private void handle(ClientMessage message) {
        try (MDC.MDCCloseable ignored = MDC.putCloseable("accountId", accountId)) {
            OrderOutcome outcome = apply(message);
            push(ServerMessage.Ack.from(requestIdOf(message), outcome));
        } catch (RuntimeException failure) {
            LOG.info(
                    "event=tcp_command_failed account={} reason=\"{}\"",
                    accountId,
                    failure.getMessage());
            OrderOutcome synthetic = OrderOutcome.rejected(
                    0,
                    "",
                    RejectionSource.MATCHING_ENGINE,
                    failure.getClass().getSimpleName(),
                    String.valueOf(failure.getMessage()));
            push(ServerMessage.Ack.from(requestIdOf(message), synthetic));
        }
    }

    private OrderOutcome apply(ClientMessage message) {
        return switch (message) {
            case ClientMessage.Submit submit -> ingress.submit(
                    accountId,
                    submit.clientOrderId(),
                    new OrderContent(
                            submit.symbol(),
                            submit.side(),
                            submit.type(),
                            submit.quantity(),
                            submit.price(),
                            submit.strategyId() == null || submit.strategyId().isBlank()
                                    ? "DEFAULT"
                                    : submit.strategyId()));
            case ClientMessage.Cancel cancel -> ingress.cancel(accountId, cancel.orderId());
            case ClientMessage.Replace replace ->
                    ingress.replace(accountId, replace.orderId(), replace.quantity(), replace.price());
            case ClientMessage.Auth auth ->
                    throw new TcpProtocolException("AUTH is only valid as the first message");
        };
    }

    private static long requestIdOf(ClientMessage message) {
        return switch (message) {
            case ClientMessage.Submit submit -> submit.requestId();
            case ClientMessage.Cancel cancel -> cancel.requestId();
            case ClientMessage.Replace replace -> replace.requestId();
            case ClientMessage.Auth auth -> -1L;
        };
    }

    private void writerLoop() {
        try {
            while (!closed.get()) {
                ServerMessage message = outboundQueue.poll(200, TimeUnit.MILLISECONDS);
                if (message != null) {
                    writeDirectly(message);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (IOException failure) {
            if (!closed.get()) {
                LOG.info(
                        "event=tcp_connection_writer_failed account={} reason=\"{}\"",
                        accountId,
                        failure.getMessage());
            }
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (accountId != null) {
            registry.unregister(accountId, this);
        }
        readerThread.interrupt();
        writerThread.interrupt();
        try {
            socket.close();
        } catch (IOException ignored) {
            // Nothing useful to do with a failure to close an already-failing socket.
        }
    }
}
