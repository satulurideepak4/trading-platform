package com.tradingplatform.benchmark.load;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.SubmitOrder;
import com.tradingplatform.gateway.tcp.protocol.ClientMessage;
import com.tradingplatform.gateway.tcp.protocol.FrameIO;
import com.tradingplatform.gateway.tcp.protocol.OrderEntryCodec;
import com.tradingplatform.gateway.tcp.protocol.ServerMessage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 10's persistent TCP+binary comparison target — one real socket, authenticated once, then
 * pipelined: every {@link #submit}/{@link #cancel} writes its frame and returns immediately rather
 * than waiting for that command's own {@code ACK}, the same pipelining {@code TcpConnection}'s
 * design is built to allow (see its Javadoc). A background reader thread continuously decodes
 * frames and completes whichever pending {@link CompletableFuture} that {@code ACK}'s {@code
 * requestId} belongs to; an unsolicited {@code EXECUTION_PUSH} is simply not resolved to anything
 * here, since this target's counterparty is always this same process's own producer traffic and the
 * fills that matter to a caller already arrive inline in that caller's own {@code ACK}.
 *
 * <p>Writes are synchronized: {@code LoadTestRunner} drives every {@link SubmissionTarget} from
 * several concurrent producer threads sharing one target instance, and two threads interleaving
 * their frame bytes on the same socket would corrupt the stream for both.
 */
public final class TcpBinarySubmissionTarget implements SubmissionTarget {
    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private final Thread readerThread;
    private final AtomicLong requestIds = new AtomicLong();
    private final Map<Long, PendingRequest> pending = new ConcurrentHashMap<>();
    private final Map<Long, Long> serverOrderIds = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public TcpBinarySubmissionTarget(String host, int port, String apiKey) {
        try {
            this.socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            authenticate(apiKey);
        } catch (IOException e) {
            throw new UncheckedIOException("could not connect/authenticate to " + host + ":" + port, e);
        }
        this.readerThread = Thread.ofPlatform().name("tcp-target-reader").start(this::readLoop);
    }

    private void authenticate(String apiKey) throws IOException {
        writeFrame(new ClientMessage.Auth(apiKey));
        byte[] frame = FrameIO.readFrame(in, 65_536)
                .orElseThrow(() -> new IOException("connection closed during auth"));
        if (!(OrderEntryCodec.decodeServerMessage(frame) instanceof ServerMessage.AuthAck)) {
            throw new IOException("authentication rejected");
        }
    }

    @Override
    public CompletableFuture<SubmissionOutcome> submit(SubmitOrder command) {
        long requestId = requestIds.incrementAndGet();
        CompletableFuture<SubmissionOutcome> future = new CompletableFuture<>();
        pending.put(requestId, new PendingRequest(future, command.orderId()));
        // Empty strategyId means "use the account default" - SubmitOrder itself carries no
        // strategy concept (see its own field list); OrderRouter/OrderIngressService default it.
        writeFrameOrFail(requestId, new ClientMessage.Submit(
                requestId, command.clientOrderId(), command.symbol(), command.side(), command.type(),
                command.quantity(), command.price(), ""), future);
        return future;
    }

    @Override
    public CompletableFuture<SubmissionOutcome> cancel(CancelOrder command) {
        Long serverOrderId = serverOrderIds.get(command.orderId());
        if (serverOrderId == null) {
            return CompletableFuture.completedFuture(new SubmissionOutcome(false, 0));
        }
        long requestId = requestIds.incrementAndGet();
        CompletableFuture<SubmissionOutcome> future = new CompletableFuture<>();
        pending.put(requestId, new PendingRequest(future, command.orderId()));
        writeFrameOrFail(requestId, new ClientMessage.Cancel(requestId, serverOrderId), future);
        return future;
    }

    private void writeFrameOrFail(
            long requestId, ClientMessage message, CompletableFuture<SubmissionOutcome> future) {
        try {
            writeFrame(message);
        } catch (IOException failed) {
            pending.remove(requestId);
            future.completeExceptionally(failed);
        }
    }

    private synchronized void writeFrame(ClientMessage message) throws IOException {
        FrameIO.writeFrame(out, OrderEntryCodec.encode(message));
    }

    private void readLoop() {
        try {
            while (!closed) {
                Optional<byte[]> frame = FrameIO.readFrame(in, 65_536);
                if (frame.isEmpty()) {
                    break;
                }
                handle(OrderEntryCodec.decodeServerMessage(frame.get()));
            }
        } catch (IOException ignored) {
            // The connection closing (deliberately, at close(), or the server ending it) is the
            // ordinary way this loop stops; any request still pending gets failed below either way.
        } finally {
            failAllPending();
        }
    }

    private void handle(ServerMessage message) {
        if (message instanceof ServerMessage.Ack ack) {
            PendingRequest request = pending.remove(ack.requestId());
            if (request == null) {
                return;
            }
            if (ack.accepted()) {
                serverOrderIds.put(request.localOrderId(), ack.orderId());
            }
            request.future().complete(new SubmissionOutcome(ack.accepted(), ack.executions().size()));
        }
        // ExecutionPush/Error/AuthAck/AuthReject: nothing pending to resolve for these here - see
        // the class Javadoc.
    }

    private void failAllPending() {
        IOException closedException = new IOException("connection closed");
        pending.forEach((requestId, request) -> request.future().completeExceptionally(closedException));
        pending.clear();
    }

    @Override
    public int queueDepth() {
        return pending.size();
    }

    @Override
    public void close() {
        closed = true;
        readerThread.interrupt();
        try {
            socket.close();
        } catch (IOException ignored) {
            // Nothing useful to do with a failure to close an already-closing socket.
        }
    }

    private record PendingRequest(CompletableFuture<SubmissionOutcome> future, long localOrderId) {}
}
