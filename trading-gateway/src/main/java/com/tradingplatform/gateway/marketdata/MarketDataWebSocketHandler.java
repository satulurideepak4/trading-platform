package com.tradingplatform.gateway.marketdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingplatform.marketdata.codec.MarketDataCodec;
import com.tradingplatform.marketdata.process.MarketDataProcessor;
import com.tradingplatform.marketdata.process.NormalizedUpdate;
import com.tradingplatform.marketdata.process.SnapshotUpdates;
import com.tradingplatform.marketdata.publish.MarketDataHub;
import com.tradingplatform.marketdata.publish.MarketDataListener;
import com.tradingplatform.marketdata.publish.Subscription;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

/**
 * The live feed over WebSocket. One connection may subscribe to any number of symbols and change
 * its subscription at any time; see {@code docs/market-data.md} for the wire protocol.
 *
 * <p>Control messages — {@code subscribe}/{@code unsubscribe} — are always JSON, independent of
 * which {@link MarketDataCodec} is configured for data frames. They are small, infrequent, and a
 * client needs to be able to speak them before it has negotiated anything else.
 *
 * <p>Every session is wrapped in a {@link ConcurrentWebSocketSessionDecorator} the moment it is
 * established, and every send — the initial snapshot pushed from this handler's own thread, and
 * every live update pushed from that subscription's drain thread — goes through the same wrapper.
 * A raw {@link WebSocketSession} is not safe for concurrent {@code sendMessage} calls, and both of
 * those are exactly concurrent calls from different threads.
 */
public class MarketDataWebSocketHandler extends AbstractWebSocketHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataWebSocketHandler.class);
    private static final int SEND_TIME_LIMIT_MILLIS = 5_000;
    private static final int SEND_BUFFER_SIZE_LIMIT_BYTES = 512 * 1024;

    private final MarketDataHub hub;
    private final MarketDataProcessor processor;
    private final MarketDataCodec codec;
    private final ObjectMapper controlMessageMapper;
    private final ConcurrentHashMap<String, SessionState> sessions = new ConcurrentHashMap<>();

    public MarketDataWebSocketHandler(
            MarketDataHub hub,
            MarketDataProcessor processor,
            MarketDataCodec codec,
            ObjectMapper controlMessageMapper) {
        this.hub = hub;
        this.processor = processor;
        this.codec = codec;
        this.controlMessageMapper = controlMessageMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        WebSocketSession decorated = new ConcurrentWebSocketSessionDecorator(
                session, SEND_TIME_LIMIT_MILLIS, SEND_BUFFER_SIZE_LIMIT_BYTES);
        sessions.put(session.getId(), new SessionState(decorated));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        SessionState state = sessions.get(session.getId());
        if (state == null) {
            return;
        }
        ControlMessage control;
        try {
            control = controlMessageMapper.readValue(message.getPayload(), ControlMessage.class);
        } catch (IOException malformed) {
            LOG.debug("event=marketdata_control_message_invalid session={}", session.getId());
            return;
        }
        switch (control.type() == null ? "" : control.type()) {
            case "subscribe" -> subscribe(state, control.symbolSet());
            case "unsubscribe" -> unsubscribe(state, control.symbolSet());
            default -> LOG.debug(
                    "event=marketdata_control_message_unknown session={} type={}",
                    session.getId(),
                    control.type());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        SessionState state = sessions.remove(session.getId());
        if (state != null) {
            state.close();
        }
    }

    private synchronized void subscribe(SessionState state, Set<String> requested) {
        if (requested.isEmpty()) {
            return;
        }
        state.symbols.addAll(requested);
        MarketDataListener listener = update -> send(state.session, update);
        if (state.subscription == null) {
            state.subscription = hub.subscribe(state.symbols, listener);
        } else {
            state.subscription.updateSymbols(state.symbols);
        }
        // Newly subscribed symbols get their current state pushed immediately, from this thread —
        // a client must not have to wait for the next tick to learn what a symbol is doing right
        // now. Symbols the session was already subscribed to are not re-sent.
        requested.forEach(symbol ->
                processor.snapshot(symbol).map(SnapshotUpdates::of).orElse(List.of())
                        .forEach(update -> send(state.session, update)));
    }

    private synchronized void unsubscribe(SessionState state, Set<String> toRemove) {
        state.symbols.removeAll(toRemove);
        if (state.subscription != null) {
            state.subscription.updateSymbols(state.symbols);
        }
    }

    private void send(WebSocketSession session, NormalizedUpdate update) {
        try {
            byte[] encoded = codec.encode(update);
            if (codec.isText()) {
                session.sendMessage(new TextMessage(encoded));
            } else {
                session.sendMessage(new BinaryMessage(encoded));
            }
        } catch (IOException | IllegalStateException failure) {
            // A session mid-close or over its own buffer limit is not this handler's problem to
            // recover from; afterConnectionClosed will clean up the subscription shortly.
            LOG.debug("event=marketdata_send_failed session={}", session.getId(), failure);
        }
    }

    private static final class SessionState {
        private final WebSocketSession session;
        private final Set<String> symbols = new HashSet<>();
        private volatile Subscription subscription;

        SessionState(WebSocketSession session) {
            this.session = session;
        }

        void close() {
            if (subscription != null) {
                subscription.close();
            }
        }
    }

    /** {@code symbols} defaults to none if the client omits it, which is a no-op either way. */
    private record ControlMessage(String type, List<String> symbols) {
        Set<String> symbolSet() {
            return symbols == null ? Set.of() : new HashSet<>(symbols);
        }
    }
}
