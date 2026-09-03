package com.tradingplatform.gateway.tcp.protocol;

import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;

/**
 * One decoded client-to-server frame. {@code requestId} is client-chosen and echoed back in the
 * matching {@link ServerMessage.Ack} — a persistent connection lets a client have more than one
 * command in flight at once, so responses need something to correlate against besides arrival
 * order.
 */
public sealed interface ClientMessage {

    /** Must be the first frame on every connection; see {@link Auth}'s own Javadoc. */
    record Auth(String apiKey) implements ClientMessage {}

    record Submit(
            long requestId,
            String clientOrderId,
            String symbol,
            Side side,
            OrderType type,
            long quantity,
            long price,
            String strategyId)
            implements ClientMessage {}

    record Cancel(long requestId, long orderId) implements ClientMessage {}

    record Replace(long requestId, long orderId, long quantity, long price) implements ClientMessage {}
}
