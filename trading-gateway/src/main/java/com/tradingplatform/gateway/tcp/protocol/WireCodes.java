package com.tradingplatform.gateway.tcp.protocol;

import com.tradingplatform.domain.OrderStatus;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.Side;
import com.tradingplatform.gateway.ingress.RejectionSource;

/**
 * Explicit, protocol-owned byte codes for every domain enum this wire format carries — never
 * {@code .ordinal()}.
 *
 * <p>This exists because of a real bug found and fixed earlier in this project: {@code
 * matching-engine}'s journal codec once stored {@code Side}/{@code OrderType} ordinals directly,
 * with nothing stopping a future reordering of those enum constants from silently changing what an
 * already-written record decodes to. A network protocol has the identical failure shape — a client
 * built against one version of these enums, talking to a server built against a reordered one —
 * except the failure mode is worse: it corrupts every connected client at once instead of one
 * journal file. Every mapping here is a explicit {@code switch}, in both directions, completely
 * independent of the enums' own declared order.
 */
final class WireCodes {
    private WireCodes() {}

    static byte side(Side side) {
        return switch (side) {
            case BUY -> 0;
            case SELL -> 1;
        };
    }

    static Side side(byte code) {
        return switch (code) {
            case 0 -> Side.BUY;
            case 1 -> Side.SELL;
            default -> throw new TcpProtocolException("unknown side code " + code);
        };
    }

    static byte orderType(OrderType type) {
        return switch (type) {
            case LIMIT -> 0;
            case MARKET -> 1;
        };
    }

    static OrderType orderType(byte code) {
        return switch (code) {
            case 0 -> OrderType.LIMIT;
            case 1 -> OrderType.MARKET;
            default -> throw new TcpProtocolException("unknown order type code " + code);
        };
    }

    static byte orderStatus(OrderStatus status) {
        return switch (status) {
            case NEW -> 0;
            case PARTIALLY_FILLED -> 1;
            case FILLED -> 2;
            case CANCELLED -> 3;
            case REJECTED -> 4;
        };
    }

    static OrderStatus orderStatus(byte code) {
        return switch (code) {
            case 0 -> OrderStatus.NEW;
            case 1 -> OrderStatus.PARTIALLY_FILLED;
            case 2 -> OrderStatus.FILLED;
            case 3 -> OrderStatus.CANCELLED;
            case 4 -> OrderStatus.REJECTED;
            default -> throw new TcpProtocolException("unknown order status code " + code);
        };
    }

    static byte rejectionSource(RejectionSource source) {
        return switch (source) {
            case RISK -> 0;
            case MATCHING_ENGINE -> 1;
        };
    }

    static RejectionSource rejectionSource(byte code) {
        return switch (code) {
            case 0 -> RejectionSource.RISK;
            case 1 -> RejectionSource.MATCHING_ENGINE;
            default -> throw new TcpProtocolException("unknown rejection source code " + code);
        };
    }

    /** MAKER when the order this execution is being reported to was already resting. */
    static byte liquidity(boolean maker) {
        return maker ? (byte) 0 : (byte) 1;
    }

    static boolean liquidity(byte code) {
        return switch (code) {
            case 0 -> true;
            case 1 -> false;
            default -> throw new TcpProtocolException("unknown liquidity code " + code);
        };
    }
}
