package com.tradingplatform.matching;

import com.tradingplatform.domain.CancelOrder;
import com.tradingplatform.domain.CommandResult;
import com.tradingplatform.domain.Execution;
import com.tradingplatform.domain.OrderSnapshot;
import com.tradingplatform.domain.OrderType;
import com.tradingplatform.domain.RejectionReason;
import com.tradingplatform.domain.ReplaceOrder;
import com.tradingplatform.domain.Side;
import com.tradingplatform.domain.SubmitOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/** Deterministic, synchronous matching engine. The caller must serialize all commands. */
public final class MatchingEngine {
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("[A-Z0-9][A-Z0-9._-]{0,15}");

    private final Map<String, OrderBook> books = new HashMap<>();
    private final Map<Long, OrderState> orders = new HashMap<>();
    private final Set<Long> seenOrderIds = new HashSet<>();
    private final Set<String> seenClientOrderIds = new HashSet<>();

    private final long executionIdIncrement;
    private long nextPrioritySequence = 1;
    private long nextExecutionId;

    public MatchingEngine() {
        this(1, 1);
    }

    /**
     * Creates an engine with a deterministic execution-ID sequence. Sharded owners can use a
     * different positive first ID and a shared increment to produce disjoint ID sequences without
     * coordinating on the matching path.
     */
    public MatchingEngine(long firstExecutionId, long executionIdIncrement) {
        if (firstExecutionId <= 0 || executionIdIncrement <= 0) {
            throw new IllegalArgumentException("execution ID parameters must be positive");
        }
        this.nextExecutionId = firstExecutionId;
        this.executionIdIncrement = executionIdIncrement;
    }

    public CommandResult submit(SubmitOrder command) {
        if (command == null) {
            return CommandResult.rejected(0, RejectionReason.INVALID_ORDER_ID);
        }

        RejectionReason identifierRejection = validateIdentifiers(command);
        if (identifierRejection != null) {
            return CommandResult.rejected(command.orderId(), identifierRejection);
        }

        // Reserve valid identifiers even when later validation rejects the command. Reuse would
        // make a replay ambiguous because the same external identity would represent two attempts.
        seenOrderIds.add(command.orderId());
        seenClientOrderIds.add(command.clientOrderId());

        RejectionReason contentRejection = validateContent(command);
        if (contentRejection != null) {
            return CommandResult.rejected(command.orderId(), contentRejection);
        }

        OrderState order = new OrderState(command, nextPrioritySequence++);
        orders.put(order.orderId(), order);
        OrderBook book = books.computeIfAbsent(order.symbol(), OrderBook::new);
        List<Execution> executions = match(book, order, command.timestamp());
        if (order.remainingQuantity() > 0) {
            if (order.type() == OrderType.LIMIT) {
                book.add(order);
            } else {
                order.cancel(command.timestamp());
            }
        }
        return CommandResult.accepted(order.snapshot(), executions);
    }

    public CommandResult cancel(CancelOrder command) {
        if (command == null) {
            return CommandResult.rejected(0, RejectionReason.ORDER_NOT_FOUND);
        }
        OrderState order = orders.get(command.orderId());
        if (order == null) {
            return CommandResult.rejected(command.orderId(), RejectionReason.ORDER_NOT_FOUND);
        }
        if (command.timestamp() == null) {
            return rejectedWithOrder(order, RejectionReason.INVALID_TIMESTAMP);
        }
        if (!order.isActive()) {
            return rejectedWithOrder(order, RejectionReason.ORDER_NOT_ACTIVE);
        }

        books.get(order.symbol()).remove(order);
        order.cancel(command.timestamp());
        return CommandResult.accepted(order.snapshot(), List.of());
    }

    public CommandResult replace(ReplaceOrder command) {
        if (command == null) {
            return CommandResult.rejected(0, RejectionReason.ORDER_NOT_FOUND);
        }
        OrderState order = orders.get(command.orderId());
        if (order == null) {
            return CommandResult.rejected(command.orderId(), RejectionReason.ORDER_NOT_FOUND);
        }
        RejectionReason rejection = validateReplacement(command, order);
        if (rejection != null) {
            return rejectedWithOrder(order, rejection);
        }

        OrderBook book = books.get(order.symbol());
        book.remove(order);
        order.replace(
                command.newQuantity(),
                command.newPrice(),
                command.timestamp(),
                nextPrioritySequence++);

        List<Execution> executions = order.remainingQuantity() == 0
                ? List.of()
                : match(book, order, command.timestamp());
        if (order.remainingQuantity() > 0) {
            book.add(order);
        }
        return CommandResult.accepted(order.snapshot(), executions);
    }

    public Optional<OrderSnapshot> findOrder(long orderId) {
        OrderState order = orders.get(orderId);
        return order == null ? Optional.empty() : Optional.of(order.snapshot());
    }

    public OrderBookSnapshot book(String symbol) {
        OrderBook book = books.get(symbol);
        return book == null
                ? new OrderBookSnapshot(symbol, List.of(), List.of())
                : book.snapshot();
    }

    private List<Execution> match(OrderBook book, OrderState taker, Instant timestamp) {
        List<Execution> executions = new ArrayList<>();
        while (taker.remainingQuantity() > 0) {
            OrderState maker = book.bestOpposite(taker.side());
            if (maker == null || !book.crosses(taker, maker)) {
                break;
            }

            long executedQuantity = Math.min(taker.remainingQuantity(), maker.remainingQuantity());
            long executionPrice = maker.price();
            taker.fill(executedQuantity, timestamp);
            maker.fill(executedQuantity, timestamp);

            OrderState buy = taker.side() == Side.BUY ? taker : maker;
            OrderState sell = taker.side() == Side.SELL ? taker : maker;
            executions.add(new Execution(
                    nextExecutionId(),
                    taker.symbol(),
                    executionPrice,
                    executedQuantity,
                    buy.orderId(),
                    buy.clientOrderId(),
                    sell.orderId(),
                    sell.clientOrderId(),
                    maker.orderId(),
                    taker.orderId(),
                    buy.remainingQuantity(),
                    sell.remainingQuantity(),
                    timestamp));

            if (!maker.isActive()) {
                book.remove(maker);
            }
        }
        return List.copyOf(executions);
    }

    private long nextExecutionId() {
        long executionId = nextExecutionId;
        try {
            nextExecutionId = Math.addExact(nextExecutionId, executionIdIncrement);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("execution ID sequence exhausted", overflow);
        }
        return executionId;
    }

    private RejectionReason validateIdentifiers(SubmitOrder command) {
        if (command.orderId() <= 0) {
            return RejectionReason.INVALID_ORDER_ID;
        }
        if (command.clientOrderId() == null
                || command.clientOrderId().isBlank()
                || command.clientOrderId().length() > 64) {
            return RejectionReason.INVALID_CLIENT_ORDER_ID;
        }
        if (seenOrderIds.contains(command.orderId())) {
            return RejectionReason.DUPLICATE_ORDER_ID;
        }
        if (seenClientOrderIds.contains(command.clientOrderId())) {
            return RejectionReason.DUPLICATE_CLIENT_ORDER_ID;
        }
        return null;
    }

    private static RejectionReason validateContent(SubmitOrder command) {
        if (command.symbol() == null || !SYMBOL_PATTERN.matcher(command.symbol()).matches()) {
            return RejectionReason.INVALID_SYMBOL;
        }
        if (command.side() == null) {
            return RejectionReason.INVALID_SIDE;
        }
        if (command.type() == null) {
            return RejectionReason.INVALID_ORDER_TYPE;
        }
        if (command.quantity() <= 0) {
            return RejectionReason.INVALID_QUANTITY;
        }
        if ((command.type() == OrderType.LIMIT && command.price() <= 0)
                || (command.type() == OrderType.MARKET && command.price() != 0)) {
            return RejectionReason.INVALID_PRICE;
        }
        if (command.timestamp() == null) {
            return RejectionReason.INVALID_TIMESTAMP;
        }
        return null;
    }

    private static RejectionReason validateReplacement(ReplaceOrder command, OrderState order) {
        if (command.timestamp() == null) {
            return RejectionReason.INVALID_TIMESTAMP;
        }
        if (!order.isActive()) {
            return RejectionReason.ORDER_NOT_ACTIVE;
        }
        if (order.type() != OrderType.LIMIT) {
            return RejectionReason.REPLACE_NOT_SUPPORTED;
        }
        if (command.newQuantity() <= 0) {
            return RejectionReason.INVALID_QUANTITY;
        }
        if (command.newPrice() <= 0) {
            return RejectionReason.INVALID_PRICE;
        }
        if (command.newQuantity() < order.executedQuantity()) {
            return RejectionReason.REPLACEMENT_QUANTITY_BELOW_FILLED_QUANTITY;
        }
        return null;
    }

    private static CommandResult rejectedWithOrder(OrderState order, RejectionReason reason) {
        return CommandResult.rejected(order.orderId(), reason, Optional.of(order.snapshot()));
    }
}
