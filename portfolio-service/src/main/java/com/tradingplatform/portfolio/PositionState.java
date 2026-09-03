package com.tradingplatform.portfolio;

import com.tradingplatform.domain.Side;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Optional;

/**
 * What one book holds in one instrument, and the arithmetic that gets it there.
 *
 * <h2>Representation</h2>
 *
 * Everything is a {@code long}, and nothing is a floating-point number. Prices are integer ticks
 * and quantities are whole units, so every monetary value here is an exact product of the two —
 * call the unit "tick-quantity".
 *
 * <ul>
 *   <li>{@code openQuantity} is signed: positive is long, negative is short.
 *   <li>{@code openCost} is the cost of the position still open, signed to match. A long has paid
 *       out, so its cost is positive; a short has received proceeds, so its cost is negative.
 * </ul>
 *
 * <p>Holding cost rather than average price is what keeps the arithmetic exact. Unrealized P&L is
 * {@code openQuantity * mark - openCost}, with no division anywhere. The average entry price is the
 * only value that needs one, and it is derived on read and never written back, so a rounded number
 * can never feed into the next calculation.
 *
 * <h2>Accounting method</h2>
 *
 * Weighted average cost. Adding to a position adds to its cost; reducing one realizes the
 * difference between the trade price and the average cost of the part closed. A trade large enough
 * to cross through zero closes the old position completely and opens a new one at the trade price,
 * which is the only sane reading of a sell of 15 against a long of 10.
 *
 * <p>Reducing part of a position needs a proportional share of its cost, and that is the one place
 * integer division appears. The remainder is deliberately left in {@code openCost} rather than
 * discarded, and closing a position out uses its entire remaining cost, so a full round trip
 * realizes exactly the difference between what was paid and what was received. A partially closed
 * position can be off by at most one tick-quantity unit until it is fully closed, and that unit is
 * recovered rather than lost.
 */
public record PositionState(
        long openQuantity,
        long openCost,
        long realizedPnl,
        long boughtQuantity,
        long soldQuantity,
        long executionCount) {

    public static final PositionState FLAT = new PositionState(0, 0, 0, 0, 0, 0);

    public PositionState {
        if (openQuantity == 0 && openCost != 0) {
            throw new IllegalArgumentException(
                    "a flat position cannot hold cost, but held " + openCost);
        }
    }

    public PositionState applyFill(Side side, long quantity, long price) {
        if (quantity <= 0 || price <= 0) {
            throw new IllegalArgumentException("a fill needs a positive quantity and price");
        }
        long signedQuantity = side == Side.BUY ? quantity : -quantity;

        long newQuantity = openQuantity;
        long newCost = openCost;
        long realized = realizedPnl;

        long opening = signedQuantity;
        if (openQuantity != 0 && Long.signum(openQuantity) != Long.signum(signedQuantity)) {
            long closing = Math.min(quantity, Math.abs(openQuantity));
            long signedClosing = Long.signum(signedQuantity) * closing;
            long costRemoved = costOfClosing(closing);

            // Closing a long sells it: proceeds are positive and cost is subtracted. Closing a
            // short buys it back: the sign of both flips, and one expression covers each case.
            realized += -(signedClosing * price) - costRemoved;
            newQuantity += signedClosing;
            newCost -= costRemoved;
            opening = signedQuantity - signedClosing;
        }

        if (opening != 0) {
            newQuantity += opening;
            newCost += opening * price;
        }
        if (newQuantity == 0) {
            // Any residual left by proportional division belongs to the closed position.
            realized -= newCost;
            newCost = 0;
        }

        return new PositionState(
                newQuantity,
                newCost,
                realized,
                boughtQuantity + (side == Side.BUY ? quantity : 0),
                soldQuantity + (side == Side.SELL ? quantity : 0),
                executionCount + 1);
    }

    /** Empty when flat: an average price for a position of nothing is not a number. */
    public Optional<BigDecimal> averageEntryPrice() {
        if (openQuantity == 0) {
            return Optional.empty();
        }
        return Optional.of(BigDecimal.valueOf(openCost)
                .divide(BigDecimal.valueOf(openQuantity), new MathContext(20, RoundingMode.HALF_UP)));
    }

    /** Exact: no division, so no rounding. */
    public long unrealizedPnl(long markPrice) {
        return markPrice <= 0 ? 0 : openQuantity * markPrice - openCost;
    }

    public long totalPnl(long markPrice) {
        return realizedPnl + unrealizedPnl(markPrice);
    }

    /**
     * The share of the open cost attributable to the part being closed.
     *
     * <p>A full close takes the whole remaining cost, which is what makes a round trip exact. A
     * partial close divides, and the wide intermediate is not caution for its own sake: a large
     * position multiplied by a large close overflows a {@code long} well inside plausible trading
     * sizes, and an overflow here is a wrong number rather than a failure.
     */
    private long costOfClosing(long closing) {
        long absoluteQuantity = Math.abs(openQuantity);
        if (closing == absoluteQuantity) {
            return openCost;
        }
        return BigInteger.valueOf(openCost)
                .multiply(BigInteger.valueOf(closing))
                .divide(BigInteger.valueOf(absoluteQuantity))
                .longValueExact();
    }
}
