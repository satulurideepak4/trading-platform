package com.tradingplatform.portfolio;

import static com.tradingplatform.domain.Side.BUY;
import static com.tradingplatform.domain.Side.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.domain.Side;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class PositionStateTest {

    @Test
    void buyingOpensALongAtItsCost() {
        PositionState position = PositionState.FLAT.applyFill(BUY, 10, 100);

        assertEquals(10, position.openQuantity());
        assertEquals(1_000, position.openCost());
        assertEquals(0, position.realizedPnl());
        assertEquals(BigDecimal.valueOf(100), position.averageEntryPrice().orElseThrow());
    }

    @Test
    void sellingOpensAShortWithNegativeCost() {
        PositionState position = PositionState.FLAT.applyFill(SELL, 10, 100);

        assertEquals(-10, position.openQuantity());
        // Negative because the position received proceeds rather than paying them.
        assertEquals(-1_000, position.openCost());
        assertEquals(BigDecimal.valueOf(100), position.averageEntryPrice().orElseThrow());
    }

    @Test
    void addingToALongAveragesTheEntryPrice() {
        PositionState position =
                PositionState.FLAT.applyFill(BUY, 10, 100).applyFill(BUY, 10, 120);

        assertEquals(20, position.openQuantity());
        assertEquals(2_200, position.openCost());
        assertEquals(0, position.realizedPnl());
        assertEquals(0, BigDecimal.valueOf(110).compareTo(position.averageEntryPrice().orElseThrow()));
    }

    @Test
    void closingALongRealizesTheDifference() {
        PositionState position =
                PositionState.FLAT.applyFill(BUY, 10, 100).applyFill(SELL, 10, 110);

        assertEquals(0, position.openQuantity());
        assertEquals(0, position.openCost());
        assertEquals(100, position.realizedPnl());
        assertTrue(position.averageEntryPrice().isEmpty());
    }

    @Test
    void closingAShortRealizesTheDifferenceTheOtherWay() {
        PositionState profit =
                PositionState.FLAT.applyFill(SELL, 10, 100).applyFill(BUY, 10, 90);
        PositionState loss =
                PositionState.FLAT.applyFill(SELL, 10, 100).applyFill(BUY, 10, 130);

        assertEquals(100, profit.realizedPnl());
        assertEquals(-300, loss.realizedPnl());
    }

    @Test
    void partiallyClosingLeavesTheRestOpenAtTheSameAverage() {
        PositionState position =
                PositionState.FLAT.applyFill(BUY, 10, 100).applyFill(SELL, 4, 130);

        assertEquals(6, position.openQuantity());
        assertEquals(600, position.openCost());
        assertEquals(120, position.realizedPnl());
        assertEquals(0, BigDecimal.valueOf(100).compareTo(position.averageEntryPrice().orElseThrow()));
    }

    @Test
    void aTradeThroughZeroClosesTheOldPositionAndOpensANewOne() {
        PositionState position =
                PositionState.FLAT.applyFill(BUY, 10, 100).applyFill(SELL, 15, 110);

        assertEquals(-5, position.openQuantity());
        assertEquals(-550, position.openCost());
        // Only the 10 that were held are realized; the extra 5 opened a short at 110.
        assertEquals(100, position.realizedPnl());
        assertEquals(0, position.unrealizedPnl(110));
    }

    @Test
    void unrealizedPnlIsMeasuredAgainstTheMark() {
        PositionState longPosition = PositionState.FLAT.applyFill(BUY, 10, 100);
        PositionState shortPosition = PositionState.FLAT.applyFill(SELL, 10, 100);

        assertEquals(200, longPosition.unrealizedPnl(120));
        assertEquals(-200, longPosition.unrealizedPnl(80));
        assertEquals(-200, shortPosition.unrealizedPnl(120));
        assertEquals(200, shortPosition.unrealizedPnl(80));
    }

    @Test
    void totalPnlCombinesWhatIsBankedAndWhatIsStillOpen() {
        PositionState position = PositionState.FLAT
                .applyFill(BUY, 10, 100)
                .applyFill(SELL, 5, 120); // banks 100

        assertEquals(100, position.realizedPnl());
        assertEquals(100, position.unrealizedPnl(120));
        assertEquals(200, position.totalPnl(120));
    }

    @Test
    void aFlatPositionHasNoUnrealizedPnl() {
        PositionState position =
                PositionState.FLAT.applyFill(BUY, 10, 100).applyFill(SELL, 10, 130);

        assertEquals(0, position.unrealizedPnl(500));
        assertEquals(300, position.totalPnl(500));
    }

    @Test
    void anUnpricedInstrumentContributesNoUnrealizedPnl() {
        PositionState position = PositionState.FLAT.applyFill(BUY, 10, 100);

        // A missing mark must not be read as a price of zero, which would show a total loss.
        assertEquals(0, position.unrealizedPnl(0));
    }

    @Test
    void partialClosesDoNotLoseValueToRounding() {
        // 3 does not divide 1000 evenly, so every partial close has a remainder to carry.
        PositionState position = PositionState.FLAT.applyFill(BUY, 3, 1_000);
        position = position.applyFill(SELL, 1, 1_100);
        position = position.applyFill(SELL, 1, 1_100);
        position = position.applyFill(SELL, 1, 1_100);

        assertEquals(0, position.openQuantity());
        assertEquals(0, position.openCost());
        // Exactly 3 * 100, with nothing lost to the two divisions along the way.
        assertEquals(300, position.realizedPnl());
    }

    @Test
    void aRandomRoundTripRealizesExactlyProceedsMinusCost() {
        Random random = new Random(20260818L);

        for (int run = 0; run < 200; run++) {
            List<long[]> fills = new ArrayList<>();
            long netQuantity = 0;
            PositionState position = PositionState.FLAT;

            for (int step = 0; step < 12; step++) {
                Side side = random.nextBoolean() ? BUY : SELL;
                long quantity = 1 + random.nextInt(97);
                long price = 900 + random.nextInt(400);
                fills.add(new long[] {side == BUY ? quantity : -quantity, price});
                netQuantity += side == BUY ? quantity : -quantity;
                position = position.applyFill(side, quantity, price);
            }

            // Close whatever is left, so the book is flat and all P&L must be realized.
            if (netQuantity != 0) {
                long closingPrice = 900 + random.nextInt(400);
                Side closingSide = netQuantity > 0 ? SELL : BUY;
                long closingQuantity = Math.abs(netQuantity);
                fills.add(new long[] {netQuantity > 0 ? -closingQuantity : closingQuantity, closingPrice});
                position = position.applyFill(closingSide, closingQuantity, closingPrice);
            }

            // Flat, so realized P&L is simply everything received minus everything paid.
            long expected = 0;
            for (long[] fill : fills) {
                expected -= fill[0] * fill[1];
            }

            assertEquals(0, position.openQuantity(), "run " + run + " should end flat");
            assertEquals(0, position.openCost(), "run " + run + " should hold no cost");
            assertEquals(expected, position.realizedPnl(), "run " + run + " realized P&L");
        }
    }

    @Test
    void aLargePositionDoesNotOverflowIntoAWrongNumber() {
        // Big enough that cost * closing exceeds a long, which is where a naive implementation
        // silently produces a wrong price rather than failing.
        long quantity = 3_000_000_000L;
        long price = 1_000_000L;
        PositionState position = PositionState.FLAT.applyFill(BUY, quantity, price);

        PositionState reduced = position.applyFill(SELL, quantity / 3, price);

        assertEquals(quantity - quantity / 3, reduced.openQuantity());
        assertEquals(0, reduced.realizedPnl());
    }
}
