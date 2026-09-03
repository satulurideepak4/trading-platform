package com.tradingplatform.marketdata.simulator;

import java.util.SplittableRandom;

/**
 * How often {@link MarketSimulator} deliberately misbehaves, so that {@code MarketDataProcessor}'s
 * duplicate/out-of-order/gap handling is exercised by every run rather than assumed to work.
 *
 * @param duplicateRate probability an event repeats the previous sequence number for its symbol
 * @param outOfOrderRate probability an event is labelled with a sequence number already superseded
 * @param gapRate probability one or more sequence numbers are skipped before this event
 */
public record FeedImperfectionPolicy(double duplicateRate, double outOfOrderRate, double gapRate) {
    public static final FeedImperfectionPolicy NONE = new FeedImperfectionPolicy(0, 0, 0);

    /** A visibly imperfect feed, without drowning the normal case in noise. */
    public static final FeedImperfectionPolicy DEFAULT = new FeedImperfectionPolicy(0.01, 0.01, 0.005);

    public FeedImperfectionPolicy {
        requireProbability(duplicateRate, "duplicateRate");
        requireProbability(outOfOrderRate, "outOfOrderRate");
        requireProbability(gapRate, "gapRate");
    }

    enum Fault {
        NONE,
        DUPLICATE,
        OUT_OF_ORDER,
        GAP
    }

    Fault next(SplittableRandom random) {
        double roll = random.nextDouble();
        if (roll < duplicateRate) {
            return Fault.DUPLICATE;
        }
        roll -= duplicateRate;
        if (roll < outOfOrderRate) {
            return Fault.OUT_OF_ORDER;
        }
        roll -= outOfOrderRate;
        return roll < gapRate ? Fault.GAP : Fault.NONE;
    }

    private static void requireProbability(double value, String name) {
        if (value < 0 || value > 1) {
            throw new IllegalArgumentException(name + " must be within [0,1], was " + value);
        }
    }
}
