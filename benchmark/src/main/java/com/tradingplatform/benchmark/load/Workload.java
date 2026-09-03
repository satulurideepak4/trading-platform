package com.tradingplatform.benchmark.load;

/**
 * One named traffic shape, each differing from {@link #steady()} by exactly one parameter — change
 * one variable at a time, the same experimental discipline the optimization experiments in
 * {@code docs/performance-engineering.md} follow. Together these are the master prompt's Stage 9
 * workload list, minus "slow downstream consumer": that one needs a real Kafka consumer to slow
 * down, which doesn't exist in this in-process harness, and is run separately against the
 * docker-compose stack instead — see docs/benchmark-methodology.md.
 *
 * @param name used in report filenames and titles
 * @param symbolCount how many distinct symbols orders are spread across; 1 is "single hot symbol"
 * @param ordersPerSecond target admission rate; 0 means unthrottled ("burst")
 * @param cancelRatio fraction of admitted traffic that cancels a previously-submitted own order
 *     instead of submitting a new one
 * @param matchBandTicks width of the band prices are drawn from around a fixed per-symbol
 *     reference price; narrow means BUY and SELL prices usually overlap (high match rate), wide
 *     means they usually don't (low match rate)
 * @param orderCount total commands to generate
 * @param workerCount workers/shards for whichever {@link SubmissionTarget} is under test
 * @param queueCapacity per-shard admission queue capacity, where the target has one
 */
public record Workload(
        String name,
        int symbolCount,
        long ordersPerSecond,
        double cancelRatio,
        long matchBandTicks,
        int orderCount,
        int workerCount,
        int queueCapacity,
        long seed) {

    private static final int BASELINE_SYMBOLS = 16;
    private static final long BASELINE_RATE = 50_000;
    private static final long BASELINE_MATCH_BAND = 200;
    private static final int BASELINE_ORDER_COUNT = 500_000;
    private static final int BASELINE_WORKERS = 4;
    private static final int BASELINE_QUEUE_CAPACITY = 8_192;
    private static final long BASELINE_SEED = 7_921;

    public static Workload steady() {
        return new Workload(
                "steady", BASELINE_SYMBOLS, BASELINE_RATE, 0.0, BASELINE_MATCH_BAND,
                BASELINE_ORDER_COUNT, BASELINE_WORKERS, BASELINE_QUEUE_CAPACITY, BASELINE_SEED);
    }

    public static Workload burst() {
        return steady().withNameAndRate("burst", 0);
    }

    public static Workload singleHotSymbol() {
        return steady().withNameAndSymbols("single-hot-symbol", 1);
    }

    public static Workload manySymbols() {
        return steady().withNameAndSymbols("many-symbols", 64);
    }

    public static Workload highCancelRate() {
        return steady().withNameAndCancelRatio("high-cancel-rate", 0.4);
    }

    public static Workload highMatchRate() {
        return steady().withNameAndMatchBand("high-match-rate", 2);
    }

    public static Workload lowMatchRate() {
        return steady().withNameAndMatchBand("low-match-rate", 2_000);
    }

    public static Workload[] named() {
        return new Workload[] {
            steady(), burst(), singleHotSymbol(), manySymbols(),
            highCancelRate(), highMatchRate(), lowMatchRate()
        };
    }

    private Workload withNameAndRate(String newName, long newRate) {
        return new Workload(
                newName, symbolCount, newRate, cancelRatio, matchBandTicks, orderCount, workerCount,
                queueCapacity, seed);
    }

    private Workload withNameAndSymbols(String newName, int newSymbolCount) {
        return new Workload(
                newName, newSymbolCount, ordersPerSecond, cancelRatio, matchBandTicks, orderCount,
                workerCount, queueCapacity, seed);
    }

    private Workload withNameAndCancelRatio(String newName, double newCancelRatio) {
        return new Workload(
                newName, symbolCount, ordersPerSecond, newCancelRatio, matchBandTicks, orderCount,
                workerCount, queueCapacity, seed);
    }

    private Workload withNameAndMatchBand(String newName, long newMatchBandTicks) {
        return new Workload(
                newName, symbolCount, ordersPerSecond, cancelRatio, newMatchBandTicks, orderCount,
                workerCount, queueCapacity, seed);
    }

    public static Workload byName(String name) {
        for (Workload workload : named()) {
            if (workload.name().equals(name)) {
                return workload;
            }
        }
        throw new IllegalArgumentException("unknown workload: " + name);
    }
}
