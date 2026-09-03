package com.tradingplatform.gateway.marketdata;

import com.tradingplatform.marketdata.process.MarketDataProcessor;
import com.tradingplatform.marketdata.process.SymbolSnapshot;
import com.tradingplatform.portfolio.PortfolioRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Feeds the durable {@code mark_prices} table from the live market-data feed, closing the gap left
 * open at the end of Stage 5: {@code PortfolioUpdater} still marks a fresh position at its own
 * trade price the moment it prints, but from here on an idle position's unrealized P&L moves with
 * the market rather than sitting frozen at whatever it last traded at.
 *
 * <p>Runs on a fixed schedule rather than reacting to every tick. The feed can run at hundreds of
 * events per second across many symbols; a database write per tick would make market-data ingestion
 * rate limited by Postgres for no benefit; nothing reads a mark price faster than this poll interval
 * anyway. A symbol whose reference price has not moved since the last flush is skipped entirely, so
 * a quiet symbol costs nothing beyond one map lookup.
 *
 * <p>Writes only the source {@code MARKET_DATA}; {@code PortfolioUpdater} keeps writing
 * {@code LAST_TRADE} from real executions. {@link PortfolioRepository#upsertMarkPrice} already
 * refuses to let an older {@code asOf} overwrite a newer one regardless of source, so whichever fed
 * the more recent price wins — there is no ordering to enforce between the two here.
 */
public class MarkPriceUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MarkPriceUpdater.class);
    private static final String SOURCE = "MARKET_DATA";

    private final MarketDataProcessor processor;
    private final PortfolioRepository repository;
    private final Map<String, Long> lastFlushed = new ConcurrentHashMap<>();

    public MarkPriceUpdater(MarketDataProcessor processor, PortfolioRepository repository) {
        this.processor = processor;
        this.repository = repository;
    }

    @Scheduled(fixedRateString = "${trading.marketdata.mark-price-flush-interval:1s}")
    public void flush() {
        for (String symbol : processor.symbols()) {
            processor.snapshot(symbol).ifPresent(this::flushOne);
        }
    }

    private void flushOne(SymbolSnapshot snapshot) {
        Long reference = snapshot.referencePrice();
        if (reference == null) {
            return;
        }
        Long previous = lastFlushed.get(snapshot.symbol());
        if (reference.equals(previous)) {
            return;
        }
        try {
            repository.upsertMarkPrice(snapshot.symbol(), reference, SOURCE, snapshot.asOf());
            lastFlushed.put(snapshot.symbol(), reference);
        } catch (RuntimeException failure) {
            // A transient database problem here is not a reason to stop marking other symbols, or
            // to bring down the scheduled task itself; the next flush retries this symbol anyway.
            LOG.warn("event=mark_price_flush_failed symbol={}", snapshot.symbol(), failure);
        }
    }
}
