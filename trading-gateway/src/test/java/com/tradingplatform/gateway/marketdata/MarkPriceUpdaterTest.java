package com.tradingplatform.gateway.marketdata;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.tradingplatform.marketdata.event.RawMarketEvent;
import com.tradingplatform.marketdata.process.MarketDataMetrics;
import com.tradingplatform.marketdata.process.MarketDataProcessor;
import com.tradingplatform.portfolio.PortfolioRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MarkPriceUpdaterTest {
    private static final Instant NOW = Instant.parse("2025-01-01T09:30:00Z");

    private final PortfolioRepository repository = mock(PortfolioRepository.class);
    private MarketDataProcessor processor;
    private MarkPriceUpdater updater;

    @BeforeEach
    void setUp() {
        processor = new MarketDataProcessor(
                update -> {}, new MarketDataMetrics(new SimpleMeterRegistry()));
        updater = new MarkPriceUpdater(processor, repository);
    }

    @Test
    void flushesTheMidPriceWhenBothSidesOfTheBookExist() {
        processor.ingest(new RawMarketEvent.BidUpdate("AAPL", 1, 19_000, 10, NOW));
        processor.ingest(new RawMarketEvent.AskUpdate("AAPL", 2, 19_010, 10, NOW));

        updater.flush();

        verify(repository).upsertMarkPrice(eq("AAPL"), eq(19_005L), eq("MARKET_DATA"), any());
    }

    @Test
    void fallsBackToTheLastTradeWhenTheBookIsOneSided() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 19_500, 5, NOW));

        updater.flush();

        verify(repository).upsertMarkPrice(eq("AAPL"), eq(19_500L), eq("MARKET_DATA"), any());
    }

    @Test
    void aSecondFlushWithNoPriceChangeWritesNothingMore() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 19_500, 5, NOW));
        updater.flush();
        updater.flush();

        verify(repository, times(1)).upsertMarkPrice(eq("AAPL"), anyLong(), eq("MARKET_DATA"), any());
    }

    @Test
    void aChangedPriceIsFlushedAgain() {
        processor.ingest(new RawMarketEvent.Trade("AAPL", 1, 19_500, 5, NOW));
        updater.flush();
        processor.ingest(new RawMarketEvent.Trade("AAPL", 2, 19_600, 5, NOW));
        updater.flush();

        verify(repository).upsertMarkPrice(eq("AAPL"), eq(19_500L), eq("MARKET_DATA"), any());
        verify(repository).upsertMarkPrice(eq("AAPL"), eq(19_600L), eq("MARKET_DATA"), any());
    }

    @Test
    void aRepositoryFailureForOneSymbolDoesNotStopOthersFromFlushing() {
        processor.ingest(new RawMarketEvent.Trade("BAD", 1, 100, 1, NOW));
        processor.ingest(new RawMarketEvent.Trade("GOOD", 1, 200, 1, NOW));
        org.mockito.Mockito.doThrow(new RuntimeException("db down"))
                .when(repository)
                .upsertMarkPrice(eq("BAD"), anyLong(), any(), any());

        updater.flush();

        verify(repository).upsertMarkPrice(eq("GOOD"), eq(200L), eq("MARKET_DATA"), any());
    }

    @Test
    void nothingIsFlushedForASymbolWithNoState() {
        updater.flush();

        verify(repository, never()).upsertMarkPrice(any(), anyLong(), any(), any());
    }
}
