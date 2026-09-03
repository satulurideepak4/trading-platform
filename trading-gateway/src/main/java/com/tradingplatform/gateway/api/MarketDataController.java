package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.marketdata.SymbolNotTrackedException;
import com.tradingplatform.marketdata.process.MarketDataProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * A REST fallback onto the same state {@code /marketdata} streams over WebSocket, for anything
 * that would rather poll than hold a connection open — a curl check, a one-off script.
 */
@RestController
@ConditionalOnProperty(name = "trading.marketdata.enabled", havingValue = "true", matchIfMissing = true)
public class MarketDataController {
    private final MarketDataProcessor processor;

    public MarketDataController(MarketDataProcessor processor) {
        this.processor = processor;
    }

    @GetMapping("/marketdata/{symbol}")
    public MarketSnapshotResponse snapshot(@PathVariable String symbol) {
        return processor
                .snapshot(symbol)
                .map(MarketSnapshotResponse::from)
                .orElseThrow(() -> new SymbolNotTrackedException(symbol));
    }
}
