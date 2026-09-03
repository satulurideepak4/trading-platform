package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.ingress.InstrumentNotFoundException;
import com.tradingplatform.portfolio.PortfolioReadModel;
import com.tradingplatform.risk.InstrumentRegistry;
import jakarta.validation.Valid;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Manual mark-price ingestion.
 *
 * <p>The last traded price already marks a position the moment its instrument trades — see
 * {@code PortfolioUpdater}. This endpoint exists for the gap that leaves: an instrument that has
 * gone quiet still needs a current price for unrealized P&L to mean anything, and until Stage 6
 * brings a real market-data feed, this is how one gets in. It is a deliberate placeholder, not a
 * pricing service, and it trusts the caller completely.
 *
 * <p>Same condition as {@link PortfolioController}, for the same reason: with no durable portfolio
 * there is nothing for a mark price to feed into.
 */
@RestController
@ConditionalOnProperty(name = "trading.pipeline.enabled", havingValue = "true", matchIfMissing = true)
public class MarkPriceController {
    private final PortfolioReadModel portfolio;
    private final InstrumentRegistry instruments;
    private final Clock clock;

    public MarkPriceController(
            PortfolioReadModel portfolio, InstrumentRegistry instruments, Clock clock) {
        this.portfolio = portfolio;
        this.instruments = instruments;
        this.clock = clock;
    }

    @PutMapping("/instruments/{symbol}/mark-price")
    public ResponseEntity<Void> setMarkPrice(
            @PathVariable String symbol, @Valid @RequestBody MarkPriceRequest request) {
        if (!instruments.isTradable(symbol)) {
            throw new InstrumentNotFoundException(symbol);
        }
        portfolio.recordMarkPrice(symbol, request.price(), "MANUAL", clock.instant());
        return ResponseEntity.noContent().build();
    }
}
