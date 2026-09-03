package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.auth.AuthenticatedClient;
import com.tradingplatform.portfolio.PortfolioReadModel;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read side of the durable portfolio: positions, trade history and P&L for the calling account.
 *
 * <p>Every endpoint here is scoped to {@code client.accountId()} from the authenticated caller —
 * there is no path parameter for account id, so one account can never read another's book by
 * changing a URL. This mirrors the same choice {@code /orders/{orderId}} makes in ADR terms: the
 * only account whose portfolio a request can see is the one the request authenticated as.
 *
 * <p>All three read from Postgres through {@link PortfolioReadModel}, which is the durable
 * projection built by {@code PortfolioProcessor} from the execution stream — see
 * docs/position-calculation.md. It can lag the synchronous risk view by however long the Kafka
 * round trip takes; {@code /risk/exposure} is the place to look for the state a new order would
 * actually be checked against right now.
 *
 * <p>Matches {@code PortfolioConfiguration}'s own condition rather than depending on
 * {@code PortfolioReadModel} being registered first: with the pipeline switched off there is no
 * durable projection to read, and these endpoints must not exist rather than exist and fail every
 * request. {@code @ConditionalOnBean} would be the more obvious spelling of that, but component-
 * scanned classes have no guaranteed ordering against an {@code @Import}ed configuration's beans,
 * so the same property condition is used directly instead.
 */
@RestController
@Validated
@ConditionalOnProperty(name = "trading.pipeline.enabled", havingValue = "true", matchIfMissing = true)
public class PortfolioController {
    private final PortfolioReadModel portfolio;

    public PortfolioController(PortfolioReadModel portfolio) {
        this.portfolio = portfolio;
    }

    @GetMapping("/positions")
    public List<PositionResponse> positions(AuthenticatedClient client) {
        return portfolio.positionsFor(client.accountId()).stream()
                .map(PositionResponse::from)
                .toList();
    }

    @GetMapping("/executions")
    public List<TradeResponse> executions(
            AuthenticatedClient client,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit,
            @RequestParam(defaultValue = "0") @PositiveOrZero int offset) {
        return portfolio.executionsFor(client.accountId(), limit, offset).stream()
                .map(execution -> TradeResponse.forAccount(client.accountId(), execution))
                .toList();
    }

    @GetMapping("/pnl")
    public ProfitAndLossResponse profitAndLoss(AuthenticatedClient client) {
        return ProfitAndLossResponse.from(portfolio.profitAndLoss(client.accountId()));
    }
}
