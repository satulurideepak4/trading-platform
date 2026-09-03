package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.auth.AuthenticatedClient;
import com.tradingplatform.risk.PreTradeRiskEngine;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RiskExposureController {
    private final PreTradeRiskEngine riskEngine;

    public RiskExposureController(PreTradeRiskEngine riskEngine) {
        this.riskEngine = riskEngine;
    }

    @GetMapping("/risk/exposure")
    public RiskExposureResponse exposure(AuthenticatedClient client) {
        return RiskExposureResponse.from(riskEngine.snapshot(client.accountId()));
    }
}
