package com.tradingplatform.gateway.tcp;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.tradingplatform.pipeline.publish.NoOpTradingEventPublisher;
import com.tradingplatform.pipeline.publish.TradingEventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Proves the {@code @Primary} wiring in {@code TcpConfiguration} actually resolves the way its own
 * Javadoc claims: with {@code trading.tcp.enabled=true}, the single {@code TradingEventPublisher}
 * the rest of the gateway autowires must be {@link ExecutionPushingTradingEventPublisher}, not the
 * plain delegate — and the context must load at all, which is itself the proof there is no {@code
 * NoUniqueBeanDefinitionException} between the two now-present {@code TradingEventPublisher} beans.
 *
 * <p>Runs against {@code trading.pipeline.enabled=false} (the "test" profile's own default, no
 * Kafka needed) so this stays a plain unit-cost context test. The resolution mechanism this proves
 * — Spring excluding a bean under construction from its own dependency candidates, see {@code
 * TcpConfiguration}'s Javadoc — does not depend on which concrete delegate is present; {@code
 * KafkaPipelineConfiguration}/{@code ExecutionPipelineConfiguration}'s mutual {@code
 * @ConditionalOnProperty} exclusivity is what guarantees exactly one delegate candidate exists
 * either way, which this same test would exercise identically under {@code pipeline.enabled=true}.
 */
@SpringBootTest(properties = {"trading.tcp.enabled=true", "trading.tcp.port=0"})
@ActiveProfiles("test")
class TcpAwareTradingEventPublisherWiringTest {

    @Autowired private TradingEventPublisher publisher;

    @Test
    void theResolvedPublisherIsTheTcpAwareDecoratorWrappingTheNoOpDelegate() {
        ExecutionPushingTradingEventPublisher decorator =
                assertInstanceOf(ExecutionPushingTradingEventPublisher.class, publisher);
        assertInstanceOf(NoOpTradingEventPublisher.class, decorator.delegateForTesting());
    }
}
