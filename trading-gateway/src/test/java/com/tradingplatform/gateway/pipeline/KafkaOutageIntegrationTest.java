package com.tradingplatform.gateway.pipeline;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradingplatform.gateway.api.SubmitOrderRequest;
import com.tradingplatform.portfolio.PortfolioReadModel;
import com.tradingplatform.portfolio.PositionKey;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * What happens to trading when the broker goes away.
 *
 * <p>Uses its own broker, which is paused rather than stopped. Pausing keeps the published port
 * mapping, so the client sees an unreachable broker and then a reachable one again — a restart
 * would move the port and test nothing but Testcontainers.
 *
 * <p>The property being asserted is the one that matters commercially: a broker outage must not
 * stop the venue from matching. Orders keep being accepted, and the downstream catches up
 * afterwards.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("pipeline-test")
class KafkaOutageIntegrationTest extends PortfolioTestBase {

    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired private MockMvc mockMvc;
    @Autowired private PortfolioReadModel portfolio;

    @Test
    void tradingContinuesWhileTheBrokerIsDownAndTheBacklogDrainsAfterwards() throws Exception {
        mockMvc.perform(order("key-one", "outage-warmup-sell", "OUTAGE", "SELL", 10, 15_000))
                .andExpect(status().isCreated());
        mockMvc.perform(order("key-two", "outage-warmup-buy", "OUTAGE", "BUY", 1, 15_000))
                .andExpect(status().isCreated());
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertEquals(1, netPosition("ACC-2", "OUTAGE")));

        pauseBroker();
        long ingressNanos;
        try {
            // Long enough that the producer has certainly failed to reach a broker and is retrying,
            // rather than merely having a record sitting in its accumulator.
            Thread.sleep(3_000);

            // The matching engine has no dependency on Kafka and the publisher buffers rather than
            // blocking, so ingress must be unaffected. This is the claim the whole buffered-
            // publisher design exists to make, so it is measured rather than assumed.
            long startedAt = System.nanoTime();
            for (int index = 0; index < 5; index++) {
                mockMvc.perform(order("key-two", "outage-" + index, "OUTAGE", "BUY", 1, 15_000))
                        .andExpect(status().isCreated());
            }
            ingressNanos = System.nanoTime() - startedAt;
        } finally {
            unpauseBroker();
        }

        Duration ingress = Duration.ofNanos(ingressNanos);
        assertTrue(
                ingress.compareTo(Duration.ofSeconds(2)) < 0,
                "orders should still be accepted promptly with the broker down, took " + ingress);

        // Buffered events are delivered once the broker is reachable again.
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertEquals(6, netPosition("ACC-2", "OUTAGE")));
        assertTrue(netPosition("ACC-1", "OUTAGE") == -6);
    }

    private long netPosition(String accountId, String symbol) {
        return portfolio
                .position(new PositionKey(accountId, SubmitOrderRequest.DEFAULT_STRATEGY, symbol))
                .map(PortfolioReadModel.PositionView::netQuantity)
                .orElse(0L);
    }

    private static void pauseBroker() {
        DockerClientFactory.instance()
                .client()
                .pauseContainerCmd(KAFKA.getContainerId())
                .exec();
    }

    private static void unpauseBroker() {
        DockerClientFactory.instance()
                .client()
                .unpauseContainerCmd(KAFKA.getContainerId())
                .exec();
    }

    private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder order(
            String apiKey, String clientOrderId, String symbol, String side, long qty, long price) {
        String body = """
                {"clientOrderId":"%s","symbol":"%s","side":"%s","type":"LIMIT",\
                "quantity":%d,"price":%d}
                """.formatted(clientOrderId, symbol, side, qty, price);
        return post("/orders")
                .header("X-Api-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }
}
