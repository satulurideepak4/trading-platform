package com.tradingplatform.gateway.pipeline;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One Postgres instance shared by every test that needs the durable portfolio.
 *
 * <p>Static and started once for the same reason the shared Kafka broker is: a fresh container per
 * test class is seconds a suite cannot afford to pay repeatedly. Flyway migrates it once, on the
 * first Spring context that connects, and every test after that reuses the same schema. Tests
 * therefore use their own accounts and symbols rather than asserting on totals, exactly like the
 * Kafka-sharing tests do.
 *
 * <p>Deliberately <b>not</b> annotated with {@code @Container}/{@code @Testcontainers}. Those
 * annotations bind a container's lifecycle to whichever single test class's extension instance
 * manages it, and every subclass sharing this field would register its own instance of that
 * extension — the container gets stopped the moment the first subclass's tests finish, taking it
 * away from every subclass that runs afterward. This is Testcontainers' own documented "singleton
 * container" pattern: start once in a static initializer, register no owning extension, and let
 * Ryuk reap it when the JVM exits.
 */
public abstract class PortfolioTestBase {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("trading")
                    .withUsername("trading")
                    .withPassword("trading");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
