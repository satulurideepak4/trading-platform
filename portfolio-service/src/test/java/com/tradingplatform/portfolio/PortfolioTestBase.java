package com.tradingplatform.portfolio;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One real Postgres, migrated by Flyway on first use, shared by every test in this module.
 *
 * <p>Deliberately <b>not</b> annotated with {@code @Container}/{@code @Testcontainers}. Those
 * annotations bind a container's lifecycle to whichever single test class's extension instance
 * manages it; every subclass sharing this field would register its own instance of that extension,
 * and the container would be stopped the moment the first subclass's tests finish — taking it away
 * from every subclass that runs afterward. This is Testcontainers' own documented "singleton
 * container" pattern: start once in a static initializer, register no owning extension, and let
 * Ryuk reap it when the JVM exits.
 */
@SpringBootTest(classes = PortfolioTestApplication.class)
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
