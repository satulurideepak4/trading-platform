# Built with the repository root as build context (see docker-compose.yml's `build:` block), not
# this directory: the reactor's parent pom and every sibling module trading-gateway depends on have
# to be visible to `mvn -am package`, since it recompiles the modules it depends on from source
# rather than pulling them from a registry that doesn't exist for this project.
#
#   docker compose build trading-gateway
#   docker compose up -d
#
# Two stages so the final image carries only a JRE and one jar, not a JDK, Maven, and every
# module's target/ directory. Nothing here changes what `mvn -pl trading-gateway -am package`
# already does locally — see the README's "Run the trading gateway" section for that path; this is
# the same build, containerised, for Prometheus (which itself runs in a container) to have a
# reachable scrape target without needing the host network.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
COPY trading-domain trading-domain
COPY matching-engine matching-engine
COPY risk-engine risk-engine
COPY market-data market-data
COPY execution-pipeline execution-pipeline
COPY portfolio-service portfolio-service
COPY trading-gateway trading-gateway
COPY exchange-simulator exchange-simulator
# Not a runtime dependency of the gateway - copied only because the root reactor pom declares it
# as a module, so `mvn -pl trading-gateway -am` still needs the directory present to parse the tree.
COPY benchmark benchmark

# Offline-friendly for repeat builds: dependency resolution is its own layer, invalidated only when
# a pom.xml actually changes, not on every source edit.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -pl trading-gateway -am dependency:go-offline

RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -pl trading-gateway -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Runs as a non-root user — this jar never needs to bind a privileged port or touch anything
# outside its own working directory and the journal volume compose mounts for it. /data is where
# docker-compose.yml points MATCHING_JOURNAL_DIR and mounts a named volume, so the matching-engine
# journal survives a container recreation the same way Postgres's own volume does; it has to be
# created and owned by this user now, before the switch below, or the journal directory creation
# at startup (ADR-015) fails with a permission error instead of recovering anything.
RUN addgroup -S trading && adduser -S trading -G trading && mkdir -p /data && chown trading:trading /data
USER trading

# -exec.jar specifically, not trading-gateway-*.jar: the plain jar now sits alongside it (the
# main artifact, unclassified) since benchmark depends on this module's plain classes as of
# Stage 10 - see the spring-boot-maven-plugin execution's own comment in this module's pom.xml.
COPY --from=build /workspace/trading-gateway/target/trading-gateway-*-exec.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
