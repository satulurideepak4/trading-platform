# Observability

Micrometer, structured logging and correlation IDs, and health checks were already in place from
earlier stages (see below for what and where). What Stage 8 actually added: a Prometheus-scrapable
endpoint, the metrics the matching layer wasn't yet publishing, and the Docker Compose stack —
Prometheus, Grafana, and the gateway's own container — needed to see any of it outside a single JVM.

## What already existed

* **Structured logging**: `trading-gateway/src/main/resources/logback-spring.xml` — key=value
  console output with `correlationId`/`accountId` from the MDC on every line, greppable without a
  JSON parser.
* **Correlation IDs**: `CorrelationId` — an `X-Correlation-Id` header, generated if the caller
  doesn't supply one, carried across the async handoff to a matching worker (see
  `OrderIngressService`'s own Javadoc on why settlement runs on the worker thread).
* **Health checks**: Spring Boot Actuator, `management.endpoint.health.probes.enabled: true` —
  liveness and readiness, the latter gated on `ReplayReadiness` so the gateway reports itself
  not-ready until a restart's Kafka replay has caught up (Stage 4).
* **Micrometer itself**: used throughout before this stage touched anything — `ProcessingMetrics`
  (execution-pipeline), `MarketDataMetrics` (market-data), `BufferedKafkaEventPublisher`'s
  counters/timer/gauge, and the Kafka client's own producer/consumer metrics via
  `MicrometerProducerListener`/`MicrometerConsumerListener` in `KafkaPipelineConfiguration`.

## What Stage 8 added

**A Prometheus registry and endpoint.** Before this stage, the only `MeterRegistry` in the context
was Spring Boot's fallback `SimpleMeterRegistry` — every meter above was being recorded into
memory with nothing able to scrape it. `micrometer-registry-prometheus` plus
`management.endpoints.web.exposure.include: ...,prometheus` is what makes `/actuator/prometheus`
exist at all.

**The matching layer's own throughput, latency and queue numbers**, which weren't bridged into
Micrometer before. `matching-engine` stays free of Spring/Micrometer, the same Stage 1 invariant
Stage 7's journal preserved — the bridge lives in `trading-gateway`, at the one place that already
sees every command's `RoutedResult` and `CommandResult`: `OrderIngressService`.

* `com.tradingplatform.gateway.metrics.GatewayMetrics` — per-command timers/counters, called from
  `OrderIngressService.awaitRouted`/`applySubmissionOutcome`/`settle` and the two risk-rejection
  sites. `Timer.record(long, TimeUnit)` accepts an already-measured duration
  (`RoutedResult.processingNanos()`/`queueWaitNanos()`), which is what makes real percentile
  histograms possible without wrapping the matching call itself.
* `com.tradingplatform.gateway.metrics.RouterMetricsBinder` — per-worker `Gauge`/`FunctionCounter`s
  reading live from `OrderRouter.metrics()`, registered once at startup
  (`RouterMetricsConfiguration`). Pull-based: nothing polls on a schedule, Micrometer calls the
  supplier at scrape time.

**Gateway HTTP latency** was not reimplemented — Spring Boot already auto-instruments
`http.server.requests`; the only change was enabling percentile histograms for it
(`management.metrics.distribution.percentiles-histogram.http.server.requests: true`).

## Metrics catalog

Every `trading.*` meter this stage added or that already existed and is now reachable. All are
tagged `application=trading-gateway` (a common tag, so a query never has to assume this is the only
job Prometheus scrapes).

| Metric | Type | Key tags | Answers |
| --- | --- | --- | --- |
| `trading_matching_latency_seconds` | Timer (p50/95/99/99.9) | `operation` | Matching latency |
| `trading_matching_queue_wait_seconds` | Timer (p50/95/99/99.9) | `operation` | Queueing delay ahead of matching latency |
| `trading_matching_queue_depth` | Gauge | `worker` | Are queues backing up; which worker |
| `trading_matching_queue_capacity` | Gauge | `worker` | Headroom left before saturation |
| `trading_matching_admitted_total` | FunctionCounter | `worker` | Orders received, at the matching layer |
| `trading_matching_processed_total` | FunctionCounter | `worker` | Orders matched (processed) |
| `trading_matching_failed_total` | FunctionCounter | `worker` | Commands the engine threw on |
| `trading_matching_saturated_total` | FunctionCounter | `worker` | Queue saturation; which worker/symbol |
| `trading_orders_accepted_total` | Counter | `operation`, `symbol` | Acceptance rate, per symbol |
| `trading_orders_rejected_total` | Counter | `operation`, `stage`, `reason` | Rejections/sec, and why |
| `trading_executions_recorded_total` | Counter | `symbol` | Executions/sec, per symbol |
| `http_server_requests_seconds` | Timer (p50/95/99, auto) | `uri`, `status` | Gateway latency |
| `trading_events_published_total` / `_dropped_total` / `_publish_failed_total` | Counter | — | Kafka publish outcomes |
| `trading_events_publish_latency_seconds` | Timer (p50/95/99/99.9) | — | Kafka producer latency |
| `trading_events_queue_depth` | Gauge | — | Publish buffer backlog |
| `trading_consumer_processing_seconds` | Timer (p50/95/99/99.9) | `consumer` | Position/execution/audit/risk consumer latency |
| `trading_consumer_applied_total` / `_duplicates_total` / `_invalid_total` | Counter | `consumer` | Consumer throughput and redelivery rate |
| `trading_consumer_dead_lettered_total` | Counter | — | Poison messages given up on |
| `kafka_consumer_fetch_manager_records_lag_max` | Gauge (Kafka client) | `client_id` | Is Kafka falling behind |
| `trading_marketdata_received_total` / `_duplicate_total` / `_out_of_order_total` / `_gap_total` | Counter | — | Market-data feed quality/rate |
| `jvm_memory_used_bytes` / `_max_bytes` | Gauge (Micrometer JVM) | `area` | Heap pressure |
| `jvm_gc_pause_seconds` | Timer (Micrometer JVM) | `action`, `cause` | Is GC affecting latency |
| `jvm_threads_live_threads`, `process_cpu_usage` | Gauge (Micrometer JVM/system) | — | Thread counts, CPU |

## Dashboards

Three, provisioned automatically (`docker/grafana/provisioning/`, `docker/grafana/dashboards/`) —
`docker compose up -d` is enough to reproduce them, no manual Grafana setup. Deliberately three, not
one per metric: each maps to two or more of the questions this stage was asked to answer, rather
than being organized around where a metric happens to live.

| Dashboard | Answers |
| --- | --- |
| **Overview** | Is the system healthy? Request/error rate, actuator up, JVM heap/CPU/threads, GC pause. |
| **Trading & Matching** | Where is latency increasing? Are queues backing up? Are orders rejected unusually? Is one symbol/partition overloaded? |
| **Kafka & Pipeline** | Is Kafka falling behind? Publish/consumer latency, lag, dropped/dead-lettered events, market-data feed health. |

## Alert rules

`docker/prometheus/alert-rules.yml` — eight rules, each the automated form of a row in
[the failure matrix](failure-matrix.md)'s "Operational alert" column, not a separate invention:
`GatewayDown`, `HighOrderRejectionRate`, `MatchingQueueSaturating`, `KafkaConsumerLagHigh`,
`KafkaEventsDropped`, `EventsPublishFailing`, `EventsDeadLettered`, `JvmHeapNearLimit`.
`EventsDeadLettered` is what closes the "DLQ alerting is Stage 8" note `docs/kafka-design.md` and
the README carried forward from Stage 4 — nothing consumes the DLQ automatically even now, but a
dead-lettered record is no longer silent. `EventsPublishFailing` (failure matrix row 15, added
during a Stage 9 verification pass) is the same idea applied to a gap that had no alert at all: a
single event's `send()` failing outright, broker otherwise healthy — permanent, silent event loss
until this alert existed to surface it. Prometheus evaluates them and exposes their state at
`http://localhost:9090/alerts`. **No Alertmanager is wired up** — these are rule definitions, not a
paging pipeline; wiring notification routing is future work, not something this stack pretends to
have.

## Running it

```bash
cp .env.example .env
docker compose up -d --build
```

Brings up Postgres, Kafka, the gateway (now containerised — see `trading-gateway/Dockerfile`),
Prometheus (`localhost:9090`) and Grafana (`localhost:3000`, `admin`/`admin`) together. Submit a few
orders (`README.md`'s "Run the trading gateway" section) and the Trading & Matching dashboard should
show live, moving panels within a scrape interval or two.

Running the gateway from the JVM directly (the non-Docker path the rest of the README uses) still
works exactly as before; point `docker compose up -d postgres kafka prometheus grafana` at it and
edit `docker/prometheus/prometheus.yml`'s target to `host.docker.internal:8080` if you want
Prometheus to scrape a JVM-run instance instead of the containerised one.

## A naming gotcha worth documenting

`trading_executions_recorded_total` is not called `trading_executions_created_total`, even though
"created" is the more natural word for what it counts. It was originally named
`trading.executions.created`; scraped, it came out as `trading_executions_total` with the word
"created" silently gone. Micrometer's Prometheus naming convention treats a `Counter` name ending in
`created` as colliding with OpenMetrics' own reserved per-counter `_created` timestamp series and
strips it before appending `_total`. Found by actually scraping `/actuator/prometheus` against a
running stack and noticing the metric a Grafana panel expected wasn't there — not by reading
Micrometer's source first. `GatewayMetricsTest.executionsMetricSurvivesPrometheusNamingWithoutCollision`
locks this in against a real `PrometheusMeterRegistry`, not the `SimpleMeterRegistry` every other
test in that class uses, because a `SimpleMeterRegistry` has no naming convention to get wrong.

## A testing gotcha worth documenting

`PrometheusExpositionTest` needs `@AutoConfigureObservability` on the test class, not just
`@SpringBootTest`. Spring Boot's test support disables metrics (and tracing) export by default for
every `@SpringBootTest`, so a unit test never accidentally starts a real export pipeline — without
this annotation, the injected `MeterRegistry` is silently a plain `SimpleMeterRegistry` and
`/actuator/prometheus` is unmapped, a `500` through the static-resource fallback handler rather than
an obviously metrics-shaped error. The older `@AutoConfigureMetrics` annotation that did this in
Spring Boot 2.x no longer exists in this Spring Boot version; found by running the test and reading
the condition-evaluation report, not by inspection.

## Known limitations

* No Alertmanager, no notification routing — alert rules exist and evaluate, nothing pages anyone.
* Per-subscription market-data drop counts (`Subscription.droppedCount()`) still aren't a registered
  metric — noted already in `docs/market-data.md`, unchanged by this stage.
* Prometheus and Grafana have no auth beyond Grafana's own local admin login, and that login is a
  hardcoded local-only default (`docker-compose.yml`) — never expose this compose stack beyond a
  developer machine as configured.
* Percentile histograms (`publishPercentiles`) are computed client-side, in-process, over a sliding
  window Micrometer manages internally — not exact percentiles aggregated centrally the way a
  server-side histogram-with-`histogram_quantile()` approach would give. Accurate enough for
  operational use, not a claim of mathematical precision across instances (there is only one
  instance today; this would matter more with several behind a load balancer).
* No push-based export path (a Prometheus push gateway, a StatsD sink) is configured — the pull
  model matches how every other piece of local infrastructure in this repo already runs, and
  nothing today needs the alternative.
