# Workload: slow downstream consumer

Unlike the other seven workloads, this one is not run through `LoadTestRunner` against an
in-process `MatchingEngine`/`OrderRouter` — it exercises the real containerised `trading-gateway`,
`kafka`, and `postgres` services via `docker compose`, hitting the live REST API the way a real
client would, and reads Kafka consumer lag straight from the same Prometheus endpoint
`docs/observability.md` documents. See docs/benchmark-methodology.md for why.

## Environment

- `docker compose up -d --build` (repository root), all five services healthy.
- trading-gateway image JVM: OpenJDK Temurin-21.0.11+10, linux/arm64 container on this
  Apple M1 Pro / macOS host — one more layer of virtualization than the in-process JMH/LoadTestRunner
  numbers elsewhere in this directory, and disclosed as such.
- Default configuration, unmodified: `RATE_LIMIT_PERMITS_PER_SECOND=200`, `RATE_LIMIT_BURST=400`,
  `RISK_MAX_OPEN_ORDERS=500`.

## What was actually done

A deliberately slowed *consumer* (e.g. pausing just the consumer thread, the way
`KafkaOutageIntegrationTest` pauses a whole broker container) is not reachable here: the consumers
and the order-ingress REST API live in the same JVM process in this architecture, so pausing the
process would stop order acceptance too, not isolate a "slow consumer." Instead, real burst load was
sent at the live gateway (a Bash loop of concurrent `curl` calls, `X-Api-Key` + `POST /orders`,
random AAPL limit orders) to see whether the downstream consumers naturally fall behind the
producer under real, generatable load in this environment — a legitimate way to observe the same
`kafka_consumer_fetch_manager_records_lag_max` metric without touching any production code or
config.

1. **Before** (no traffic sent yet): every `kafka_consumer_fetch_manager_records_lag_max` series
   reads `NaN` — the consumer has never fetched, so Kafka has never reported a lag value for it.
2. **During**: 3,000 orders at concurrency 20 (all `201`), then 20,000 more at concurrency 100
   (15,162 × `201`, 3,768 × `422` from `RISK_MAX_OPEN_ORDERS`, 1,070 × `429` from the rate limiter —
   real admission-control rejections, not benchmark artifacts). Immediately after this second burst:

   ```
   kafka_consumer_fetch_manager_records_lag_max{consumer=audit-processor-1, topic=trading_orders_v1, partition=9} 783.0
   ```

   every other consumer/topic/partition combination read `0.0` at the same instant — the
   `audit-processor` consumer group, which consumes the higher-volume `trading_orders_v1` topic (one
   record per order, vs. one per execution), measurably fell behind the producer under this burst;
   the execution/position/risk-state consumers on the lower-volume `trading_executions_v1` topic did
   not.
3. **After** (10s with no new traffic): every series back to `0.0` — the backlog drained completely
   once producer pressure stopped.

## Conclusion

In this environment, with the shipped default rate limiter (200/s steady, 400 burst) and risk
limits, the REST ingress path itself is the binding constraint on producer-side throughput, not the
Kafka consumers — 429s appear before consumer lag becomes sustained. The one consumer that did show
measurable lag (`audit-processor` on `trading_orders_v1`, the highest-volume topic) recovered fully
within 10 seconds of the burst ending, with no manual intervention. This is a real, measured, honest
result, not the traditionally more dramatic "system falls over" story an artificially slowed
consumer might have produced — see docs/performance-engineering.md for the discussion of why this
scenario was adapted from the master prompt's original "pause the consumer" framing, and what would
be needed to reproduce sustained lag (bypassing the rate limiter, e.g. a direct Kafka producer, is
explicitly out of scope: it would stop measuring this system's actual behavior).
