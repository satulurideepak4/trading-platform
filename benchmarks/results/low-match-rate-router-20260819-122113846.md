# Load test: low-match-rate / router

## Environment

- Generated: 2026-08-19T12:21:13.846785Z
- JDK: Azul Systems, Inc. 21.0.7
- OS/arch: Mac OS X aarch64
- Available processors: 8
- **This is a developer laptop, not a dedicated benchmark rig.** See docs/benchmark-methodology.md for the full hardware disclosure and why these are directional numbers, not vendor-comparable throughput claims.

## Configuration

| Parameter | Value |
|---|---|
| target | router |
| symbolCount | 16 |
| ordersPerSecond (target) | 50000 |
| cancelRatio | 0.0 |
| matchBandTicks | 2000 |
| orderCount | 500000 |
| workerCount | 4 |
| queueCapacity | 8192 |
| producerCount | 4 |
| seed | 7921 |

## Results

- Elapsed: 10.000s
- Throughput (accepted+rejected/s): 49999.7
- Admitted: 500000
- Accepted: 500000
- Rejected: 0
- Saturated (queue-full at submit time): 0
- Executions: 387185
- Cancels sent: 0

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 4.63 | 16.71 | 118.79 | 2457.92 | 21812.67 | 500000 |

### Queue depth

- Max observed: 75
- Average observed: 1.0
