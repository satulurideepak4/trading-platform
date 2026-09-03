# Load test: high-cancel-rate / router

## Environment

- Generated: 2026-08-19T12:20:53.554998Z
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
| cancelRatio | 0.4 |
| matchBandTicks | 200 |
| orderCount | 500000 |
| workerCount | 4 |
| queueCapacity | 8192 |
| producerCount | 4 |
| seed | 7921 |

## Results

- Elapsed: 10.000s
- Throughput (accepted+rejected/s): 49999.6
- Admitted: 500000
- Accepted: 374511
- Rejected: 125489
- Saturated (queue-full at submit time): 0
- Executions: 211652
- Cancels sent: 200221

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 4.33 | 18.75 | 139.96 | 5520.08 | 17544.83 | 500000 |

### Queue depth

- Max observed: 219
- Average observed: 1.5
