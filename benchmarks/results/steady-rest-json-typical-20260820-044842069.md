# Load test: steady / rest-json

## Environment

- Generated: 2026-08-20T04:48:42.069490Z
- JDK: Azul Systems, Inc. 21.0.7
- OS/arch: Mac OS X aarch64
- Available processors: 8
- **This is a developer laptop, not a dedicated benchmark rig.** See docs/benchmark-methodology.md for the full hardware disclosure and why these are directional numbers, not vendor-comparable throughput claims.

## Configuration

| Parameter | Value |
|---|---|
| target | rest-json |
| symbolCount | 16 |
| ordersPerSecond (target) | 50000 |
| cancelRatio | 0.0 |
| matchBandTicks | 200 |
| orderCount | 150 |
| workerCount | 4 |
| queueCapacity | 8192 |
| producerCount | 4 |
| seed | 7921 |

## Results

- Elapsed: 1.306s
- Throughput (accepted+rejected/s): 114.9
- Admitted: 150
- Accepted: 150
- Rejected: 0
- Saturated (queue-full at submit time): 0
- Executions: 102
- Cancels sent: 0

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 806385.63 | 1158483.75 | 1217303.25 | 1227331.13 | 1227331.13 | 150 |

### Queue depth

- Max observed: 0
- Average observed: 0.0
