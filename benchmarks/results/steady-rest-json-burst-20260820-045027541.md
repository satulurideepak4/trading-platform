# Load test: steady / rest-json

## Environment

- Generated: 2026-08-20T04:50:27.541858Z
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
| orderCount | 400 |
| workerCount | 4 |
| queueCapacity | 8192 |
| producerCount | 8 |
| seed | 7921 |

## Results

- Elapsed: 2.223s
- Throughput (accepted+rejected/s): 180.0
- Admitted: 400
- Accepted: 400
- Rejected: 0
- Saturated (queue-full at submit time): 0
- Executions: 273
- Cancels sent: 0

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 1415680.50 | 2023333.08 | 2074316.38 | 2150274.00 | 2150274.00 | 400 |

### Queue depth

- Max observed: 0
- Average observed: 0.0
