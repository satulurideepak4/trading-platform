# Load test: steady / rest-protobuf

## Environment

- Generated: 2026-08-20T04:49:21.279611Z
- JDK: Azul Systems, Inc. 21.0.7
- OS/arch: Mac OS X aarch64
- Available processors: 8
- **This is a developer laptop, not a dedicated benchmark rig.** See docs/benchmark-methodology.md for the full hardware disclosure and why these are directional numbers, not vendor-comparable throughput claims.

## Configuration

| Parameter | Value |
|---|---|
| target | rest-protobuf |
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

- Elapsed: 0.673s
- Throughput (accepted+rejected/s): 222.8
- Admitted: 150
- Accepted: 150
- Rejected: 0
- Saturated (queue-full at submit time): 0
- Executions: 103
- Cancels sent: 0

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 464372.96 | 562591.58 | 584099.50 | 591716.13 | 591716.13 | 150 |

### Queue depth

- Max observed: 0
- Average observed: 0.0
