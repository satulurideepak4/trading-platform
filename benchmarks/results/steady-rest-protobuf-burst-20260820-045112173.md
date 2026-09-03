# Load test: steady / rest-protobuf

## Environment

- Generated: 2026-08-20T04:51:12.174868Z
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
| orderCount | 400 |
| workerCount | 4 |
| queueCapacity | 8192 |
| producerCount | 8 |
| seed | 7921 |

## Results

- Elapsed: 2.098s
- Throughput (accepted+rejected/s): 190.7
- Admitted: 400
- Accepted: 400
- Rejected: 0
- Saturated (queue-full at submit time): 0
- Executions: 280
- Cancels sent: 0

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 1001676.29 | 1862093.00 | 1945765.04 | 1961602.42 | 1961602.42 | 400 |

### Queue depth

- Max observed: 0
- Average observed: 0.0
