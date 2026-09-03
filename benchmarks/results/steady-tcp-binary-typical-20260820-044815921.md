# Load test: steady / tcp-binary

## Environment

- Generated: 2026-08-20T04:48:15.922186Z
- JDK: Azul Systems, Inc. 21.0.7
- OS/arch: Mac OS X aarch64
- Available processors: 8
- **This is a developer laptop, not a dedicated benchmark rig.** See docs/benchmark-methodology.md for the full hardware disclosure and why these are directional numbers, not vendor-comparable throughput claims.

## Configuration

| Parameter | Value |
|---|---|
| target | tcp-binary |
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

- Elapsed: 0.284s
- Throughput (accepted+rejected/s): 528.9
- Admitted: 150
- Accepted: 150
- Rejected: 0
- Saturated (queue-full at submit time): 0
- Executions: 94
- Cancels sent: 0

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 120017.21 | 256890.50 | 262560.54 | 263702.17 | 263702.17 | 150 |

### Queue depth

- Max observed: 150
- Average observed: 65.8
