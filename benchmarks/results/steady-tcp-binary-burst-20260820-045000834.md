# Load test: steady / tcp-binary

## Environment

- Generated: 2026-08-20T04:50:00.834528Z
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
| orderCount | 400 |
| workerCount | 4 |
| queueCapacity | 8192 |
| producerCount | 8 |
| seed | 7921 |

## Results

- Elapsed: 0.836s
- Throughput (accepted+rejected/s): 478.5
- Admitted: 400
- Accepted: 400
- Rejected: 0
- Saturated (queue-full at submit time): 0
- Executions: 264
- Cancels sent: 0

### Producer-observed latency (microseconds)

| p50 | p95 | p99 | p99.9 | max | samples |
|---|---|---|---|---|---|
| 305674.92 | 767418.08 | 785066.83 | 790807.54 | 790807.54 | 400 |

### Queue depth

- Max observed: 399
- Average observed: 169.7
