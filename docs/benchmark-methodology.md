# Benchmark methodology

The repository contains two benchmark layers. Their results must never be mixed.

## 1. Deterministic routing benchmark

Command:

```bash
smdg-benchmark \
  --clients 10000 \
  --symbols-per-client 20 \
  --symbol-universe 500 \
  --events-per-symbol 100 \
  --provider-events-per-second 10 \
  --output benchmark-results
```

This benchmark compares the same client-symbol distribution in two modes:

- **Baseline:** every client-symbol relationship is treated as an independent upstream subscription and every provider event is delivered.
- **Smart:** identical upstream subscriptions are aggregated and Basic/Pro/Premium quote frequencies are applied.

Measured values include Python CPU time, wall time, peak traced memory, provider subscription count, provider events, delivered events, and per-event processing duration. It is reproducible through a fixed random seed.

This is an in-process routing benchmark. It does not measure WebSocket network latency, Redis saturation, container CPU limits, exchange latency, or provider rate limits.

## 2. Deployed WebSocket benchmark

Start the stack and run:

```bash
python benchmarks/ws_load.py \
  --url ws://localhost:8000/v1/stream \
  --clients 100 \
  --symbols-per-client 5 \
  --messages 20 \
  --output benchmark-results/network.json
```

The network benchmark creates real WebSocket connections, authenticates clients, subscribes to symbols, receives quote events, and measures provider-timestamp-to-client latency and throughput.

Scale in stages: 100, 500, 1,000, 2,500, 5,000, then 10,000 clients. Stop a stage if error rate exceeds 1%, P95 latency exceeds the declared target, memory grows without a stable bound, or Redis pending entries continue increasing after the load stops.

## Required before/after controls

- Same host type, container limits, Python version, Redis configuration, provider event profile, symbol overlap, client tiers, and test duration.
- One baseline run and at least three optimized runs after warm-up.
- Record raw results, commit SHA, environment, container limits, and exact command.
- Separate measured values from extrapolated node counts and cost estimates.
- Report median and range across repeated runs.

## Real-provider benchmark gate

Before using a real provider, complete `provider-licensing-checklist.md`. Confirm that benchmark publication is allowed, do not expose credentials or restricted payloads, and record whether data is real-time, delayed, or synthetic.

## Node and cost report

A node estimate may be calculated only after measured resource saturation. The report must include:

- maximum stable connections per node;
- CPU and memory saturation point;
- P50/P95/P99 delivery latency;
- ingress and egress bandwidth;
- Redis stream lag and Pub/Sub throughput;
- failure rate and reconnect behavior;
- baseline and optimized node counts under the same workload;
- infrastructure prices and date of collection;
- assumptions, confidence level, and limitations.
