# Engineering Case Study: Building a Real-Time Mobility Marketplace & Dynamic Pricing Backbone

## Problem Statement

Ride-hailing marketplaces do not fail because they lack dashboards; they fail when **state drifts under concurrency and movement**. Four production failure modes dominate real-time marketplace data engineering:

1. **Double-counting moving supply across spatial cells:** If an available driver drives across a hexagon boundary and the stream processor increments the new cell without atomically decrementing the old cell (or allows a delayed GPS packet to teleport the driver backward), local supply looks artificially healthy and surge pricing fails to recruit drivers.
2. **Phantom active demand from terminal requests:** If cancelled, matched, or expired rider requests remain in active window counters, surge multipliers stay artificially pinned high even after riders leave.
3. **Opaque, oscillating surge pricing:** Unsmoothed formulas jump wildly between `1.0x` and `3.0x` on every tick, eroding rider trust and creating regulatory audit exposure.
4. **Coupled serving and analytics failure:** Writing directly from a stream processor to both Kafka and a KV cache without monotonic versioning creates silent split-brain drift whenever a replay or cache restart occurs.

## Architecture & Key Engineering Outcomes

- **Zero-Double-Counting Geospatial State Engine:** Enforces 7-state driver and rider machines with monotonic sequence guards, atomic `(-1, +1)` H3 cell transfers, K-way per-partition FIFO merging for bit-for-bit checkpoint recovery parity, and per-domain driver watermarks so long trip completions never evict active idle drivers.
- **Explainable, Replayable Dynamic Pricing:** Incorporates 1-ring neighboring supply, six deterministic safety guardrails (missing/stale `1.0x` fallback, exponential smoothing, hysteresis + min update interval, step-delta cap, floor/ceiling bounds), and H3-cluster A/B experimentation gated by a chi-squared Sample Ratio Mismatch check.
- **Dual-Tier Serving & Lakehouse Consistency:** Materializes version-checked H3 views (`0.070 ms` p95 lookup latency) asynchronously from durable changelog topics while retaining full history across 12 Apache Iceberg V2 tables (`-59.97%` storage after compaction) and reconciling live KV drift back to Iceberg Gold automatically.
- **Empirically Grounded Geospatial Tuning:** Multi-resolution H3 benchmarking across Res 6, 7, 8, and 9 demonstrated why Resolution 7 (`5.16 km²`, `195` cells) outperforms Res 9 (`986` cells, `92.7%` zero-supply sparsity, `4.28×` slower materialization) and Res 6 (`74.6%` district-wide surge bleed).
