# Platform Architecture Overview

## Core Architectural Principles

1. **Authoritative Event Log + Rebuildable Low-Latency Serving State:** Kafka topics (`marketplace.*`) and Apache Iceberg V2 tables hold the durable system of record. Valkey (or the local SQLite WAL fallback) holds strictly ephemeral, version-checked materialized views (`mkt:v1:{supply|demand|pricing}:h3:{cell}`) that can be wiped and rebuilt deterministically from durable changelog topics at any time.
2. **Strict Separation of Serving vs. Analytical Paths:**
   - **Live Serving Path (`serving_tier="live_kv"`):** Serves `/marketplace/demand`, `/marketplace/supply`, `/marketplace/imbalance`, `/marketplace/pricing`, and `/marketplace/pricing/{h3_cell}` in sub-millisecond local p95 latency (`0.070 ms` measured) with safe `1.0x` outage/stale fallbacks.
   - **Historical Analytical Path (`serving_tier="lakehouse_iceberg"`):** Serves `/marketplace/regions`, `/marketplace/trips`, `/marketplace/matching`, `/marketplace/experiments`, and `/platform/data-quality` directly from curated Apache Iceberg V2 Gold and Silver tables via DuckDB / Trino / Spark.
3. **Correct Geospatial State Under Movement:** Every driver belongs to at most one H3 resolution-7 cell at any instant. Cell boundary crossings emit atomic `(-1 old_cell, +1 new_cell)` transitions, and stale sequence numbers are dropped before mutating state.
4. **Bounded Multi-Entity Streaming Joins:** Rider requests (`request_id`), dispatch offers (`request_id`), trip lifecycles (`trip_id`), and pricing policies (`region_id`) are correlated using bounded event-time intervals and out-of-order join stubs so state growth remains finite and conversion funnels remain monotonic.
5. **Privacy by Construction:** Public and dashboard serving routes expose strictly H3-cell and city-tier aggregates; raw individual rider and driver GPS trajectories are never exposed through public APIs.

## End-to-End Component Responsibilities

- **Simulator (`marketplace_platform/simulator.py`):** Generates deterministic multi-city marketplace streams across NYC, San Francisco, and London, explicitly distinguishing virtual participant pools (up to 1,000,000 IDs) from concurrently active drivers and riders.
- **Event Contracts (`marketplace_platform/contracts.py`, `schemas/avro/`):** Enforces Avro wire framing (`0x00` + 4-byte schema ID + schemaless Avro payload) with automated v1↔v2 backward/forward evolution checks and dead-letter quarantine.
- **Stateful Stream Engine (`marketplace_platform/state_engine.py`, `streaming/`):** Maintains authoritative 7-state driver and rider machines, per-partition FIFO K-way event-time merging, per-domain driver watermarks, heartbeat/reservation timers, atomic checkpoints, and explainable H3 pricing computation.
- **Serving Materializer (`marketplace_platform/serving_store.py`):** Applies monotonic compare-and-swap writes (`incoming_version > stored_version`) via Valkey Lua scripts or SQLite WAL transactions.
- **Lakehouse & Reconciliation (`marketplace_platform/lakehouse.py`, `pipelines/`):** Manages 12 Apache Iceberg V2 tables across Bronze, Silver, Gold, and Quality layers, ingests Debezium-style CDC Policy SCD Type 2 history, runs 8 automated invariant checks, compacts small files, and detects/repairs serving-store drift.
- **Control Tower API & UI (`apps/marketplace-api/`, `apps/marketplace-dashboard/`):** Exposes 12 required operational/analytical endpoints, Prometheus `/metrics`, RBAC (`admin`, `operator`, `analyst`, `public_aggregated`), rate limiting, and a 12-view offline-safe H3 hexagonal Control Tower.
