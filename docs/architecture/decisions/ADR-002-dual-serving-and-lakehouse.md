# ADR-002: Strict Separation of Low-Latency Serving KV from Analytical Iceberg Lakehouse

## Status
Accepted

## Context
Dispatch and rider pricing queries require millisecond point lookups (`h3_cell -> pricing_multiplier`), whereas funnel conversion analytics, experiment SRM evaluation, and historical pricing audits scan millions of records across cities and days. Attempting to serve live dispatch traffic directly from columnar Parquet/Iceberg files violates latency SLOs; using Valkey as the primary analytical store lacks ACID snapshots, SQL joins, and cost-effective retention.

## Decision
Maintain a strict dual-storage architecture:
1. **Durable Kafka / WAL topics + Apache Iceberg V2** act as the single authoritative source of truth.
2. **Valkey 9.2.0 (or local SQLite WAL fallback)** serves strictly materialized H3 operational state fed asynchronously from durable changelog topics via version-aware compare-and-swap writes (`incoming_version > stored_version`).
3. Every FastAPI response explicitly declares `serving_tier` (`"live_kv"`, `"lakehouse_iceberg"`, or `"hybrid"`).

## Consequences
- Zero dual-write distributed transactions between Kafka and Valkey.
- Complete serving store outages degrade safely to `1.0x` baseline pricing and recover deterministically via `materialize_from_durable_log(wipe_first=True)`.
- Scheduled reconciliation (`reconcile_serving_store()`) detects and repairs any drift between Iceberg Gold and live KV state.
