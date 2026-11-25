# Technical Appendix — Ablations, Engineering Postmortems & Verification Matrix

This appendix holds extended experimental breakdowns, negative optimization results, real distributed-systems defects uncovered during verification, and the complete Acceptance Criteria traceability matrix so that [`README.md`](README.md) remains concise.

---

## 1. Optimization Ablations: What Worked vs. What Did Not

All figures below come directly from [`results/benchmarks/iceberg_optimizations.json`](results/benchmarks/iceberg_optimizations.json) and [`results/benchmarks/h3_resolution_tradeoffs.json`](results/benchmarks/h3_resolution_tradeoffs.json) measured on Apple M4 Pro (`local_native` profile, Python 3.12.8).

### What Worked

1. **Apache Iceberg V2 Small-File Compaction (`bronze.raw_events`, 124,146 rows across 6 micro-batches):**
   - **Data files:** `6 -> 1`
   - **On-disk Parquet bytes:** `6,106,482 B -> 2,444,160 B` (**-59.97%**, `2.50×` smaller vs. uncompacted)
   - **Count-scan latency:** `16.34 ms -> 12.74 ms` (**-22.0%**, `1.28×` faster vs. uncompacted)
   - **Compaction overhead:** `62.18 ms`, preserving `124,146/124,146` rows.
2. **H3 Resolution 7 Selection over Resolution 8 and Resolution 9 (identical 20,696-event workload):**
   - **Active cell cardinality:** Res 7 tracks `195` cells vs. `449` at Res 8 (`2.30×`) and `986` at Res 9 (`5.06×`).
   - **Signal density:** Zero-supply sparse cells drop from `92.70%` at Res 9 and `85.08%` at Res 8 to `73.33%` at Res 7, while avoiding the district-wide surge bleed seen at Res 6 (`74.58%` of all cells surging at avg `1.950x`).
   - **KV materialization latency:** `108.48 ms` at Res 7 vs. `232.81 ms` at Res 8 (`2.15×` slower) and `463.78 ms` at Res 9 (`4.28×` slower).

### What Did Not Speed Up (Honest Negative Results)

1. **8-Way Skew Salting on Single-Node DuckDB (`silver.fact_pricing_decision`, 195 H3 cells):**
   - **Unsalted aggregation:** `10.95 ms`
   - **8-way salted two-stage aggregation:** `11.62 ms` (**+6.1% slower** vs. unsalted on single-node DuckDB, with exact value parity verified within `1e-5`).
   - **Why:** On a single-node in-process engine scanning fewer than 1,000 distinct H3 keys, hashing and executing a two-stage `GROUP BY` adds pure CPU overhead without network-shuffle relief. Skew salting is preserved in `pipelines/spark/marketplace_batch_and_reconciliation.py` where multi-executor Spark shuffles encounter severe airport/stadium partition skew.
2. **City Predicate Filter on Single-File Unpartitioned In-Memory Arrow Scan (`bronze.raw_events`, 124,146 rows):**
   - **Full 3-city grouped scan:** `14.17 ms` (`51` groups)
   - **NYC-filtered grouped scan:** `14.68 ms` (`17` groups, **+3.6%**, within noise)
   - **Why:** After compacting all three cities into a single Parquet file (partitioned only by `day(event_time)`), DuckDB still scans the single day file in memory; city-level file skipping only yields major speedup when tables are partitioned by `(day(event_time), city_id)` across multi-gigabyte object-store datasets (as configured in `pipelines/spark/marketplace_batch_and_reconciliation.py` for `gold.spark_h3_marketplace_summary`).

---

## 2. Real Distributed Streaming Defects Discovered & Fixed During Verification

Executing the chaos suite and visually auditing live Control Tower screenshots surfaced three real streaming systems bugs that were fixed cleanly in [`marketplace_platform/state_engine.py`](marketplace_platform/state_engine.py):

1. **Non-monotonic partition offset commits under event-time sorting (caught by Chaos 02):**
   - *Root cause:* Globally sorting records across topics by `event_time` allowed offset `k+1` (with an earlier timestamp) to be processed and committed ahead of offset `k` on the same partition. After a mid-stream crash, restoring from `committed_offsets` skipped unprocessed lower-offset records, causing pricing multiplier drift on 4 cells.
   - *Fix:* Replaced global sorting with a **K-way per-partition FIFO min-heap merge** (`heapq`) over `(topic, partition)` queues plus monotonic `max(prev, offset + 1)` tracking. Chaos 02 now passes with **100% bit-for-bit supply and pricing parity** after crash recovery.
2. **Dropped right-side join events when dispatch offers arrived ahead of `ride_requested` (caught via visual funnel QA):**
   - *Root cause:* Under strict per-partition FIFO consumption across independent Kafka topics, a `driver_offer_received` record on `marketplace.dispatch.offers` occasionally arrived before its corresponding `ride_requested` record on `marketplace.rider.requests`, causing `if req is None: return` to drop the offer (driving `Dispatch Offered` from `90.8%` down to `22.8%`).
   - *Fix:* Added `_get_or_create_join_stub()` so right-side dispatch/trip events upsert a keyed join state shell that merges cleanly when `ride_requested` arrives, preserving monotonic funnel conversion (`666 -> 605 -> 543 -> 517`).
3. **Cross-domain watermark eviction of idle available drivers (caught via live utilization audit):**
   - *Root cause:* Completed trips have timestamps `5–20 minutes` after initial dispatch. Advancing a single global watermark from `marketplace.trip.lifecycle` past `driver_heartbeat_ttl_s = 180s` prematurely evicted idle `AVAILABLE` drivers whose last GPS ping occurred at the end of the driver stream.
   - *Fix:* Isolated driver heartbeat and reservation timers to a dedicated per-domain `driver_watermark_ts` fed strictly by driver availability and location topics, persisted in stream checkpoints.

---

## 3. Complete Acceptance Criteria Verification Matrix (A–V)

| ID | Criterion | Verification Method & Raw Evidence | Status |
| --- | --- | --- | --- |
| A | Open-source pinned stack (zero `:latest`, verified licenses, SBOM) | `docker-compose.yml`, `scripts/check_licenses.py` (`69` pkgs, `0` flagged), `results/security/sbom.cdx.json` | PASS |
| B | Authoritative 7-state driver machine & zero double-counting across H3 transfers | `test_driver_h3_cell_transfers_never_double_count_and_terminal_requests_clear`, `assert_state_invariants()` | PASS |
| C | Authoritative rider request machine & immediate terminal-request removal | `test_driver_h3_cell_transfers_never_double_count_and_terminal_requests_clear`, `assert_state_invariants()` | PASS |
| D | Out-of-order stale driver GPS/state rejection via monotonic sequence numbers | `test_chaos_04_out_of_order_driver_location_rejected` | PASS |
| E | Multi-entity streaming joins (requests, offers, trips, pricing) into conversion funnel | `MarketplaceStreamProcessor`, `streaming/flink-sql/marketplace_streaming_joins.sql`, `gold.matching_conversion_funnel` | PASS |
| F | Explainable dynamic pricing with 1-ring neighbor supply, bounds, smoothing, hysteresis, step cap | `test_pricing_bounds_cap_and_explainability`, `test_pricing_smoothing_hysteresis_min_interval_and_step_cap` | PASS |
| G | Stale/missing signal safe `1.0x` fallback & deterministic replay parity | `test_pricing_stale_and_missing_safe_fallbacks_and_determinism` | PASS |
| H | H3-cluster A/B pricing experimentation with chi-squared SRM check | `test_experiment_h3_assignment_and_srm_check`, `gold.pricing_experiment_results` (`PASS`) | PASS |
| I | Avro wire contracts, v1↔v2 backward/forward evolution, and DLQ quarantine | `tests/contracts/test_avro_contracts.py`, `test_chaos_09_hot_h3_cell_skew_and_poison_quarantine` | PASS |
| J | Low-latency version-aware serving store (`Valkey 9.2.0` Lua CAS / local SQLite WAL) | `test_chaos_07_stale_serving_store_version_rejected`, `load_profiles.json` (`0.070 ms p95`) | PASS |
| K | Full serving-store outage -> safe `1.0x` fallback -> `wipe_and_rebuild_from_log()` | `test_chaos_03_serving_store_outage_safe_fallback_and_full_rebuild` | PASS |
| L | Apache Iceberg V2 medallion + dimensional star schema + DuckDB/Trino/Spark SQL | `marketplace_platform/lakehouse.py`, `pipelines/spark/`, `sql/trino/marketplace_analytics.sql` | PASS |
| M | Iceberg time travel, schema evolution, small-file compaction, snapshot expiration | `test_full_pipeline_iceberg_timetravel_evolution_compaction_and_reconciliation`, `iceberg_optimizations.json` | PASS |
| N | Operational PostgreSQL/PostGIS CDC policy history (SCD Type 2) | `pipelines/cdc/`, `materialize_cdc_policy_scd2()`, `silver.dim_pricing_policy_scd2` | PASS |
| O | Serving-store vs. Iceberg reconciliation detecting & repairing injected drift | `test_chaos_08_serving_drift_detection_and_repair` | PASS |
| P | 8-check automated Data Quality suite persisted to `quality.dq_check_results` | `run_data_quality()` (`8/8 PASS` across every load profile) | PASS |
| Q | All 12 required FastAPI endpoints separating `live_kv` from `lakehouse_iceberg` | `test_api_endpoints_rbac_and_safe_fallback` (16 GET + 3 RBAC POST checks) | PASS |
| R | Security: RBAC roles, rate limiting, audit logs, secret scrubbing, H3-only privacy | `apps/marketplace-api/security.py`, `test_secret_scrubbing`, `docs/security.md` | PASS |
| S | Live Marketplace Control Tower dashboard (all 12 views) verified via real browser capture | `docs/assets/control_tower_top.png`, `docs/assets/control_tower_full.png` | PASS |
| T | 9-scenario chaos & fault-injection suite + postmortem incident report | `results/chaos/chaos_results.json` (`9/9 PASS`), `docs/incident-reports/INC-001-chaos-drill.md` | PASS |
| U | Sustained load benchmarks (incl. 1M virtual IDs) & H3 res 6/7/8/9 tradeoff study | `results/benchmarks/*.{json,csv}`, `docs/benchmarks/benchmark-report.md` | PASS |
| V | Out-of-the-box portability (Standalone Docker, Zero-Docker script, Compose, K8s) & CI | `Dockerfile.standalone`, `scripts/run_portable.sh`, `docker-compose.yml`, `infra/kubernetes/`, `.github/workflows/` | PASS |
