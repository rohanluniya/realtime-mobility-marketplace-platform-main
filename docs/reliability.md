# Reliability Engineering, Recovery Architecture & Alert Runbooks

Every Prometheus alert defined in [`infra/monitoring/alerts.yml`](../infra/monitoring/alerts.yml) maps directly to a recovery procedure below.

## Recovery Objectives & Guarantees

- **Authoritative Durability:** Durable Kafka/WAL event logs + Apache Iceberg V2 snapshots guarantee zero authoritative state loss (`RPO = 0` for committed Kafka offsets).
- **Serving Store Rebuildability:** Valkey / local KV state is fully rebuildable from durable changelog topics (`marketplace.supply.aggregates`, `marketplace.demand.aggregates`, `marketplace.pricing.decisions`) in **~120 ms** locally (`local` profile, `585` keys).
- **Fail-Safe Pricing Degradation:** Serving store outages or stale signals automatically return baseline **`1.00x`** pricing (`SAFE_FALLBACK_OUTAGE` / `FALLBACK_STALE`), preventing runaway surge charges during infrastructure faults.

---

## Alert Runbooks

### `serving-store-outage-and-rebuild`
**Trigger:** `MarketplacePricingServingStoreErrors` (`store_unavailable > 0`).
1. Confirm `/health` reports `degraded_serving_fallback` and verify `/marketplace/pricing/{h3_cell}` is serving safe `1.0x` fallback responses.
2. Restore Valkey (`kubectl rollout restart statefulset/valkey -n mobility-marketplace` or `docker compose restart valkey`).
3. Rebuild live materialized state deterministically from durable changelog topics:
   ```bash
   .venv/bin/python services/kafka-ingestion/replay.py --rebuild-serving-store --wipe-first
   ```
4. Confirm zero drift by triggering operator reconciliation (`POST /platform/actions/reconcile`).

### `dead-letter-quarantine-triage`
**Trigger:** `MarketplaceDLQSpike` (`>25` DLQ records in 5 minutes).
1. Inspect error codes via `GET /platform/data-quality` (`dlq_summary`) or query `bronze.dead_letter`.
2. Verify producer schema compatibility against Apicurio Registry (`tests/contracts/test_avro_contracts.py`).
3. Fix upstream producer regression; valid traffic continues flowing unimpeded during triage.

### `data-quality-failure-response`
**Trigger:** `MarketplaceDataQualityFailure` (`dq_checks_total{status="FAIL"} > 0`).
1. Query `/platform/data-quality` to isolate failing check(s) (e.g., `pricing_policy_bounds_compliance`, `zero_double_counted_drivers_and_active_requests`).
2. Pause downstream pricing policy rollout and audit recent Iceberg snapshots via Trino `FOR VERSION AS OF <snapshot_id>`.
3. Reprocess affected time window via `services/kafka-ingestion/replay.py --reprocess-pricing --replay-batch-id <id>`.

### `serving-reconciliation`
**Trigger:** `MarketplaceServingDriftDetected`.
1. Review `GET /platform/metrics` reconciliation counters (`drifted_cells_detected`, `missing_cells_detected`, `cells_repaired`).
2. Investigate whether an unversioned writer or stale consumer attempted out-of-order writes against Valkey (`serving_store_errors_total{reason="stale_version_rejected"}`).
3. Confirm subsequent reconciliation returns `status: "CONSISTENT"`.

### `latency-slo-investigation`
**Trigger:** `MarketplaceHighPricingLookupLatency` (p95 lookup `> 50 ms`).
1. Check Valkey memory pressure (`valkey-cli info memory`) and active H3 cell cardinality.
2. Verify H3 pricing resolution remains at Resolution 7 (shifting to Res 9 increases KV key cardinality by `5.06×`).
3. Scale API replicas via HPA (`infra/kubernetes/marketplace-platform.yaml`).
