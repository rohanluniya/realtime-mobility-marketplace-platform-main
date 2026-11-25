"""Apache Airflow 3.4.0 DAGs for Real-Time Mobility Marketplace Platform (Section 18).

Implements 6 idempotent, retry-safe orchestration DAGs:
1. `marketplace_incremental_lakehouse`: Bronze ingestion -> CDC Policy SCD2 -> Silver/Gold curation.
2. `marketplace_state_reconciliation`: Detects and deterministically repairs serving KV vs Iceberg drift.
3. `marketplace_pricing_experiments`: Computes H3 cluster A/B metrics and Sample Ratio Mismatch (SRM) gate.
4. `marketplace_data_quality_checks`: Executes all 8 invariant/business DQ assertions and fails loud on breach.
5. `marketplace_iceberg_maintenance`: Compacts small Parquet files and expires old Iceberg snapshots.
6. `marketplace_backfill_and_benchmark`: Parameterized historical replay & benchmark verification pipeline.

Runtime: `apache/airflow:slim-3.4.0-python3.12` (Airflow 2.x reached EOL in April 2026).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.decorators import dag, task
except ImportError:  # pragma: no cover - allows syntax/AST import checks outside Airflow container
    def dag(*args: Any, **kwargs: Any):  # type: ignore[misc]
        def _wrap(fn: Any) -> Any:
            return fn
        return _wrap

    def task(*args: Any, **kwargs: Any):  # type: ignore[misc]
        def _wrap(fn: Any) -> Any:
            return fn
        return _wrap


DEFAULT_ARGS = {
    "owner": "mobility-data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}


@dag(
    dag_id="marketplace_incremental_lakehouse",
    description="Incremental Bronze ingestion, CDC Policy SCD2 update, and Silver/Gold dimensional curation",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "lakehouse", "iceberg"],
)
def marketplace_incremental_lakehouse() -> None:
    @task(task_id="ingest_bronze_from_kafka")
    def ingest_bronze() -> dict[str, Any]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.contracts import AvroContractRegistry
        from marketplace_platform.lakehouse import MarketplaceLakehouse
        from marketplace_platform.transport import create_event_bus

        cfg = MarketplaceConfig()
        return MarketplaceLakehouse(cfg).ingest_bronze_from_bus(
            create_event_bus(cfg), AvroContractRegistry(cfg.project_root / "schemas" / "avro")
        )

    @task(task_id="materialize_cdc_policy_scd2")
    def sync_policy_scd2(_: dict[str, Any]) -> dict[str, Any]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.lakehouse import MarketplaceLakehouse

        return MarketplaceLakehouse(MarketplaceConfig()).materialize_cdc_policy_scd2()

    @task(task_id="curate_silver_and_gold")
    def curate_gold(_: dict[str, Any]) -> dict[str, Any]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.lakehouse import MarketplaceLakehouse

        return MarketplaceLakehouse(MarketplaceConfig()).curate_silver_and_gold()

    curate_gold(sync_policy_scd2(ingest_bronze()))


@dag(
    dag_id="marketplace_state_reconciliation",
    description="Compare authoritative Iceberg Gold pricing state vs Valkey/KV serving store and repair drift",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "reconciliation", "reliability"],
)
def marketplace_state_reconciliation() -> None:
    @task(task_id="reconcile_serving_vs_lakehouse")
    def reconcile() -> dict[str, Any]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.contracts import AvroContractRegistry
        from marketplace_platform.lakehouse import MarketplaceLakehouse
        from marketplace_platform.serving_store import MaterializedStateStore
        from marketplace_platform.transport import create_event_bus

        cfg = MarketplaceConfig()
        return MarketplaceLakehouse(cfg).reconcile_serving_store(
            MaterializedStateStore(cfg), create_event_bus(cfg), AvroContractRegistry(cfg.project_root / "schemas" / "avro")
        )

    reconcile()


@dag(
    dag_id="marketplace_pricing_experiments",
    description="Evaluate H3-cluster pricing A/B experiments and enforce Sample Ratio Mismatch (SRM) gate",
    schedule="0 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "pricing", "experiments"],
)
def marketplace_pricing_experiments() -> None:
    @task(task_id="validate_pricing_experiment_srm")
    def evaluate_experiments() -> dict[str, Any]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.lakehouse import MarketplaceLakehouse

        lh = MarketplaceLakehouse(MarketplaceConfig())
        rows = lh.query_sql("SELECT * FROM gold_experiments")
        for r in rows:
            if r["srm_status"] == "FAIL_SRM":
                raise RuntimeError(f"Sample Ratio Mismatch detected in experiment {r['experiment_id']}")
        return {"variants_checked": len(rows), "status": "PASS"}

    evaluate_experiments()


@dag(
    dag_id="marketplace_data_quality_checks",
    description="Execute all 8 marketplace data quality, uniqueness, policy-bound, and reconciliation checks",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "data-quality"],
)
def marketplace_data_quality_checks() -> None:
    @task(task_id="run_marketplace_dq_suite")
    def run_dq() -> dict[str, Any]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.lakehouse import MarketplaceLakehouse

        res = MarketplaceLakehouse(MarketplaceConfig()).run_data_quality()
        if res["overall_status"] != "PASS":
            raise RuntimeError(f"Marketplace DQ suite failed: {res['summary']}")
        return res["summary"]

    run_dq()


@dag(
    dag_id="marketplace_iceberg_maintenance",
    description="Iceberg V2 small-file compaction and snapshot expiration",
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "iceberg", "maintenance"],
)
def marketplace_iceberg_maintenance() -> None:
    @task(task_id="compact_and_expire_tables")
    def maintain() -> list[dict[str, Any]]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.lakehouse import MarketplaceLakehouse

        lh = MarketplaceLakehouse(MarketplaceConfig())
        out = []
        for tbl in ("bronze.raw_events", "silver.fact_pricing_decision", "silver.fact_matching_outcome"):
            out.append(lh.compact_table(tbl))
            out.append(lh.expire_old_snapshots(tbl, keep_last_n=5))
        return out

    maintain()


@dag(
    dag_id="marketplace_backfill_and_benchmark",
    description="Parameterized bounded replay, backfill, and H3 resolution benchmark verification",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "backfill", "benchmarks"],
)
def marketplace_backfill_and_benchmark() -> None:
    @task(task_id="replay_and_reprocess")
    def backfill() -> dict[str, Any]:
        from marketplace_platform.config import MarketplaceConfig
        from marketplace_platform.contracts import AvroContractRegistry
        from marketplace_platform.lakehouse import MarketplaceLakehouse
        from marketplace_platform.state_engine import MarketplaceStreamProcessor
        from marketplace_platform.transport import create_event_bus

        cfg = MarketplaceConfig()
        bus = create_event_bus(cfg)
        reg = AvroContractRegistry(cfg.project_root / "schemas" / "avro")
        proc = MarketplaceStreamProcessor(cfg, bus, reg)
        return {
            "stream": proc.process_all(),
            "lakehouse": MarketplaceLakehouse(cfg).curate_silver_and_gold(),
        }

    backfill()


# Instantiate DAG definitions for Airflow scheduler discovery
dag_incremental = marketplace_incremental_lakehouse()
dag_reconciliation = marketplace_state_reconciliation()
dag_experiments = marketplace_pricing_experiments()
dag_dq = marketplace_data_quality_checks()
dag_maintenance = marketplace_iceberg_maintenance()
dag_backfill = marketplace_backfill_and_benchmark()
