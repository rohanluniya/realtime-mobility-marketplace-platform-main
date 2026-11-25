"""Apache Iceberg Format V2 Dimensional Medallion Lakehouse, CDC Policy SCD2,
Data Quality Framework, and Serving-Store Reconciliation Engine (Sections 14-20).
"""

from __future__ import annotations

import json
import math
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry, SchemaValidationError
from marketplace_platform.geospatial import CITIES, h3_center, h3_parent
from marketplace_platform.observability import OpenLineageEmitter, get_logger, get_metrics
from marketplace_platform.pricing_engine import (
    DEFAULT_EXPERIMENT_ID,
    POLICY_CATALOG,
    compute_chi_square_srm,
)
from marketplace_platform.serving_store import MaterializedStateStore, StateMaterializer
from marketplace_platform.state_engine import MarketplaceStreamProcessor
from marketplace_platform.transport import EventBus


def _parse_dt(iso: str | None) -> datetime:
    if not iso:
        return datetime.now(UTC).replace(tzinfo=None)
    dt = datetime.fromisoformat(iso)
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


def _bronze_events_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "source_topic", StringType(), required=True),
        NestedField(3, "event_type", StringType(), required=True),
        NestedField(4, "event_time", TimestampType(), required=True),
        NestedField(5, "ingestion_time", TimestampType(), required=True),
        NestedField(6, "city_id", StringType(), required=True),
        NestedField(7, "region_id", StringType(), required=True),
        NestedField(8, "rider_id", StringType(), required=False),
        NestedField(9, "driver_id", StringType(), required=False),
        NestedField(10, "request_id", StringType(), required=False),
        NestedField(11, "trip_id", StringType(), required=False),
        NestedField(12, "latitude", DoubleType(), required=True),
        NestedField(13, "longitude", DoubleType(), required=True),
        NestedField(14, "sequence_number", LongType(), required=False),
        NestedField(15, "payload_json", StringType(), required=True),
        NestedField(16, "replay_batch_id", StringType(), required=False),
    )


def _pricing_decision_schema() -> Schema:
    return Schema(
        NestedField(1, "decision_id", StringType(), required=True),
        NestedField(2, "decision_version", LongType(), required=True),
        NestedField(3, "region_id", StringType(), required=True),
        NestedField(4, "city_id", StringType(), required=True),
        NestedField(5, "h3_cell", StringType(), required=True),
        NestedField(6, "h3_coarse_res5", StringType(), required=True),
        NestedField(7, "h3_resolution", IntegerType(), required=True),
        NestedField(8, "decision_time", TimestampType(), required=True),
        NestedField(9, "input_window", StringType(), required=True),
        NestedField(10, "demand_count", IntegerType(), required=True),
        NestedField(11, "supply_count", IntegerType(), required=True),
        NestedField(12, "neighbor_supply_count", IntegerType(), required=True),
        NestedField(13, "estimated_wait_seconds", DoubleType(), required=True),
        NestedField(14, "traffic_factor", DoubleType(), required=True),
        NestedField(15, "regional_event_active", BooleanType(), required=True),
        NestedField(16, "input_freshness_seconds", DoubleType(), required=True),
        NestedField(17, "raw_multiplier", DoubleType(), required=True),
        NestedField(18, "pricing_multiplier", DoubleType(), required=True),
        NestedField(19, "policy_version", StringType(), required=True),
        NestedField(20, "model_version", StringType(), required=True),
        NestedField(21, "experiment_id", StringType(), required=False),
        NestedField(22, "experiment_variant", StringType(), required=False),
        NestedField(23, "reason_codes_csv", StringType(), required=True),
        NestedField(24, "decision_status", StringType(), required=True),
    )


def _matching_outcome_schema() -> Schema:
    return Schema(
        NestedField(1, "outcome_id", StringType(), required=True),
        NestedField(2, "request_id", StringType(), required=True),
        NestedField(3, "trip_id", StringType(), required=False),
        NestedField(4, "rider_id", StringType(), required=True),
        NestedField(5, "driver_id", StringType(), required=False),
        NestedField(6, "city_id", StringType(), required=True),
        NestedField(7, "region_id", StringType(), required=True),
        NestedField(8, "h3_cell_res7", StringType(), required=True),
        NestedField(9, "service_type", StringType(), required=True),
        NestedField(10, "request_time", TimestampType(), required=True),
        NestedField(11, "outcome_time", TimestampType(), required=True),
        NestedField(12, "final_status", StringType(), required=True),
        NestedField(13, "request_to_offer_ms", LongType(), required=False),
        NestedField(14, "offer_to_accept_ms", LongType(), required=False),
        NestedField(15, "request_to_match_ms", LongType(), required=False),
        NestedField(16, "offers_sent", IntegerType(), required=True),
        NestedField(17, "offers_rejected", IntegerType(), required=True),
        NestedField(18, "pricing_multiplier", DoubleType(), required=True),
        NestedField(19, "policy_version", StringType(), required=True),
        NestedField(20, "experiment_variant", StringType(), required=False),
        NestedField(21, "estimated_pickup_seconds", DoubleType(), required=False),
        NestedField(22, "trip_completed", BooleanType(), required=True),
        NestedField(23, "cancelled_by", StringType(), required=False),
    )


def _dlq_schema() -> Schema:
    return Schema(
        NestedField(1, "dlq_id", StringType(), required=True),
        NestedField(2, "source_topic", StringType(), required=True),
        NestedField(3, "error_code", StringType(), required=True),
        NestedField(4, "error_message", StringType(), required=True),
        NestedField(5, "quarantined_at", TimestampType(), required=True),
        NestedField(6, "raw_payload_b64", StringType(), required=True),
    )


def _dim_policy_scd2_schema() -> Schema:
    return Schema(
        NestedField(1, "policy_sk", StringType(), required=True),
        NestedField(2, "region_id", StringType(), required=True),
        NestedField(3, "policy_version", StringType(), required=True),
        NestedField(4, "min_multiplier", DoubleType(), required=True),
        NestedField(5, "max_multiplier", DoubleType(), required=True),
        NestedField(6, "smoothing_alpha", DoubleType(), required=True),
        NestedField(7, "neighbor_supply_weight", DoubleType(), required=True),
        NestedField(8, "effective_from", TimestampType(), required=True),
        NestedField(9, "effective_to", TimestampType(), required=False),
        NestedField(10, "is_current", BooleanType(), required=True),
        NestedField(11, "cdc_lsn", LongType(), required=True),
    )


def _gold_city_summary_schema() -> Schema:
    return Schema(
        NestedField(1, "city_id", StringType(), required=True),
        NestedField(2, "region_id", StringType(), required=True),
        NestedField(3, "active_h3_cells", LongType(), required=True),
        NestedField(4, "total_requests", LongType(), required=True),
        NestedField(5, "matched_requests", LongType(), required=True),
        NestedField(6, "completed_trips", LongType(), required=True),
        NestedField(7, "available_drivers", LongType(), required=True),
        NestedField(8, "avg_pricing_multiplier", DoubleType(), required=True),
        NestedField(9, "max_pricing_multiplier", DoubleType(), required=True),
        NestedField(10, "match_rate_pct", DoubleType(), required=True),
        NestedField(11, "completion_rate_pct", DoubleType(), required=True),
        NestedField(12, "liquidity_score", DoubleType(), required=True),
    )


def _gold_funnel_schema() -> Schema:
    return Schema(
        NestedField(1, "city_id", StringType(), required=True),
        NestedField(2, "service_type", StringType(), required=True),
        NestedField(3, "requests_total", LongType(), required=True),
        NestedField(4, "requests_offered", LongType(), required=True),
        NestedField(5, "requests_matched", LongType(), required=True),
        NestedField(6, "trips_completed", LongType(), required=True),
        NestedField(7, "rider_cancellations", LongType(), required=True),
        NestedField(8, "driver_cancellations", LongType(), required=True),
        NestedField(9, "unfulfilled_requests", LongType(), required=True),
        NestedField(10, "driver_acceptance_rate_pct", DoubleType(), required=True),
        NestedField(11, "match_success_rate_pct", DoubleType(), required=True),
        NestedField(12, "avg_request_to_offer_ms", DoubleType(), required=True),
        NestedField(13, "avg_request_to_match_ms", DoubleType(), required=True),
        NestedField(14, "avg_estimated_pickup_s", DoubleType(), required=True),
    )


def _gold_h3_cell_schema() -> Schema:
    return Schema(
        NestedField(1, "city_id", StringType(), required=True),
        NestedField(2, "region_id", StringType(), required=True),
        NestedField(3, "h3_cell", StringType(), required=True),
        NestedField(4, "h3_coarse_res5", StringType(), required=True),
        NestedField(5, "center_lat", DoubleType(), required=True),
        NestedField(6, "center_lon", DoubleType(), required=True),
        NestedField(7, "demand_count", IntegerType(), required=True),
        NestedField(8, "supply_count", IntegerType(), required=True),
        NestedField(9, "neighbor_supply_count", IntegerType(), required=True),
        NestedField(10, "imbalance_ratio", DoubleType(), required=True),
        NestedField(11, "pricing_multiplier", DoubleType(), required=True),
        NestedField(12, "policy_version", StringType(), required=True),
        NestedField(13, "experiment_variant", StringType(), required=True),
        NestedField(14, "reason_codes_csv", StringType(), required=True),
        NestedField(15, "decision_status", StringType(), required=True),
        NestedField(16, "decision_time", TimestampType(), required=True),
    )


def _gold_experiment_schema() -> Schema:
    return Schema(
        NestedField(1, "experiment_id", StringType(), required=True),
        NestedField(2, "experiment_variant", StringType(), required=True),
        NestedField(3, "policy_version", StringType(), required=True),
        NestedField(4, "cells_exposed", LongType(), required=True),
        NestedField(5, "requests_exposed", LongType(), required=True),
        NestedField(6, "match_rate_pct", DoubleType(), required=True),
        NestedField(7, "completion_rate_pct", DoubleType(), required=True),
        NestedField(8, "cancellation_rate_pct", DoubleType(), required=True),
        NestedField(9, "avg_pricing_multiplier", DoubleType(), required=True),
        NestedField(10, "avg_request_to_match_ms", DoubleType(), required=True),
        NestedField(11, "srm_chi2_stat", DoubleType(), required=True),
        NestedField(12, "srm_status", StringType(), required=True),
    )


def _dq_schema() -> Schema:
    return Schema(
        NestedField(1, "run_id", StringType(), required=True),
        NestedField(2, "executed_at", TimestampType(), required=True),
        NestedField(3, "layer", StringType(), required=True),
        NestedField(4, "check_name", StringType(), required=True),
        NestedField(5, "status", StringType(), required=True),
        NestedField(6, "observed_value", DoubleType(), required=True),
        NestedField(7, "threshold", DoubleType(), required=True),
        NestedField(8, "details_json", StringType(), required=True),
    )


class MarketplaceLakehouse:
    """Real Apache Iceberg Format V2 dimensional medallion lakehouse + DuckDB analytical SQL."""

    def __init__(self, config: MarketplaceConfig) -> None:
        self.config = config
        self.config.ensure_local_dirs()
        self.catalog = self._create_catalog()
        self.lineage = OpenLineageEmitter(
            namespace=config.openlineage_namespace,
            marquez_url=config.openlineage_url,
            local_store_dir=config.lineage_dir,
        )
        self.logger = get_logger("marketplace-lakehouse", "iceberg", environment=config.env)
        self.metrics = get_metrics()
        self.ensure_tables()

    def _create_catalog(self) -> Catalog:
        if self.config.execution_profile == "docker":
            return load_catalog(
                "marketplace_rest",
                **{
                    "type": "rest",
                    "uri": self.config.iceberg_rest_uri,
                    "warehouse": self.config.iceberg_warehouse,
                    "s3.endpoint": self.config.s3_endpoint_url,
                    "s3.access-key-id": self.config.s3_access_key_id,
                    "s3.secret-access-key": self.config.s3_secret_access_key,
                    "s3.path-style-access": "true",
                },
            )
        return load_catalog(
            "marketplace_local",
            **{
                "type": "sql",
                "uri": f"sqlite:///{self.config.sqlite_catalog_path}",
                "warehouse": f"file://{self.config.warehouse_dir}",
            },
        )

    def _ensure_ns(self, ns: str) -> None:
        try:
            self.catalog.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass

    def _ensure_table(
        self, ident: str, schema: Schema, spec: PartitionSpec | None = None
    ) -> Table:
        try:
            return self.catalog.load_table(ident)
        except NoSuchTableError:
            props = {
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
            }
            if spec is not None:
                return self.catalog.create_table(ident, schema=schema, partition_spec=spec, properties=props)
            return self.catalog.create_table(ident, schema=schema, properties=props)

    def ensure_tables(self) -> None:
        for ns in ("bronze", "silver", "gold", "quality"):
            self._ensure_ns(ns)

        self._ensure_table(
            "bronze.raw_events",
            _bronze_events_schema(),
            PartitionSpec(PartitionField(4, 1000, DayTransform(), "event_day")),
        )
        self._ensure_table(
            "bronze.pricing_decisions_raw",
            _pricing_decision_schema(),
            PartitionSpec(PartitionField(8, 1000, DayTransform(), "decision_day")),
        )
        self._ensure_table(
            "bronze.matching_outcomes_raw",
            _matching_outcome_schema(),
            PartitionSpec(PartitionField(10, 1000, DayTransform(), "request_day")),
        )
        self._ensure_table("bronze.dead_letter", _dlq_schema())

        self._ensure_table(
            "silver.fact_pricing_decision",
            _pricing_decision_schema(),
            PartitionSpec(PartitionField(8, 1000, DayTransform(), "decision_day")),
        )
        self._ensure_table(
            "silver.fact_matching_outcome",
            _matching_outcome_schema(),
            PartitionSpec(PartitionField(10, 1000, DayTransform(), "request_day")),
        )
        self._ensure_table("silver.dim_pricing_policy_scd2", _dim_policy_scd2_schema())

        self._ensure_table("gold.city_supply_demand_summary", _gold_city_summary_schema())
        self._ensure_table("gold.matching_conversion_funnel", _gold_funnel_schema())
        self._ensure_table("gold.h3_cell_marketplace_state", _gold_h3_cell_schema())
        self._ensure_table("gold.pricing_experiment_results", _gold_experiment_schema())
        self._ensure_table("quality.dq_check_results", _dq_schema())

    def load_table(self, ident: str) -> Table:
        return self.catalog.load_table(ident)

    def append_pylist(self, ident: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        tbl = self.load_table(ident)
        tbl.append(pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow()))
        return len(rows)

    def overwrite_pylist(self, ident: str, rows: list[dict[str, Any]]) -> int:
        tbl = self.load_table(ident)
        tbl.overwrite(pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow()))
        return len(rows)

    def read_arrow(self, ident: str, *, snapshot_id: int | None = None) -> pa.Table:
        tbl = self.load_table(ident)
        return (tbl.scan(snapshot_id=snapshot_id) if snapshot_id else tbl.scan()).to_arrow()

    def query_sql(
        self,
        sql: str,
        params: list[Any] | None = None,
        snapshot_overrides: dict[str, int] | None = None,
    ) -> list[dict[str, Any]]:
        con = duckdb.connect(":memory:")
        try:
            snaps = snapshot_overrides or {}
            mapping = {
                "bronze_raw_events": "bronze.raw_events",
                "bronze_pricing": "bronze.pricing_decisions_raw",
                "bronze_matching": "bronze.matching_outcomes_raw",
                "bronze_dlq": "bronze.dead_letter",
                "silver_pricing": "silver.fact_pricing_decision",
                "silver_matching": "silver.fact_matching_outcome",
                "silver_dim_policy": "silver.dim_pricing_policy_scd2",
                "gold_city_summary": "gold.city_supply_demand_summary",
                "gold_funnel": "gold.matching_conversion_funnel",
                "gold_h3_state": "gold.h3_cell_marketplace_state",
                "gold_experiments": "gold.pricing_experiment_results",
                "quality_dq": "quality.dq_check_results",
            }
            for alias, ident in mapping.items():
                if alias in sql:
                    con.register(alias, self.read_arrow(ident, snapshot_id=snaps.get(ident)))
            cur = con.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        finally:
            con.close()

    def table_stats(self, ident: str) -> dict[str, Any]:
        tbl = self.load_table(ident)
        snaps = list(tbl.snapshots())
        cur = tbl.current_snapshot()
        tasks = list(tbl.scan().plan_files()) if cur else []
        return {
            "table": ident,
            "format_version": tbl.metadata.format_version,
            "current_snapshot_id": cur.snapshot_id if cur else None,
            "snapshot_count": len(snaps),
            "data_file_count": len(tasks),
            "total_records": int(sum(t.file.record_count for t in tasks)),
            "total_bytes": int(sum(t.file.file_size_in_bytes for t in tasks)),
        }

    def all_table_stats(self) -> list[dict[str, Any]]:
        names = [
            "bronze.raw_events",
            "bronze.pricing_decisions_raw",
            "bronze.matching_outcomes_raw",
            "bronze.dead_letter",
            "silver.fact_pricing_decision",
            "silver.fact_matching_outcome",
            "silver.dim_pricing_policy_scd2",
            "gold.city_supply_demand_summary",
            "gold.matching_conversion_funnel",
            "gold.h3_cell_marketplace_state",
            "gold.pricing_experiment_results",
            "quality.dq_check_results",
        ]
        return [self.table_stats(n) for n in names]

    def compact_table(self, ident: str) -> dict[str, Any]:
        t0 = time.perf_counter()
        before = self.table_stats(ident)
        if before["data_file_count"] <= 1 or before["total_records"] == 0:
            return {"table": ident, "action": "skipped", "files_before": before["data_file_count"], "files_after": before["data_file_count"]}
        arr = self.read_arrow(ident)
        tbl = self.load_table(ident)
        tbl.overwrite(arr)
        after = self.table_stats(ident)
        return {
            "table": ident,
            "action": "compacted",
            "files_before": before["data_file_count"],
            "files_after": after["data_file_count"],
            "bytes_before": before["total_bytes"],
            "bytes_after": after["total_bytes"],
            "records_preserved": after["total_records"],
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        }

    def ingest_bronze_from_bus(
        self, bus: EventBus, registry: AvroContractRegistry, *, replay_batch_id: str | None = None
    ) -> dict[str, Any]:
        """Durably commit raw events, pricing decisions, matching outcomes, and DLQ into Bronze."""
        t0 = time.perf_counter()
        run_id = str(uuid.uuid4())
        self.lineage.emit(
            event_type="START",
            job_name="marketplace_bronze_ingestion",
            run_id=run_id,
            inputs=[{"name": "marketplace.*"}],
            outputs=[
                {"name": "bronze.raw_events"},
                {"name": "bronze.pricing_decisions_raw"},
                {"name": "bronze.matching_outcomes_raw"},
                {"name": "bronze.dead_letter"},
            ],
        )

        raw_rows: list[dict[str, Any]] = []
        raw_topics = [
            "marketplace.rider.requests",
            "marketplace.driver.locations",
            "marketplace.driver.availability",
            "marketplace.trip.lifecycle",
            "marketplace.dispatch.offers",
            "marketplace.regional.events",
            "marketplace.pricing.policies",
        ]
        for topic in raw_topics:
            for rec in bus.read_range(topic):
                try:
                    _, ev = registry.deserialize(rec.value, expected_topic=topic)
                except SchemaValidationError:
                    continue
                raw_rows.append(
                    {
                        "event_id": ev["event_id"],
                        "source_topic": topic,
                        "event_type": ev["event_type"],
                        "event_time": _parse_dt(ev["event_time"]),
                        "ingestion_time": _parse_dt(ev.get("ingestion_time")),
                        "city_id": ev["city_id"],
                        "region_id": ev["region_id"],
                        "rider_id": ev.get("rider_id"),
                        "driver_id": ev.get("driver_id"),
                        "request_id": ev.get("request_id"),
                        "trip_id": ev.get("trip_id"),
                        "latitude": float(ev["latitude"]),
                        "longitude": float(ev["longitude"]),
                        "sequence_number": ev.get("sequence_number"),
                        "payload_json": ev["payload_json"],
                        "replay_batch_id": replay_batch_id,
                    }
                )

        pricing_rows: list[dict[str, Any]] = []
        for rec in bus.read_range("marketplace.pricing.decisions"):
            _, p = registry.deserialize(rec.value, expected_topic="marketplace.pricing.decisions")
            pricing_rows.append(
                {
                    "decision_id": p["decision_id"],
                    "decision_version": int(p["decision_version"]),
                    "region_id": p["region_id"],
                    "city_id": p["city_id"],
                    "h3_cell": p["h3_cell"],
                    "h3_coarse_res5": h3_parent(p["h3_cell"], 5),
                    "h3_resolution": int(p["h3_resolution"]),
                    "decision_time": _parse_dt(p["decision_time"]),
                    "input_window": p["input_window"],
                    "demand_count": int(p["demand_count"]),
                    "supply_count": int(p["supply_count"]),
                    "neighbor_supply_count": int(p["neighbor_supply_count"]),
                    "estimated_wait_seconds": float(p["estimated_wait_seconds"]),
                    "traffic_factor": float(p["traffic_factor"]),
                    "regional_event_active": bool(p["regional_event_active"]),
                    "input_freshness_seconds": float(p["input_freshness_seconds"]),
                    "raw_multiplier": float(p["raw_multiplier"]),
                    "pricing_multiplier": float(p["pricing_multiplier"]),
                    "policy_version": p["policy_version"],
                    "model_version": p["model_version"],
                    "experiment_id": p.get("experiment_id"),
                    "experiment_variant": p.get("experiment_variant"),
                    "reason_codes_csv": ",".join(p.get("reason_codes") or []),
                    "decision_status": p["decision_status"],
                }
            )

        matching_rows: list[dict[str, Any]] = []
        for rec in bus.read_range("marketplace.matching.outcomes"):
            _, m = registry.deserialize(rec.value, expected_topic="marketplace.matching.outcomes")
            matching_rows.append(
                {
                    "outcome_id": m["outcome_id"],
                    "request_id": m["request_id"],
                    "trip_id": m.get("trip_id"),
                    "rider_id": m["rider_id"],
                    "driver_id": m.get("driver_id"),
                    "city_id": m["city_id"],
                    "region_id": m["region_id"],
                    "h3_cell_res7": m["h3_cell_res7"],
                    "service_type": m["service_type"],
                    "request_time": _parse_dt(m["request_time"]),
                    "outcome_time": _parse_dt(m["outcome_time"]),
                    "final_status": m["final_status"],
                    "request_to_offer_ms": m.get("request_to_offer_ms"),
                    "offer_to_accept_ms": m.get("offer_to_accept_ms"),
                    "request_to_match_ms": m.get("request_to_match_ms"),
                    "offers_sent": int(m["offers_sent"]),
                    "offers_rejected": int(m["offers_rejected"]),
                    "pricing_multiplier": float(m["pricing_multiplier"]),
                    "policy_version": m["policy_version"],
                    "experiment_variant": m.get("experiment_variant"),
                    "estimated_pickup_seconds": m.get("estimated_pickup_seconds"),
                    "trip_completed": bool(m["trip_completed"]),
                    "cancelled_by": m.get("cancelled_by"),
                }
            )

        dlq_rows: list[dict[str, Any]] = []
        for rec in bus.read_range("marketplace.dead-letter"):
            _, d = registry.deserialize(rec.value, expected_topic="marketplace.dead-letter")
            dlq_rows.append(
                {
                    "dlq_id": d["dlq_id"],
                    "source_topic": d["source_topic"],
                    "error_code": d["error_code"],
                    "error_message": d["error_message"],
                    "quarantined_at": _parse_dt(d["quarantined_at"]),
                    "raw_payload_b64": d["raw_payload_b64"],
                }
            )

        self.append_pylist("bronze.raw_events", raw_rows)
        self.append_pylist("bronze.pricing_decisions_raw", pricing_rows)
        self.append_pylist("bronze.matching_outcomes_raw", matching_rows)
        self.append_pylist("bronze.dead_letter", dlq_rows)

        self.lineage.emit(
            event_type="COMPLETE",
            job_name="marketplace_bronze_ingestion",
            run_id=run_id,
            outputs=[
                {"name": "bronze.raw_events", "row_count": len(raw_rows)},
                {"name": "bronze.pricing_decisions_raw", "row_count": len(pricing_rows)},
                {"name": "bronze.matching_outcomes_raw", "row_count": len(matching_rows)},
            ],
        )
        return {
            "raw_events": len(raw_rows),
            "pricing_decisions": len(pricing_rows),
            "matching_outcomes": len(matching_rows),
            "dlq_records": len(dlq_rows),
            "duration_seconds": round(time.perf_counter() - t0, 4),
        }

    def materialize_cdc_policy_scd2(self) -> dict[str, Any]:
        """Build SCD Type 2 `silver.dim_pricing_policy_scd2` from policy history (simulating Debezium CDC)."""
        t0 = datetime(2026, 11, 1, 0, 0, 0)
        t1 = datetime(2026, 11, 24, 16, 0, 0)
        rows: list[dict[str, Any]] = []
        lsn = 5000
        for region_id in sorted({c.region_id for c in CITIES.values()}):
            p1 = POLICY_CATALOG["policy-v1.0"]
            p2 = POLICY_CATALOG["policy-v2.0-smoothed"]
            lsn += 1
            rows.append(
                {
                    "policy_sk": f"PSK-{region_id}-v1",
                    "region_id": region_id,
                    "policy_version": p1.policy_version,
                    "min_multiplier": p1.min_multiplier,
                    "max_multiplier": p1.max_multiplier,
                    "smoothing_alpha": p1.smoothing_alpha,
                    "neighbor_supply_weight": p1.neighbor_supply_weight,
                    "effective_from": t0,
                    "effective_to": t1,
                    "is_current": False,
                    "cdc_lsn": lsn,
                }
            )
            lsn += 1
            rows.append(
                {
                    "policy_sk": f"PSK-{region_id}-v2",
                    "region_id": region_id,
                    "policy_version": p2.policy_version,
                    "min_multiplier": p2.min_multiplier,
                    "max_multiplier": p2.max_multiplier,
                    "smoothing_alpha": p2.smoothing_alpha,
                    "neighbor_supply_weight": p2.neighbor_supply_weight,
                    "effective_from": t1,
                    "effective_to": None,
                    "is_current": True,
                    "cdc_lsn": lsn,
                }
            )
        self.overwrite_pylist("silver.dim_pricing_policy_scd2", rows)
        return {"policy_scd2_rows": len(rows)}

    def curate_silver_and_gold(self) -> dict[str, Any]:
        """Deduplicate Bronze facts into Silver and materialize all Gold analytical tables."""
        t0 = time.perf_counter()
        run_id = str(uuid.uuid4())
        self.lineage.emit(
            event_type="START",
            job_name="marketplace_silver_gold_curation",
            run_id=run_id,
            inputs=[{"name": "bronze.pricing_decisions_raw"}, {"name": "bronze.matching_outcomes_raw"}],
            outputs=[
                {"name": "silver.fact_pricing_decision"},
                {"name": "silver.fact_matching_outcome"},
                {"name": "gold.city_supply_demand_summary"},
                {"name": "gold.matching_conversion_funnel"},
                {"name": "gold.h3_cell_marketplace_state"},
                {"name": "gold.pricing_experiment_results"},
            ],
        )

        # 1. Deduplicate pricing decisions by decision_id
        raw_p = self.read_arrow("bronze.pricing_decisions_raw").to_pylist()
        p_dedup = {r["decision_id"]: r for r in raw_p}
        silver_p = list(p_dedup.values())
        self.overwrite_pylist("silver.fact_pricing_decision", silver_p)

        # 2. Deduplicate matching outcomes by request_id (latest outcome_time wins)
        raw_m = self.read_arrow("bronze.matching_outcomes_raw").to_pylist()
        m_dedup: dict[str, dict[str, Any]] = {}
        for r in raw_m:
            rid = r["request_id"]
            if rid not in m_dedup or r["outcome_time"] >= m_dedup[rid]["outcome_time"]:
                m_dedup[rid] = r
        silver_m = list(m_dedup.values())
        self.overwrite_pylist("silver.fact_matching_outcome", silver_m)

        # 3. gold.h3_cell_marketplace_state (latest decision version per H3 cell)
        latest_by_cell: dict[str, dict[str, Any]] = {}
        for p in silver_p:
            c = p["h3_cell"]
            if c not in latest_by_cell or p["decision_version"] >= latest_by_cell[c]["decision_version"]:
                latest_by_cell[c] = p

        h3_gold: list[dict[str, Any]] = []
        for c, p in sorted(latest_by_cell.items()):
            clat, clon = h3_center(c)
            eff_sup = max(1.0, float(p["supply_count"]) + 0.25 * float(p["neighbor_supply_count"]))
            h3_gold.append(
                {
                    "city_id": p["city_id"],
                    "region_id": p["region_id"],
                    "h3_cell": c,
                    "h3_coarse_res5": p["h3_coarse_res5"],
                    "center_lat": round(clat, 5),
                    "center_lon": round(clon, 5),
                    "demand_count": p["demand_count"],
                    "supply_count": p["supply_count"],
                    "neighbor_supply_count": p["neighbor_supply_count"],
                    "imbalance_ratio": round(p["demand_count"] / eff_sup, 3),
                    "pricing_multiplier": p["pricing_multiplier"],
                    "policy_version": p["policy_version"],
                    "experiment_variant": p.get("experiment_variant") or "control",
                    "reason_codes_csv": p["reason_codes_csv"],
                    "decision_status": p["decision_status"],
                    "decision_time": p["decision_time"],
                }
            )
        self.overwrite_pylist("gold.h3_cell_marketplace_state", h3_gold)

        # 4. gold.matching_conversion_funnel (by city_id x service_type)
        funnel_grp: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for m in silver_m:
            funnel_grp[(m["city_id"], m["service_type"])].append(m)

        funnel_rows: list[dict[str, Any]] = []
        for (cid, svc), grp in sorted(funnel_grp.items()):
            tot = len(grp)
            offered = sum(1 for x in grp if x["offers_sent"] > 0)
            matched = sum(1 for x in grp if x["trip_id"] is not None)
            completed = sum(1 for x in grp if x["trip_completed"])
            rcan = sum(1 for x in grp if x.get("cancelled_by") == "rider")
            dcan = sum(1 for x in grp if x.get("cancelled_by") == "driver")
            unful = sum(1 for x in grp if x["final_status"] in ("SEARCHING", "OFFERED", "EXPIRED"))
            tot_offers = sum(x["offers_sent"] for x in grp)
            tot_acc = max(0, tot_offers - sum(x["offers_rejected"] for x in grp))
            r2o_vals = [x["request_to_offer_ms"] for x in grp if x.get("request_to_offer_ms") is not None]
            r2m_vals = [x["request_to_match_ms"] for x in grp if x.get("request_to_match_ms") is not None]
            eta_vals = [x["estimated_pickup_seconds"] for x in grp if x.get("estimated_pickup_seconds") is not None]
            funnel_rows.append(
                {
                    "city_id": cid,
                    "service_type": svc,
                    "requests_total": tot,
                    "requests_offered": offered,
                    "requests_matched": matched,
                    "trips_completed": completed,
                    "rider_cancellations": rcan,
                    "driver_cancellations": dcan,
                    "unfulfilled_requests": unful,
                    "driver_acceptance_rate_pct": round((tot_acc / max(1, tot_offers)) * 100.0, 2),
                    "match_success_rate_pct": round((matched / max(1, tot)) * 100.0, 2),
                    "avg_request_to_offer_ms": round(sum(r2o_vals) / max(1, len(r2o_vals)), 1),
                    "avg_request_to_match_ms": round(sum(r2m_vals) / max(1, len(r2m_vals)), 1),
                    "avg_estimated_pickup_s": round(sum(eta_vals) / max(1, len(eta_vals)), 1),
                }
            )
        self.overwrite_pylist("gold.matching_conversion_funnel", funnel_rows)

        # 5. gold.city_supply_demand_summary
        city_cells: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for h in h3_gold:
            city_cells[h["city_id"]].append(h)
        city_reqs: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for m in silver_m:
            city_reqs[m["city_id"]].append(m)

        city_rows: list[dict[str, Any]] = []
        for cid, cspec in sorted(CITIES.items()):
            cells = city_cells.get(cid, [])
            reqs = city_reqs.get(cid, [])
            tot_r = len(reqs)
            mat_r = sum(1 for x in reqs if x["trip_id"] is not None)
            cmp_r = sum(1 for x in reqs if x["trip_completed"])
            avail_d = sum(x["supply_count"] for x in cells)
            mults = [x["pricing_multiplier"] for x in cells] or [1.0]
            match_pct = (mat_r / max(1, tot_r)) * 100.0
            comp_pct = (cmp_r / max(1, tot_r)) * 100.0
            # Composite liquidity index (0..100): match success + completion + balance
            liq = round(min(100.0, 0.55 * match_pct + 0.45 * comp_pct), 2)
            city_rows.append(
                {
                    "city_id": cid,
                    "region_id": cspec.region_id,
                    "active_h3_cells": len(cells),
                    "total_requests": tot_r,
                    "matched_requests": mat_r,
                    "completed_trips": cmp_r,
                    "available_drivers": avail_d,
                    "avg_pricing_multiplier": round(sum(mults) / len(mults), 3),
                    "max_pricing_multiplier": round(max(mults), 2),
                    "match_rate_pct": round(match_pct, 2),
                    "completion_rate_pct": round(comp_pct, 2),
                    "liquidity_score": liq,
                }
            )
        self.overwrite_pylist("gold.city_supply_demand_summary", city_rows)

        # 6. gold.pricing_experiment_results (with chi-squared Sample Ratio Mismatch check!)
        by_var_req: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for m in silver_m:
            by_var_req[m.get("experiment_variant") or "control"].append(m)
        by_var_cell: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for h in h3_gold:
            by_var_cell[h["experiment_variant"]].append(h)

        srm = compute_chi_square_srm(
            len(by_var_cell.get("control", [])), len(by_var_cell.get("treatment", []))
        )
        exp_rows: list[dict[str, Any]] = []
        for variant, pol_name in (("control", "policy-v1.0"), ("treatment", "policy-v2.0-smoothed")):
            reqs = by_var_req.get(variant, [])
            cells = by_var_cell.get(variant, [])
            tot = len(reqs)
            mat = sum(1 for x in reqs if x["trip_id"] is not None)
            cmp = sum(1 for x in reqs if x["trip_completed"])
            can = sum(1 for x in reqs if x.get("cancelled_by") is not None)
            r2m = [x["request_to_match_ms"] for x in reqs if x.get("request_to_match_ms") is not None]
            mults = [x["pricing_multiplier"] for x in cells] or [1.0]
            exp_rows.append(
                {
                    "experiment_id": DEFAULT_EXPERIMENT_ID,
                    "experiment_variant": variant,
                    "policy_version": pol_name,
                    "cells_exposed": len(cells),
                    "requests_exposed": tot,
                    "match_rate_pct": round((mat / max(1, tot)) * 100.0, 2),
                    "completion_rate_pct": round((cmp / max(1, tot)) * 100.0, 2),
                    "cancellation_rate_pct": round((can / max(1, tot)) * 100.0, 2),
                    "avg_pricing_multiplier": round(sum(mults) / len(mults), 3),
                    "avg_request_to_match_ms": round(sum(r2m) / max(1, len(r2m)), 1),
                    "srm_chi2_stat": float(srm["chi2_stat"]),
                    "srm_status": str(srm["status"]),
                }
            )
        self.overwrite_pylist("gold.pricing_experiment_results", exp_rows)

        self.lineage.emit(
            event_type="COMPLETE",
            job_name="marketplace_silver_gold_curation",
            run_id=run_id,
            outputs=[
                {"name": "gold.h3_cell_marketplace_state", "row_count": len(h3_gold)},
                {"name": "gold.matching_conversion_funnel", "row_count": len(funnel_rows)},
                {"name": "gold.pricing_experiment_results", "row_count": len(exp_rows)},
            ],
        )
        return {
            "silver_pricing_rows": len(silver_p),
            "silver_matching_rows": len(silver_m),
            "gold_h3_cells": len(h3_gold),
            "gold_funnel_rows": len(funnel_rows),
            "gold_experiment_rows": len(exp_rows),
            "srm_check": srm,
            "duration_seconds": round(time.perf_counter() - t0, 4),
        }

    def reconcile_serving_store(
        self,
        store: MaterializedStateStore,
        bus: EventBus,
        registry: AvroContractRegistry,
    ) -> dict[str, Any]:
        """Detect drift between authoritative Iceberg Gold cell pricing and low-latency serving KV;
        repair any mismatched or missing keys deterministically (Acceptance Criterion O).
        """
        gold_cells = self.read_arrow("gold.h3_cell_marketplace_state").to_pylist()
        drifted_cells: list[str] = []
        missing_cells: list[str] = []

        for g in gold_cells:
            cell = g["h3_cell"]
            cur = store.get(f"mkt:v1:pricing:h3:{cell}")
            if cur is None:
                missing_cells.append(cell)
            else:
                served_mult = float(cur["payload"].get("pricing_multiplier", -1.0))
                if not math.isclose(served_mult, float(g["pricing_multiplier"]), abs_tol=1e-4):
                    drifted_cells.append(cell)

        repaired = 0
        if drifted_cells or missing_cells:
            materializer = StateMaterializer(bus, registry, store)
            materializer.materialize_from_durable_log(wipe_first=True)
            repaired = len(drifted_cells) + len(missing_cells)
            self.metrics.reconciliation_drift_total.labels(entity_type="h3_pricing_cell").inc(repaired)

        return {
            "cells_checked": len(gold_cells),
            "missing_cells_detected": len(missing_cells),
            "drifted_cells_detected": len(drifted_cells),
            "cells_repaired": repaired,
            "status": "REPAIRED" if repaired > 0 else "CONSISTENT",
        }

    def run_data_quality(self, processor: MarketplaceStreamProcessor | None = None) -> dict[str, Any]:
        """Run Section 20 Marketplace Data Quality & Invariant Suite and persist to Iceberg."""
        run_id = str(uuid.uuid4())
        now = datetime.now(UTC).replace(tzinfo=None)
        checks: list[dict[str, Any]] = []

        def record(layer: str, name: str, passed: bool, obs: float, th: float, details: dict[str, Any]) -> None:
            st = "PASS" if passed else "FAIL"
            checks.append(
                {
                    "run_id": run_id,
                    "executed_at": now,
                    "layer": layer,
                    "check_name": name,
                    "status": st,
                    "observed_value": round(obs, 4),
                    "threshold": th,
                    "details_json": json.dumps(details, default=str),
                }
            )
            self.metrics.dq_checks_total.labels(check_name=name, status=st).inc()

        # 1. Pricing multiplier policy bounds compliance
        bad_prices = self.query_sql(
            "SELECT COUNT(*) AS cnt FROM silver_pricing WHERE pricing_multiplier < 1.0 OR pricing_multiplier > 3.0"
        )[0]["cnt"]
        record("pricing", "pricing_policy_bounds_compliance", bad_prices == 0, bad_prices, 0.0, {"out_of_bounds": bad_prices})

        # 2. Non-negative supply & demand in Gold H3 cells
        neg_cells = self.query_sql(
            "SELECT COUNT(*) AS cnt FROM gold_h3_state WHERE supply_count < 0 OR demand_count < 0"
        )[0]["cnt"]
        record("geospatial", "nonnegative_supply_and_demand", neg_cells == 0, neg_cells, 0.0, {"negative_cells": neg_cells})

        # 3. Matching outcome uniqueness on request_id
        m_uniq = self.query_sql(
            "SELECT COUNT(*) - COUNT(DISTINCT request_id) AS dups FROM silver_matching"
        )[0]["dups"]
        record("silver", "matching_request_id_uniqueness", m_uniq == 0, m_uniq, 0.0, {"duplicate_request_ids": m_uniq})

        # 4. Pricing decision uniqueness on decision_id
        p_uniq = self.query_sql(
            "SELECT COUNT(*) - COUNT(DISTINCT decision_id) AS dups FROM silver_pricing"
        )[0]["dups"]
        record("silver", "pricing_decision_id_uniqueness", p_uniq == 0, p_uniq, 0.0, {"duplicate_decision_ids": p_uniq})

        # 5. Valid city completeness (3 metro cities)
        cities_cnt = self.query_sql("SELECT COUNT(DISTINCT city_id) AS cnt FROM gold_city_summary")[0]["cnt"]
        record("gold", "city_completeness", cities_cnt >= 3, cities_cnt, 3.0, {"cities_present": cities_cnt})

        # 6. Zero double-counted drivers & zero phantom active demand (when live processor passed)
        if processor is not None:
            inv_ok = True
            try:
                processor.assert_state_invariants()
            except AssertionError:
                inv_ok = False
            record(
                "streaming",
                "zero_double_counted_drivers_and_active_requests",
                inv_ok,
                0.0 if inv_ok else 1.0,
                0.0,
                {
                    "available_drivers": sum(processor.supply_by_cell.values()),
                    "active_requests": sum(processor.demand_by_cell.values()),
                },
            )

        # 7. DLQ poison rate under 5%
        b_cnt = self.query_sql("SELECT COUNT(*) AS cnt FROM bronze_raw_events")[0]["cnt"]
        d_cnt = self.query_sql("SELECT COUNT(*) AS cnt FROM bronze_dlq")[0]["cnt"]
        dlq_pct = (d_cnt / max(1, b_cnt + d_cnt)) * 100.0
        record("bronze", "dlq_poison_rate_pct", dlq_pct <= 5.0, dlq_pct, 5.0, {"dlq_count": d_cnt, "bronze_count": b_cnt})

        # 8. Funnel vs city summary request reconciliation
        city_req_sum = self.query_sql("SELECT COALESCE(SUM(total_requests), 0) AS s FROM gold_city_summary")[0]["s"]
        fun_req_sum = self.query_sql("SELECT COALESCE(SUM(requests_total), 0) AS s FROM gold_funnel")[0]["s"]
        diff = abs(int(city_req_sum) - int(fun_req_sum))
        record("reconciliation", "city_summary_to_funnel_reconciliation", diff == 0, diff, 0.0, {"city_sum": city_req_sum, "funnel_sum": fun_req_sum})

        self.append_pylist("quality.dq_check_results", checks)
        passed = sum(1 for c in checks if c["status"] == "PASS")
        failed = len(checks) - passed
        return {
            "run_id": run_id,
            "executed_at": now.isoformat(),
            "overall_status": "PASS" if failed == 0 else "FAIL",
            "summary": {"passed": passed, "failed": failed, "total": len(checks)},
            "checks": checks,
        }

    def expire_old_snapshots(self, ident: str, *, keep_last_n: int = 5) -> dict[str, Any]:
        tbl = self.load_table(ident)
        snaps = list(tbl.snapshots())
        if len(snaps) > keep_last_n and hasattr(tbl, "maintenance"):
            try:
                tbl.maintenance.expire_snapshots().older_than(datetime.now(UTC) - timedelta(days=7)).commit()
            except Exception:
                pass
        return {"table": ident, "snapshots_after": len(list(self.load_table(ident).snapshots()))}
