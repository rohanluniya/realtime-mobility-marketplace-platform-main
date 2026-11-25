"""FastAPI Real-Time Mobility Marketplace Serving Layer & Control Tower Host (Section 23).

Explicitly separates:
1. Low-Latency Live Serving Endpoints (`serving_tier="live_kv"`): read directly from the
   version-aware Valkey / local SQLite WAL materialized state store with safe 1.0x fallback.
2. Analytical Lakehouse Endpoints (`serving_tier="lakehouse_iceberg"`): query curated
   Apache Iceberg V2 Gold & Silver tables via DuckDB SQL.
3. Privacy Invariant: all public/dashboard endpoints return H3-cell or city-tier aggregates
   only — individual rider/driver GPS histories are never exposed.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST

# Ensure local sibling & repo-root imports work regardless of uvicorn --app-dir or CWD
_API_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _API_DIR.parent.parent
for _p in (str(_REPO_ROOT), str(_API_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from security import Principal, require_role  # noqa: E402

from marketplace_platform.config import MarketplaceConfig, load_config  # noqa: E402
from marketplace_platform.contracts import TOPIC_SPECS, AvroContractRegistry  # noqa: E402
from marketplace_platform.geospatial import (  # noqa: E402
    CITIES,
    export_cities_basemap_geojson,
    h3_boundary_ring,
    h3_center,
)
from marketplace_platform.lakehouse import MarketplaceLakehouse  # noqa: E402
from marketplace_platform.observability import get_logger, get_metrics  # noqa: E402
from marketplace_platform.serving_store import MaterializedStateStore, ServingStoreUnavailableError  # noqa: E402
from marketplace_platform.state_engine import MarketplaceStreamProcessor  # noqa: E402
from marketplace_platform.transport import create_event_bus  # noqa: E402

DASHBOARD_DIR = _API_DIR.parent / "marketplace-dashboard"

app = FastAPI(
    title="Real-Time Mobility Marketplace & Dynamic Pricing Platform",
    version="1.0.0",
    description=(
        "Production-style serving API exposing low-latency H3 supply/demand/pricing state "
        "alongside Apache Iceberg V2 analytical funnels, A/B pricing experiments, and platform health."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

_logger = get_logger("marketplace-api", "serving")
_metrics = get_metrics()


class _Runtime:
    def __init__(self) -> None:
        self.cfg: MarketplaceConfig = load_config()
        self.bus = create_event_bus(self.cfg)
        self.registry = AvroContractRegistry(self.cfg.project_root / "schemas" / "avro")
        self.store = MaterializedStateStore(self.cfg)
        self.lakehouse = MarketplaceLakehouse(self.cfg)
        self.last_reconciliation: dict[str, Any] = {
            "status": "CONSISTENT",
            "cells_checked": 0,
            "drifted_cells_detected": 0,
            "missing_cells_detected": 0,
            "cells_repaired": 0,
            "checked_at_utc": datetime.now(UTC).isoformat(),
        }

    def refresh(self) -> None:
        self.cfg = load_config()
        self.bus = create_event_bus(self.cfg)
        self.store = MaterializedStateStore(self.cfg)
        self.lakehouse = MarketplaceLakehouse(self.cfg)


_rt = _Runtime()


@app.middleware("http")
async def _instrument_requests(request: Request, call_next: Any) -> Response:
    t0 = time.perf_counter()
    response: Response = await call_next(request)
    dur = time.perf_counter() - t0
    _metrics.api_requests_total.labels(
        endpoint=request.url.path,
        method=request.method,
        status_code=str(response.status_code),
        role="api",
    ).inc()
    _metrics.api_latency_seconds.labels(endpoint=request.url.path).observe(dur)
    response.headers["X-Response-Time-Ms"] = f"{dur * 1000.0:.2f}"
    response.headers["X-Data-Notice"] = "Synthetic-Marketplace-Simulation"
    return response


def _freshness_now() -> str:
    return datetime.now(UTC).isoformat()


def _safe_live_pricing(city_id: str | None = None, limit: int = 500) -> tuple[list[dict[str, Any]], str]:
    try:
        items = _rt.store.list_by_prefix("mkt:v1:pricing:h3:", limit=limit)
        if city_id:
            items = [x for x in items if x.get("city_id") == city_id]
        return items, "LIVE_KV"
    except ServingStoreUnavailableError:
        return [], "FALLBACK_STORE_OFFLINE"


def _safe_live_agg(prefix: str, city_id: str | None = None, limit: int = 500) -> tuple[list[dict[str, Any]], str]:
    try:
        items = _rt.store.list_by_prefix(prefix, limit=limit)
        if city_id:
            items = [x for x in items if x.get("city_id") == city_id]
        return items, "LIVE_KV"
    except ServingStoreUnavailableError:
        return [], "FALLBACK_STORE_OFFLINE"


# =============================================================================
# 1. Health & Observability Endpoints
# =============================================================================


@app.get("/health")
def get_health() -> dict[str, Any]:
    store_stats = _rt.store.stats()
    table_stats = _rt.lakehouse.all_table_stats()
    iceberg_rows = sum(int(t["total_records"]) for t in table_stats)
    return {
        "status": "healthy" if store_stats.get("online") else "degraded_serving_fallback",
        "service": "realtime-mobility-marketplace-api",
        "execution_profile": _rt.cfg.execution_profile,
        "serving_tier": "hybrid",
        "serving_store": store_stats,
        "lakehouse": {
            "tables": len(table_stats),
            "total_records": iceberg_rows,
            "format_version": 2,
        },
        "cities_supported": sorted(CITIES.keys()),
        "synthetic_data_notice": "All marketplace events and coordinates are deterministically simulated.",
        "freshness_utc": _freshness_now(),
    }


@app.get("/metrics")
def get_prometheus_metrics() -> Response:
    return Response(content=_metrics.render_prometheus(), media_type=CONTENT_TYPE_LATEST)


# =============================================================================
# 2. Executive Summary (Hybrid Live KV + Iceberg Gold)
# =============================================================================


@app.get("/marketplace/summary")
def get_marketplace_summary(
    city_id: str | None = Query(default=None),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    where = "WHERE city_id = ?" if city_id else ""
    params = [city_id] if city_id else []
    city_rows = _rt.lakehouse.query_sql(
        f"SELECT * FROM gold_city_summary {where} ORDER BY city_id", params
    )
    pricing_live, store_status = _safe_live_pricing(city_id=city_id)

    total_requests = sum(int(r["total_requests"]) for r in city_rows)
    matched_requests = sum(int(r["matched_requests"]) for r in city_rows)
    completed_trips = sum(int(r["completed_trips"]) for r in city_rows)
    avail_drivers = sum(int(r["available_drivers"]) for r in city_rows)
    mults = [float(p["pricing_multiplier"]) for p in pricing_live] or [1.0]
    surged_cells = sum(1 for m in mults if m > 1.2)

    return {
        "serving_tier": "hybrid",
        "store_status": store_status,
        "filter_city": city_id or "all",
        "kpis": {
            "active_h3_cells": len(pricing_live) or sum(int(r["active_h3_cells"]) for r in city_rows),
            "available_drivers": avail_drivers,
            "total_requests": total_requests,
            "matched_requests": matched_requests,
            "completed_trips": completed_trips,
            "match_rate_pct": round((matched_requests / max(1, total_requests)) * 100.0, 2),
            "completion_rate_pct": round((completed_trips / max(1, total_requests)) * 100.0, 2),
            "avg_pricing_multiplier": round(sum(mults) / len(mults), 3),
            "max_pricing_multiplier": round(max(mults), 2),
            "surged_cells_gt_1_2x": surged_cells,
            "avg_liquidity_score": round(
                sum(float(r["liquidity_score"]) for r in city_rows) / max(1, len(city_rows)), 2
            ),
        },
        "cities": city_rows,
        "lookup_latency_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        "freshness_utc": _freshness_now(),
    }


# =============================================================================
# 3-6. Low-Latency Live Serving Store Endpoints (Valkey / Local KV)
# =============================================================================


@app.get("/marketplace/demand")
def get_marketplace_demand(
    city_id: str | None = Query(default=None),
    min_demand: int = Query(default=0, ge=0),
    limit: int = Query(default=250, ge=1, le=1000),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    rows, store_status = _safe_live_agg("mkt:v1:demand:h3:", city_id=city_id, limit=1000)
    enriched = []
    for r in rows:
        cnt = int(r.get("count", 0))
        if cnt < min_demand:
            continue
        lat, lon = h3_center(r["h3_cell"])
        enriched.append(
            {
                "h3_cell": r["h3_cell"],
                "h3_resolution": r.get("h3_resolution", 7),
                "city_id": r["city_id"],
                "region_id": r["region_id"],
                "demand_count": cnt,
                "center_lat": round(lat, 5),
                "center_lon": round(lon, 5),
                "state_version": r.get("state_version", 1),
                "window_end": r.get("window_end"),
            }
        )
    enriched.sort(key=lambda x: x["demand_count"], reverse=True)
    return {
        "serving_tier": "live_kv",
        "store_status": store_status,
        "total_cells": len(enriched),
        "total_demand": sum(x["demand_count"] for x in enriched),
        "cells": enriched[:limit],
        "lookup_latency_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        "freshness_utc": _freshness_now(),
    }


@app.get("/marketplace/supply")
def get_marketplace_supply(
    city_id: str | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    rows, store_status = _safe_live_agg("mkt:v1:supply:h3:", city_id=city_id, limit=1000)
    enriched = []
    for r in rows:
        lat, lon = h3_center(r["h3_cell"])
        enriched.append(
            {
                "h3_cell": r["h3_cell"],
                "h3_resolution": r.get("h3_resolution", 7),
                "city_id": r["city_id"],
                "region_id": r["region_id"],
                "available_drivers": int(r.get("count", 0)),
                "center_lat": round(lat, 5),
                "center_lon": round(lon, 5),
                "state_version": r.get("state_version", 1),
                "window_end": r.get("window_end"),
            }
        )
    enriched.sort(key=lambda x: x["available_drivers"], reverse=True)

    # Also summarize driver state distribution from checkpoint (aggregated counts only, zero PII)
    ckpt_path = _rt.cfg.checkpoint_dir / "marketplace_ckpt.json"
    state_dist: dict[str, int] = {}
    if ckpt_path.exists():
        ckpt = json.loads(ckpt_path.read_text(encoding="utf-8"))
        for drv in ckpt.get("drivers", {}).values():
            if city_id and drv.get("city_id") != city_id:
                continue
            st = str(drv.get("state", "UNKNOWN"))
            state_dist[st] = state_dist.get(st, 0) + 1

    return {
        "serving_tier": "live_kv",
        "store_status": store_status,
        "total_cells": len(enriched),
        "total_available_drivers": sum(x["available_drivers"] for x in enriched),
        "driver_state_distribution": state_dist,
        "cells": enriched[:limit],
        "lookup_latency_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        "freshness_utc": _freshness_now(),
    }


@app.get("/marketplace/imbalance")
def get_marketplace_imbalance(
    city_id: str | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    pricing_live, store_status = _safe_live_pricing(city_id=city_id, limit=1000)
    rows = []
    for p in pricing_live:
        d = int(p.get("demand_count", 0))
        s = int(p.get("supply_count", 0))
        nb = int(p.get("neighbor_supply_count", 0))
        eff_supply = max(1.0, float(s) + 0.35 * float(nb))
        ratio = round(d / eff_supply, 3)
        lat, lon = h3_center(p["h3_cell"])
        severity = "CRITICAL_SHORTAGE" if ratio >= 2.5 else ("ELEVATED_DEFICIT" if ratio >= 1.3 else ("SURPLUS" if ratio < 0.7 else "BALANCED"))
        rows.append(
            {
                "h3_cell": p["h3_cell"],
                "city_id": p["city_id"],
                "region_id": p["region_id"],
                "demand_count": d,
                "supply_count": s,
                "neighbor_supply_count": nb,
                "effective_supply": round(eff_supply, 2),
                "imbalance_ratio": ratio,
                "net_gap": d - s,
                "severity": severity,
                "pricing_multiplier": float(p["pricing_multiplier"]),
                "estimated_wait_seconds": float(p.get("estimated_wait_seconds", 0.0)),
                "center_lat": round(lat, 5),
                "center_lon": round(lon, 5),
            }
        )
    rows.sort(key=lambda x: (x["imbalance_ratio"], x["demand_count"]), reverse=True)
    return {
        "serving_tier": "live_kv",
        "store_status": store_status,
        "total_cells": len(rows),
        "critical_shortage_cells": sum(1 for r in rows if r["severity"] == "CRITICAL_SHORTAGE"),
        "elevated_deficit_cells": sum(1 for r in rows if r["severity"] == "ELEVATED_DEFICIT"),
        "balanced_or_surplus_cells": sum(1 for r in rows if r["severity"] in ("BALANCED", "SURPLUS")),
        "cells": rows[:limit],
        "lookup_latency_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        "freshness_utc": _freshness_now(),
    }


@app.get("/marketplace/pricing")
def get_marketplace_pricing(
    city_id: str | None = Query(default=None),
    min_multiplier: float = Query(default=1.0, ge=1.0),
    limit: int = Query(default=300, ge=1, le=1000),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    items, store_status = _safe_live_pricing(city_id=city_id, limit=1000)
    filtered = [x for x in items if float(x.get("pricing_multiplier", 1.0)) >= min_multiplier]
    filtered.sort(key=lambda x: (float(x.get("pricing_multiplier", 1.0)), int(x.get("demand_count", 0))), reverse=True)
    for x in filtered:
        lat, lon = h3_center(x["h3_cell"])
        x["center_lat"] = round(lat, 5)
        x["center_lon"] = round(lon, 5)
    return {
        "serving_tier": "live_kv",
        "store_status": store_status,
        "total_cells": len(filtered),
        "avg_multiplier": round(
            sum(float(x["pricing_multiplier"]) for x in filtered) / max(1, len(filtered)), 3
        ),
        "max_multiplier": round(max((float(x["pricing_multiplier"]) for x in filtered), default=1.0), 2),
        "decisions": filtered[:limit],
        "lookup_latency_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        "freshness_utc": _freshness_now(),
    }


@app.get("/marketplace/pricing/{h3_cell}")
def get_cell_pricing_lookup(
    h3_cell: str,
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    """Single-cell low-latency pricing lookup with safe 1.0x outage/stale fallback."""
    t0 = time.perf_counter()
    res = _rt.store.get_pricing_with_safe_fallback(h3_cell, max_age_seconds=3600.0)
    res["serving_tier"] = "live_kv"
    res["lookup_latency_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
    return res


# =============================================================================
# 7-10. Analytical Lakehouse Endpoints (Apache Iceberg V2 via DuckDB)
# =============================================================================


@app.get("/marketplace/regions")
def get_marketplace_regions(
    city_id: str | None = Query(default=None),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    where = "WHERE city_id = ?" if city_id else ""
    params = [city_id] if city_id else []
    city_summary = _rt.lakehouse.query_sql(f"SELECT * FROM gold_city_summary {where} ORDER BY city_id", params)
    district_rollups = _rt.lakehouse.query_sql(
        f"""
        SELECT
            city_id,
            region_id,
            h3_coarse_res5 AS district_h3_res5,
            COUNT(*) AS active_res7_cells,
            SUM(demand_count) AS district_demand,
            SUM(supply_count) AS district_supply,
            ROUND(AVG(pricing_multiplier), 3) AS avg_multiplier,
            ROUND(MAX(pricing_multiplier), 2) AS max_multiplier
        FROM gold_h3_state
        {where}
        GROUP BY city_id, region_id, h3_coarse_res5
        ORDER BY district_demand DESC
        LIMIT 30
        """,
        params,
    )
    policies = _rt.lakehouse.query_sql(
        "SELECT region_id, policy_version, min_multiplier, max_multiplier, smoothing_alpha, neighbor_supply_weight, is_current FROM silver_dim_policy ORDER BY region_id, policy_version"
    )
    return {
        "serving_tier": "lakehouse_iceberg",
        "cities": city_summary,
        "h3_res5_districts": district_rollups,
        "policy_scd2_history": policies,
        "freshness_utc": _freshness_now(),
    }


@app.get("/marketplace/trips")
def get_marketplace_trips(
    city_id: str | None = Query(default=None),
    service_type: str | None = Query(default=None),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[Any] = []
    if city_id:
        clauses.append("city_id = ?")
        params.append(city_id)
    if service_type:
        clauses.append("service_type = ?")
        params.append(service_type)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    status_breakdown = _rt.lakehouse.query_sql(
        f"""
        SELECT
            final_status,
            COUNT(*) AS count,
            ROUND(AVG(COALESCE(request_to_match_ms, 0)), 1) AS avg_request_to_match_ms,
            ROUND(AVG(COALESCE(estimated_pickup_seconds, 0)), 1) AS avg_pickup_eta_s,
            ROUND(AVG(pricing_multiplier), 3) AS avg_multiplier
        FROM silver_matching
        {where}
        GROUP BY final_status
        ORDER BY count DESC
        """,
        params,
    )
    by_service = _rt.lakehouse.query_sql(
        f"""
        SELECT
            service_type,
            COUNT(*) AS total_requests,
            SUM(CASE WHEN trip_id IS NOT NULL THEN 1 ELSE 0 END) AS trips_matched,
            SUM(CASE WHEN trip_completed THEN 1 ELSE 0 END) AS trips_completed,
            SUM(CASE WHEN cancelled_by = 'rider' THEN 1 ELSE 0 END) AS rider_cancellations,
            SUM(CASE WHEN cancelled_by = 'driver' THEN 1 ELSE 0 END) AS driver_cancellations,
            ROUND(AVG(estimated_pickup_seconds), 1) AS avg_pickup_eta_s,
            ROUND(AVG(request_to_match_ms), 1) AS avg_request_to_match_ms
        FROM silver_matching
        {where}
        GROUP BY service_type
        ORDER BY total_requests DESC
        """,
        params,
    )
    return {
        "serving_tier": "lakehouse_iceberg",
        "privacy_notice": "Aggregated lifecycle statistics only; individual rider/driver trajectories are redacted.",
        "status_breakdown": status_breakdown,
        "by_service_type": by_service,
        "freshness_utc": _freshness_now(),
    }


@app.get("/marketplace/matching")
def get_marketplace_matching(
    city_id: str | None = Query(default=None),
    service_type: str | None = Query(default=None),
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[Any] = []
    if city_id:
        clauses.append("city_id = ?")
        params.append(city_id)
    if service_type:
        clauses.append("service_type = ?")
        params.append(service_type)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    rows = _rt.lakehouse.query_sql(f"SELECT * FROM gold_funnel {where} ORDER BY city_id, service_type", params)
    tot = sum(int(r["requests_total"]) for r in rows)
    off = sum(int(r["requests_offered"]) for r in rows)
    mat = sum(int(r["requests_matched"]) for r in rows)
    cmp = sum(int(r["trips_completed"]) for r in rows)
    rcan = sum(int(r["rider_cancellations"]) for r in rows)
    dcan = sum(int(r["driver_cancellations"]) for r in rows)
    unful = sum(int(r["unfulfilled_requests"]) for r in rows)

    return {
        "serving_tier": "lakehouse_iceberg",
        "aggregate_funnel": {
            "requests_total": tot,
            "requests_offered": off,
            "requests_matched": mat,
            "trips_completed": cmp,
            "rider_cancellations": rcan,
            "driver_cancellations": dcan,
            "unfulfilled_requests": unful,
            "offer_rate_pct": round((off / max(1, tot)) * 100.0, 2),
            "match_rate_pct": round((mat / max(1, tot)) * 100.0, 2),
            "completion_rate_pct": round((cmp / max(1, tot)) * 100.0, 2),
            "avg_request_to_offer_ms": round(
                sum(float(r["avg_request_to_offer_ms"]) * int(r["requests_total"]) for r in rows) / max(1, tot), 1
            ),
            "avg_request_to_match_ms": round(
                sum(float(r["avg_request_to_match_ms"]) * int(r["requests_total"]) for r in rows) / max(1, tot), 1
            ),
            "avg_estimated_pickup_s": round(
                sum(float(r["avg_estimated_pickup_s"]) * int(r["requests_total"]) for r in rows) / max(1, tot), 1
            ),
        },
        "by_city_service": rows,
        "freshness_utc": _freshness_now(),
    }


@app.get("/marketplace/experiments")
def get_marketplace_experiments(
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    rows = _rt.lakehouse.query_sql("SELECT * FROM gold_experiments ORDER BY experiment_id, experiment_variant")
    control = next((r for r in rows if r["experiment_variant"] == "control"), None)
    treatment = next((r for r in rows if r["experiment_variant"] == "treatment"), None)
    lift: dict[str, Any] = {}
    if control and treatment:
        lift = {
            "match_rate_delta_pp": round(float(treatment["match_rate_pct"]) - float(control["match_rate_pct"]), 2),
            "completion_rate_delta_pp": round(
                float(treatment["completion_rate_pct"]) - float(control["completion_rate_pct"]), 2
            ),
            "avg_multiplier_delta": round(
                float(treatment["avg_pricing_multiplier"]) - float(control["avg_pricing_multiplier"]), 3
            ),
            "match_latency_delta_ms": round(
                float(treatment["avg_request_to_match_ms"]) - float(control["avg_request_to_match_ms"]), 1
            ),
            "srm_status": treatment["srm_status"],
            "srm_chi2_stat": treatment["srm_chi2_stat"],
        }
    return {
        "serving_tier": "lakehouse_iceberg",
        "randomization_unit": "h3_res7_cell_cluster",
        "variants": rows,
        "lift_summary": lift,
        "freshness_utc": _freshness_now(),
    }


# =============================================================================
# 11-12. Platform Metrics, Streaming Health, Data Quality & Incidents
# =============================================================================


@app.get("/platform/metrics")
def get_platform_metrics(
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    topic_metrics = []
    for t in TOPIC_SPECS.values():
        recs = _rt.bus.read_range(t.name)
        lag_info = _rt.bus.get_consumer_lag(t.name, MarketplaceStreamProcessor.CONSUMER_GROUP)
        topic_metrics.append(
            {
                "topic": t.name,
                "partitions": t.partitions,
                "retention_hours": t.retention_ms // 3_600_000,
                "records_retained": len(recs),
                "consumer_lag": int(lag_info.get("total_lag", 0)),
            }
        )

    ckpt_path = _rt.cfg.checkpoint_dir / "marketplace_ckpt.json"
    ckpt_summary: dict[str, Any] = {"available": False}
    if ckpt_path.exists():
        c = json.loads(ckpt_path.read_text(encoding="utf-8"))
        ckpt_summary = {
            "available": True,
            "checkpoint_id": c.get("checkpoint_id", 0),
            "tracked_drivers": len(c.get("drivers", {})),
            "tracked_requests": len(c.get("requests", {})),
            "active_h3_cells": len(c.get("prior_pricing", {})),
            "counters": c.get("counters", {}),
        }

    lineage_file = _rt.cfg.lineage_dir / "openlineage_events.jsonl"
    lineage_events = len(lineage_file.read_text(encoding="utf-8").splitlines()) if lineage_file.exists() else 0

    return {
        "serving_tier": "hybrid",
        "execution_profile": _rt.cfg.execution_profile,
        "transport_mode": _rt.cfg.transport_mode,
        "serving_store": _rt.store.stats(),
        "stream_checkpoint": ckpt_summary,
        "topics": topic_metrics,
        "iceberg_tables": _rt.lakehouse.all_table_stats(),
        "openlineage_events_emitted": lineage_events,
        "reconciliation": _rt.last_reconciliation,
        "freshness_utc": _freshness_now(),
    }


@app.get("/platform/data-quality")
def get_platform_data_quality(
    _: Principal = require_role("public_aggregated"),
) -> dict[str, Any]:
    checks = _rt.lakehouse.query_sql(
        """
        SELECT run_id, executed_at, layer, check_name, status, observed_value, threshold, details_json
        FROM quality_dq
        WHERE run_id = (SELECT run_id FROM quality_dq ORDER BY executed_at DESC LIMIT 1)
        ORDER BY layer, check_name
        """
    )
    passed = sum(1 for c in checks if c["status"] == "PASS")
    failed = len(checks) - passed

    # Surface recent chaos / incident drill results if present
    chaos_path = _rt.cfg.project_root / "results" / "chaos" / "chaos_results.json"
    chaos_summary: dict[str, Any] | None = None
    if chaos_path.exists():
        chaos_summary = json.loads(chaos_path.read_text(encoding="utf-8"))

    dlq_rows = _rt.lakehouse.query_sql("SELECT source_topic, error_code, COUNT(*) AS cnt FROM bronze_dlq GROUP BY 1, 2")

    return {
        "serving_tier": "lakehouse_iceberg",
        "overall_status": "PASS" if failed == 0 and len(checks) > 0 else ("NO_RUNS" if not checks else "FAIL"),
        "summary": {"passed": passed, "failed": failed, "total": len(checks)},
        "checks": checks,
        "dlq_summary": dlq_rows,
        "reconciliation_status": _rt.last_reconciliation,
        "chaos_drill_summary": chaos_summary,
        "freshness_utc": _freshness_now(),
    }


@app.post("/platform/actions/reconcile")
def trigger_reconciliation(
    _: Principal = require_role("operator"),
) -> dict[str, Any]:
    """Operator-protected action: reconcile live KV serving state against Iceberg Gold."""
    res = _rt.lakehouse.reconcile_serving_store(_rt.store, _rt.bus, _rt.registry)
    res["checked_at_utc"] = _freshness_now()
    _rt.last_reconciliation = res
    return res


# =============================================================================
# Geospatial GeoJSON Endpoints for Self-Hosted H3 Control Tower Map
# =============================================================================


@app.get("/api/map/basemap.geojson")
def get_basemap_geojson() -> JSONResponse:
    return JSONResponse(content=export_cities_basemap_geojson())


@app.get("/api/map/h3-cells.geojson")
def get_h3_cells_geojson(
    city_id: str | None = Query(default=None),
    _: Principal = require_role("public_aggregated"),
) -> JSONResponse:
    """Return H3 resolution-7 hexagonal polygons with live demand, supply, imbalance, and pricing."""
    items, store_status = _safe_live_pricing(city_id=city_id, limit=1000)
    features: list[dict[str, Any]] = []
    for p in items:
        cell = p["h3_cell"]
        d = int(p.get("demand_count", 0))
        s = int(p.get("supply_count", 0))
        nb = int(p.get("neighbor_supply_count", 0))
        eff = max(1.0, float(s) + 0.35 * float(nb))
        lat, lon = h3_center(cell)
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "h3_cell": cell,
                    "city_id": p["city_id"],
                    "region_id": p["region_id"],
                    "demand_count": d,
                    "supply_count": s,
                    "neighbor_supply_count": nb,
                    "imbalance_ratio": round(d / eff, 2),
                    "raw_multiplier": float(p.get("raw_multiplier", 1.0)),
                    "pricing_multiplier": float(p.get("pricing_multiplier", 1.0)),
                    "estimated_wait_seconds": float(p.get("estimated_wait_seconds", 0.0)),
                    "policy_version": p.get("policy_version", "policy-v1.0"),
                    "experiment_variant": p.get("experiment_variant", "control"),
                    "reason_codes": p.get("reason_codes", []),
                    "decision_status": p.get("decision_status", "ACTIVE"),
                    "center_lat": round(lat, 5),
                    "center_lon": round(lon, 5),
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [h3_boundary_ring(cell)],
                },
            }
        )
    return JSONResponse(
        content={
            "type": "FeatureCollection",
            "serving_tier": "live_kv",
            "store_status": store_status,
            "cities": {
                cid: {
                    "display_name": c.display_name,
                    "center_lat": c.center_lat,
                    "center_lon": c.center_lon,
                    "bbox": list(c.bbox),
                    "hot_zones": [
                        {"zone_id": z.zone_id, "name": z.name, "zone_type": z.zone_type, "lat": z.lat, "lon": z.lon}
                        for z in c.hot_zones
                    ],
                }
                for cid, c in CITIES.items()
            },
            "features": features,
        }
    )


# =============================================================================
# Dashboard Static Hosting
# =============================================================================


if (DASHBOARD_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(DASHBOARD_DIR / "assets")), name="assets")


@app.get("/", include_in_schema=False)
def serve_dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_DIR / "index.html")
