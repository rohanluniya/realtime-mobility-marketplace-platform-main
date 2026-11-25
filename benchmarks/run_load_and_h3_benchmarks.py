"""Sustained Load, Multi-Resolution H3 Tradeoff, and Iceberg Optimization Benchmark Suite.

Produces 100% measured, reproducible raw artifacts in `results/benchmarks/` and
publication-grade matplotlib figures in `docs/assets/`. Zero fabricated or extrapolated data.
"""

from __future__ import annotations

import csv
import json
import platform
import resource
import shutil
import statistics
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.lakehouse import MarketplaceLakehouse
from marketplace_platform.serving_store import MaterializedStateStore, StateMaterializer
from marketplace_platform.simulator import MarketplaceSimulator, SimConfig
from marketplace_platform.state_engine import MarketplaceStreamProcessor
from marketplace_platform.transport import LocalWalEventBus

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "results" / "benchmarks"
ASSETS_DIR = REPO_ROOT / "docs" / "assets"

H3_AREA_KM2 = {6: 36.13, 7: 5.16, 8: 0.74, 9: 0.11}


def _peak_rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux reports KiB
    return round(rss / (1024 * 1024) if platform.system() == "Darwin" else rss / 1024.0, 2)


def _make_cfg(base: Path) -> MarketplaceConfig:
    cfg = MarketplaceConfig(
        env="bench",
        execution_profile="local_native",
        transport_mode="local_wal",
        serving_store_mode="local_kv",
        local_data_dir=base,
        warehouse_dir=base / "warehouse",
        wal_dir=base / "wal",
        checkpoint_dir=base / "checkpoints",
        kv_store_path=base / "serving_kv.sqlite",
        lineage_dir=base / "lineage",
        sqlite_catalog_path=base / "iceberg_catalog.db",
    )
    cfg.ensure_local_dirs()
    return cfg


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round((p / 100.0) * (len(sorted_vals) - 1)))))
    return round(sorted_vals[idx], 4)


def benchmark_load_profiles() -> list[dict[str, Any]]:
    """Measure smoke, local, stress, and virtual-scale (1M virtual IDs) end-to-end profiles."""
    rows: list[dict[str, Any]] = []
    schemas_dir = REPO_ROOT / "schemas" / "avro"

    for profile in ("smoke", "local", "stress", "virtual-scale"):
        tmp = Path(tempfile.mkdtemp(prefix=f"mkt_bench_{profile}_"))
        try:
            cfg = _make_cfg(tmp)
            bus = LocalWalEventBus(cfg.wal_dir)
            reg = AvroContractRegistry(schemas_dir)

            sim_cfg = SimConfig.from_profile(profile, seed=2026)
            sim_res = MarketplaceSimulator(cfg, bus, reg, sim_cfg).run()

            proc = MarketplaceStreamProcessor(cfg, bus, reg)
            st_res = proc.process_all()

            store = MaterializedStateStore(cfg)
            mat_res = StateMaterializer(bus, reg, store).materialize_from_durable_log()

            # Measure serving KV point-lookup latency distribution (1,000 real lookups)
            pricing_keys = [p["h3_cell"] for p in store.list_by_prefix("mkt:v1:pricing:h3:", limit=1000)]
            lat_ms: list[float] = []
            if pricing_keys:
                for i in range(1000):
                    cell = pricing_keys[i % len(pricing_keys)]
                    t0 = time.perf_counter()
                    res = store.get_pricing_with_safe_fallback(cell)
                    lat_ms.append((time.perf_counter() - t0) * 1000.0)
                    assert res["serving_status"] == "LIVE_HIT"
            lat_ms.sort()

            lh = MarketplaceLakehouse(cfg)
            b_res = lh.ingest_bronze_from_bus(bus, reg)
            lh.materialize_cdc_policy_scd2()
            sg_res = lh.curate_silver_and_gold()
            dq_res = lh.run_data_quality(processor=proc)

            rows.append(
                {
                    "profile": profile,
                    "virtual_participants": sim_res["virtual_participants"],
                    "virtual_drivers": sim_res["virtual_drivers"],
                    "virtual_riders": sim_res["virtual_riders"],
                    "active_drivers": sim_res["active_drivers"],
                    "active_riders": sim_res["active_riders"],
                    "events_produced": sim_res["total_events_produced"],
                    "producer_eps": sim_res["throughput_events_per_sec"],
                    "producer_mb_s": sim_res["throughput_mb_per_sec"],
                    "stream_eps": st_res["throughput_events_per_sec"],
                    "checkpoint_ms": st_res["checkpoint"]["duration_ms"],
                    "cell_transfers": st_res["cell_transfers_processed"],
                    "active_h3_cells": st_res["active_h3_cells"],
                    "serving_kv_keys": mat_res["total_live_keys"],
                    "serving_materialize_ms": mat_res["duration_ms"],
                    "kv_lookup_p50_ms": _percentile(lat_ms, 50),
                    "kv_lookup_p95_ms": _percentile(lat_ms, 95),
                    "kv_lookup_p99_ms": _percentile(lat_ms, 99),
                    "iceberg_bronze_s": b_res["duration_seconds"],
                    "iceberg_silver_gold_s": sg_res["duration_seconds"],
                    "dq_passed": dq_res["summary"]["passed"],
                    "dq_total": dq_res["summary"]["total"],
                    "peak_rss_mb": _peak_rss_mb(),
                }
            )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    return rows


def benchmark_h3_resolutions() -> list[dict[str, Any]]:
    """Empirically compare H3 Resolutions 6, 7, 8, and 9 on the identical local marketplace event log."""
    rows: list[dict[str, Any]] = []
    schemas_dir = REPO_ROOT / "schemas" / "avro"

    # Generate ONE shared deterministic local event log so every H3 resolution sees identical events
    shared_tmp = Path(tempfile.mkdtemp(prefix="mkt_h3_shared_"))
    try:
        shared_cfg = _make_cfg(shared_tmp)
        shared_bus = LocalWalEventBus(shared_cfg.wal_dir)
        reg = AvroContractRegistry(schemas_dir)
        sim_res = MarketplaceSimulator(
            shared_cfg, shared_bus, reg, SimConfig.from_profile("local", seed=2026)
        ).run()

        for res in (6, 7, 8, 9):
            run_tmp = Path(tempfile.mkdtemp(prefix=f"mkt_h3_r{res}_"))
            try:
                shutil.copytree(shared_cfg.wal_dir, run_tmp / "wal")
                cfg = _make_cfg(run_tmp)
                bus = LocalWalEventBus(cfg.wal_dir)

                t0 = time.perf_counter()
                proc = MarketplaceStreamProcessor(cfg, bus, reg, h3_resolution=res)
                st_res = proc.process_all()
                stream_ms = (time.perf_counter() - t0) * 1000.0

                store = MaterializedStateStore(cfg)
                mat_res = StateMaterializer(bus, reg, store).materialize_from_durable_log()

                pricing_items = store.list_by_prefix("mkt:v1:pricing:h3:", limit=10_000)
                total_cells = len(pricing_items)
                zero_supply_cells = sum(1 for p in pricing_items if int(p["supply_count"]) == 0)
                surged_cells = sum(1 for p in pricing_items if float(p["pricing_multiplier"]) > 1.2)
                max_cell_demand = max((int(p["demand_count"]) for p in pricing_items), default=0)
                avg_mult = statistics.mean([float(p["pricing_multiplier"]) for p in pricing_items]) if pricing_items else 1.0

                # Measure 500 KV lookups
                lat_ms: list[float] = []
                for i in range(500):
                    cell = pricing_items[i % total_cells]["h3_cell"]
                    lk0 = time.perf_counter()
                    store.get_pricing_with_safe_fallback(cell)
                    lat_ms.append((time.perf_counter() - lk0) * 1000.0)
                lat_ms.sort()

                rows.append(
                    {
                        "h3_resolution": res,
                        "avg_hex_area_km2": H3_AREA_KM2[res],
                        "events_processed": sim_res["total_events_produced"],
                        "active_h3_cells": total_cells,
                        "driver_cell_transfers": st_res["cell_transfers_processed"],
                        "zero_supply_cell_pct": round((zero_supply_cells / max(1, total_cells)) * 100.0, 2),
                        "surged_cell_pct": round((surged_cells / max(1, total_cells)) * 100.0, 2),
                        "max_hot_cell_demand": max_cell_demand,
                        "avg_pricing_multiplier": round(avg_mult, 3),
                        "stream_process_ms": round(stream_ms, 2),
                        "checkpoint_ms": st_res["checkpoint"]["duration_ms"],
                        "serving_kv_keys": mat_res["total_live_keys"],
                        "serving_materialize_ms": mat_res["duration_ms"],
                        "kv_lookup_p95_ms": _percentile(lat_ms, 95),
                    }
                )
            finally:
                shutil.rmtree(run_tmp, ignore_errors=True)
    finally:
        shutil.rmtree(shared_tmp, ignore_errors=True)
    return rows


def benchmark_iceberg_optimizations() -> dict[str, Any]:
    """Measure small-file compaction, partition/city pruning, and hot-H3 salted vs unsalted aggregation."""
    schemas_dir = REPO_ROOT / "schemas" / "avro"
    tmp = Path(tempfile.mkdtemp(prefix="mkt_ice_opt_"))
    try:
        cfg = _make_cfg(tmp)
        bus = LocalWalEventBus(cfg.wal_dir)
        reg = AvroContractRegistry(schemas_dir)
        MarketplaceSimulator(cfg, bus, reg, SimConfig.from_profile("local", seed=2026)).run()
        MarketplaceStreamProcessor(cfg, bus, reg).process_all()
        lh = MarketplaceLakehouse(cfg)

        # Write 6 separate micro-batches into bronze.raw_events to create realistic small-file fragmentation
        for b in range(6):
            lh.ingest_bronze_from_bus(bus, reg, replay_batch_id=f"microbatch-{b}")
        lh.curate_silver_and_gold()

        stats_before = lh.table_stats("bronze.raw_events")
        t0 = time.perf_counter()
        uncompacted_cnt = lh.query_sql("SELECT COUNT(*) AS cnt FROM bronze_raw_events")[0]["cnt"]
        scan_uncompacted_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        comp = lh.compact_table("bronze.raw_events")
        t1 = time.perf_counter()
        compacted_cnt = lh.query_sql("SELECT COUNT(*) AS cnt FROM bronze_raw_events")[0]["cnt"]
        scan_compacted_ms = round((time.perf_counter() - t1) * 1000.0, 2)
        assert uncompacted_cnt == compacted_cnt

        # Filtered city scan vs full multi-city aggregation
        t2 = time.perf_counter()
        full_rows = lh.query_sql(
            "SELECT city_id, event_type, COUNT(*) AS cnt FROM bronze_raw_events GROUP BY 1, 2"
        )
        full_scan_ms = round((time.perf_counter() - t2) * 1000.0, 2)

        t3 = time.perf_counter()
        pruned_rows = lh.query_sql(
            "SELECT city_id, event_type, COUNT(*) AS cnt FROM bronze_raw_events WHERE city_id = 'nyc' GROUP BY 1, 2"
        )
        pruned_scan_ms = round((time.perf_counter() - t3) * 1000.0, 2)

        # Unsalted vs 8-way salted H3 aggregation (verifying exact parity within 1e-5)
        t4 = time.perf_counter()
        unsalted = lh.query_sql(
            "SELECT h3_cell, AVG(pricing_multiplier) AS avg_m, SUM(demand_count) AS dem FROM silver_pricing GROUP BY 1 ORDER BY 1"
        )
        unsalted_ms = round((time.perf_counter() - t4) * 1000.0, 2)

        t5 = time.perf_counter()
        salted = lh.query_sql(
            """
            WITH salted AS (
                SELECT h3_cell, ABS(HASH(decision_id)) % 8 AS salt,
                       SUM(pricing_multiplier) AS sum_m, COUNT(*) AS cnt, SUM(demand_count) AS dem
                FROM silver_pricing GROUP BY 1, 2
            )
            SELECT h3_cell, SUM(sum_m) / SUM(cnt) AS avg_m, SUM(dem) AS dem
            FROM salted GROUP BY 1 ORDER BY 1
            """
        )
        salted_ms = round((time.perf_counter() - t5) * 1000.0, 2)
        assert len(unsalted) == len(salted)
        for u, s in zip(unsalted, salted, strict=True):
            assert u["h3_cell"] == s["h3_cell"]
            assert int(u["dem"]) == int(s["dem"])
            assert abs(float(u["avg_m"]) - float(s["avg_m"])) < 1e-5

        return {
            "compaction": {
                "files_before": stats_before["data_file_count"],
                "files_after": comp["files_after"],
                "bytes_before": stats_before["total_bytes"],
                "bytes_after": comp["bytes_after"],
                "storage_reduction_pct": round(
                    (1.0 - comp["bytes_after"] / max(1, stats_before["total_bytes"])) * 100.0, 2
                ),
                "scan_uncompacted_ms": scan_uncompacted_ms,
                "scan_compacted_ms": scan_compacted_ms,
                "compaction_duration_ms": comp["duration_ms"],
                "records_preserved": compacted_cnt,
            },
            "city_filter_pruning": {
                "full_scan_ms": full_scan_ms,
                "nyc_filtered_scan_ms": pruned_scan_ms,
                "full_groups": len(full_rows),
                "nyc_groups": len(pruned_rows),
            },
            "hot_h3_skew_salting": {
                "unsalted_agg_ms": unsalted_ms,
                "salted_8way_agg_ms": salted_ms,
                "cells_verified": len(unsalted),
                "exact_parity": True,
                "note": "Salting adds a two-stage shuffle overhead on single-node DuckDB (<1k cells) and pays off only on distributed multi-node Spark clusters with severe partition skew.",
            },
        }
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def plot_charts(load_rows: list[dict[str, Any]], h3_rows: list[dict[str, Any]]) -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "figure.facecolor": "#0b111b",
            "axes.facecolor": "#111a28",
            "axes.edgecolor": "#23334d",
            "axes.labelcolor": "#cbd5e1",
            "xtick.color": "#94a3b8",
            "ytick.color": "#94a3b8",
            "text.color": "#e2e8f0",
            "grid.color": "#1e2e45",
        }
    )

    # Chart 1: Multi-Resolution H3 Tradeoffs (Res 6 / 7 / 8 / 9)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 4.8), dpi=160)
    res_labels = [f"Res {r['h3_resolution']}\n({r['avg_hex_area_km2']} km²)" for r in h3_rows]
    cells = [r["active_h3_cells"] for r in h3_rows]
    sparse = [r["zero_supply_cell_pct"] for r in h3_rows]
    mat_ms = [r["serving_materialize_ms"] for r in h3_rows]
    stream_ms = [r["stream_process_ms"] for r in h3_rows]

    bars = ax1.bar(res_labels, cells, color=["#38bdf8", "#2dd4bf", "#f59e0b", "#f87171"], width=0.55)
    ax1.set_title("H3 Cardinality vs. Sparse-Cell Sparsity (Zero Supply %)", fontsize=11.5, fontweight="bold", pad=10)
    ax1.set_ylabel("Active H3 Cells Tracked")
    ax1.grid(axis="y", alpha=0.5)
    for b, c in zip(bars, cells, strict=True):
        ax1.text(b.get_x() + b.get_width() / 2, c + max(cells) * 0.02, f"{c:,}", ha="center", va="bottom", fontsize=9.5, fontweight="bold")
    ax1b = ax1.twinx()
    ax1b.plot(res_labels, sparse, color="#fbbf24", marker="o", linewidth=2.4, label="Zero-Supply Cells (%)")
    ax1b.set_ylabel("Zero-Supply Cells (%)", color="#fbbf24")
    ax1b.tick_params(axis="y", colors="#fbbf24")
    ax1b.set_ylim(0, 105)

    x = list(range(len(res_labels)))
    w = 0.36
    ax2.bar([i - w / 2 for i in x], stream_ms, width=w, color="#60a5fa", label="Stream + Pricing (ms)")
    ax2.bar([i + w / 2 for i in x], mat_ms, width=w, color="#34d399", label="KV Materialization (ms)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(res_labels)
    ax2.set_title("Compute & Serving Store Materialization Cost by H3 Resolution", fontsize=11.5, fontweight="bold", pad=10)
    ax2.set_ylabel("Latency (ms)")
    ax2.grid(axis="y", alpha=0.5)
    ax2.legend(frameon=True, facecolor="#0e1726", edgecolor="#23334d")

    fig.tight_layout()
    fig.savefig(ASSETS_DIR / "benchmark_h3_resolutions.png", bbox_inches="tight")
    plt.close(fig)

    # Chart 2: Sustained Throughput & Serving KV Latency across Load Profiles
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 4.8), dpi=160)
    profs = [r["profile"] for r in load_rows]
    prod_eps = [r["producer_eps"] / 1000.0 for r in load_rows]
    strm_eps = [r["stream_eps"] / 1000.0 for r in load_rows]
    x = list(range(len(profs)))
    w = 0.36

    ax1.bar([i - w / 2 for i in x], prod_eps, width=w, color="#38bdf8", label="Avro Producer (k events/s)")
    ax1.bar([i + w / 2 for i in x], strm_eps, width=w, color="#2dd4bf", label="Stateful Stream Engine (k events/s)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(profs)
    ax1.set_title("Measured Pipeline Throughput by Load Profile", fontsize=11.5, fontweight="bold", pad=10)
    ax1.set_ylabel("Throughput (Thousand Events / sec)")
    ax1.grid(axis="y", alpha=0.5)
    ax1.legend(frameon=True, facecolor="#0e1726", edgecolor="#23334d")

    p50 = [r["kv_lookup_p50_ms"] for r in load_rows]
    p95 = [r["kv_lookup_p95_ms"] for r in load_rows]
    p99 = [r["kv_lookup_p99_ms"] for r in load_rows]
    ax2.plot(profs, p50, marker="o", linewidth=2.2, color="#34d399", label="p50 Lookup (ms)")
    ax2.plot(profs, p95, marker="s", linewidth=2.2, color="#f59e0b", label="p95 Lookup (ms)")
    ax2.plot(profs, p99, marker="^", linewidth=2.2, color="#f87171", label="p99 Lookup (ms)")
    ax2.set_title("Serving Store Pricing Lookup Latency (1,000 Lookups / Profile)", fontsize=11.5, fontweight="bold", pad=10)
    ax2.set_ylabel("Point-Lookup Latency (ms)")
    ax2.grid(True, alpha=0.5)
    ax2.legend(frameon=True, facecolor="#0e1726", edgecolor="#23334d")

    fig.tight_layout()
    fig.savefig(ASSETS_DIR / "benchmark_throughput_and_latency.png", bbox_inches="tight")
    plt.close(fig)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    load_rows = benchmark_load_profiles()
    h3_rows = benchmark_h3_resolutions()
    iceberg_opts = benchmark_iceberg_optimizations()
    plot_charts(load_rows, h3_rows)

    meta = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "execution_profile": "local_native",
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "python_version": platform.python_version(),
    }

    (RESULTS_DIR / "load_profiles.json").write_text(
        json.dumps({"metadata": meta, "profiles": load_rows}, indent=2), encoding="utf-8"
    )
    _write_csv(RESULTS_DIR / "load_profiles.csv", load_rows)

    (RESULTS_DIR / "h3_resolution_tradeoffs.json").write_text(
        json.dumps({"metadata": meta, "resolutions": h3_rows}, indent=2), encoding="utf-8"
    )
    _write_csv(RESULTS_DIR / "h3_resolution_tradeoffs.csv", h3_rows)

    (RESULTS_DIR / "iceberg_optimizations.json").write_text(
        json.dumps({"metadata": meta, "optimizations": iceberg_opts}, indent=2), encoding="utf-8"
    )

    print(
        json.dumps(
            {
                "load_profiles": load_rows,
                "h3_resolution_tradeoffs": h3_rows,
                "iceberg_optimizations": iceberg_opts,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
