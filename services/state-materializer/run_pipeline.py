#!/usr/bin/env python3
"""End-to-end Marketplace Pipeline CLI (Simulate -> Stateful Stream & Pricing -> Materialize KV -> Iceberg Lakehouse -> Reconcile -> DQ)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from marketplace_platform.config import load_config
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.lakehouse import MarketplaceLakehouse
from marketplace_platform.serving_store import MaterializedStateStore, StateMaterializer
from marketplace_platform.simulator import LOAD_PROFILES, MarketplaceSimulator, SimConfig
from marketplace_platform.state_engine import MarketplaceStreamProcessor
from marketplace_platform.transport import create_event_bus


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Real-Time Mobility Marketplace E2E pipeline.")
    parser.add_argument("--profile", choices=sorted(LOAD_PROFILES.keys()), default="local")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--ticks", type=int, default=None)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    bus = create_event_bus(cfg)
    reg = AvroContractRegistry()
    store = MaterializedStateStore(cfg)
    lh = MarketplaceLakehouse(cfg)

    sim_cfg = SimConfig.from_profile(
        args.profile,
        seed=args.seed,
        ticks=args.ticks,
        ground_truth_path=cfg.local_data_dir / "ground_truth.json",
    )
    sim_res = MarketplaceSimulator(cfg, bus, reg, sim_cfg).run()
    stream = MarketplaceStreamProcessor(cfg, bus, reg)
    stream_res = stream.process_all()
    mat_res = StateMaterializer(bus, reg, store).materialize_from_durable_log(wipe_first=False)
    bronze_res = lh.ingest_bronze_from_bus(bus, reg)
    cdc_res = lh.materialize_cdc_policy_scd2()
    gold_res = lh.curate_silver_and_gold()
    rec_res = lh.reconcile_serving_store(store, bus, reg)
    dq_res = lh.run_data_quality(processor=stream)

    out = {
        "simulation": {k: v for k, v in sim_res.items() if k != "counts"},
        "stream_state": stream_res,
        "serving_materializer": mat_res,
        "bronze_iceberg": bronze_res,
        "cdc_policy_scd2": cdc_res,
        "silver_gold_iceberg": gold_res,
        "reconciliation": rec_res,
        "data_quality": {"overall_status": dq_res["overall_status"], "summary": dq_res["summary"]},
    }
    if args.compact:
        out["compaction"] = [
            lh.compact_table("bronze.raw_events"),
            lh.compact_table("silver.fact_pricing_decision"),
        ]
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
