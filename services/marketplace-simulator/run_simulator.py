"""Standalone CLI entrypoint for the Real-Time Mobility Marketplace Simulator."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.simulator import LOAD_PROFILES, MarketplaceSimulator, SimConfig
from marketplace_platform.transport import create_event_bus


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic multi-city marketplace simulator")
    parser.add_argument("--profile", choices=sorted(LOAD_PROFILES), default="local")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--ticks", type=int, default=None)
    parser.add_argument("--active-drivers", type=int, default=None)
    parser.add_argument("--active-riders", type=int, default=None)
    parser.add_argument("--ground-truth-out", type=Path, default=None)
    args = parser.parse_args()

    cfg = MarketplaceConfig()
    cfg.ensure_local_dirs()
    bus = create_event_bus(cfg)
    registry = AvroContractRegistry(cfg.project_root / "schemas" / "avro")
    sim_cfg = SimConfig.from_profile(
        args.profile,
        seed=args.seed,
        ticks=args.ticks,
        active_drivers=args.active_drivers,
        active_riders=args.active_riders,
        ground_truth_path=args.ground_truth_out,
    )
    result = MarketplaceSimulator(cfg, bus, registry, sim_cfg).run()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
