"""Bounded Durable Topic Replay & Serving Store Rebuild Utility (Section 12 & 13).

Supports:
- Inspecting durable topic offsets, record counts, and consumer lag.
- Replaying a bounded `[start_offset, end_offset]` window of marketplace events through the
  stream processor into a tagged replay batch.
- Rebuilding the low-latency serving store (`Valkey` or local SQLite WAL) completely from
  durable changelog topics (`--rebuild-serving-store --wipe-first`).
"""

from __future__ import annotations

import argparse
import json

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.lakehouse import MarketplaceLakehouse
from marketplace_platform.serving_store import MaterializedStateStore, StateMaterializer
from marketplace_platform.state_engine import MarketplaceStreamProcessor
from marketplace_platform.transport import create_event_bus


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay durable marketplace topics or rebuild serving store")
    parser.add_argument("--topic", default="marketplace.rider.requests")
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--end-offset", type=int, default=None)
    parser.add_argument("--replay-batch-id", default=None)
    parser.add_argument("--rebuild-serving-store", action="store_true")
    parser.add_argument("--wipe-first", action="store_true")
    parser.add_argument("--reprocess-pricing", action="store_true")
    args = parser.parse_args()

    cfg = MarketplaceConfig()
    cfg.ensure_local_dirs()
    bus = create_event_bus(cfg)
    registry = AvroContractRegistry(cfg.project_root / "schemas" / "avro")
    out: dict[str, object] = {}

    if args.rebuild_serving_store:
        store = MaterializedStateStore(cfg)
        mat = StateMaterializer(bus, registry, store)
        out["serving_rebuild"] = mat.materialize_from_durable_log(wipe_first=args.wipe_first)

    if args.reprocess_pricing:
        # Deterministically replay full durable input streams through fresh state engine
        proc = MarketplaceStreamProcessor(cfg, bus, registry)
        proc.reset_state()
        for t in (
            "marketplace.pricing.policies",
            "marketplace.regional.events",
            "marketplace.driver.availability",
            "marketplace.driver.locations",
            "marketplace.rider.requests",
            "marketplace.dispatch.offers",
            "marketplace.trip.lifecycle",
        ):
            bus.reset_group_offsets(t, proc.CONSUMER_GROUP, to_offset=0)
        out["reprocessed_stream"] = proc.process_all()
        if args.replay_batch_id:
            lh = MarketplaceLakehouse(cfg)
            out["replayed_bronze"] = lh.ingest_bronze_from_bus(
                bus, registry, replay_batch_id=args.replay_batch_id
            )
            out["replayed_silver_gold"] = lh.curate_silver_and_gold()

    if not args.rebuild_serving_store and not args.reprocess_pricing:
        recs = bus.read_range(args.topic, start_offset=args.start_offset, end_offset=args.end_offset)
        out["topic_inspection"] = {
            "topic": args.topic,
            "start_offset": args.start_offset,
            "end_offset": args.end_offset,
            "records_found": len(recs),
            "consumer_lag": bus.get_consumer_lag(args.topic, MarketplaceStreamProcessor.CONSUMER_GROUP),
        }

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
