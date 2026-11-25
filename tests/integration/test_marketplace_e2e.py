"""End-to-end integration tests verifying Acceptance Criteria B–S on isolated storage."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient
from pyiceberg.types import StringType

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.geospatial import to_h3
from marketplace_platform.lakehouse import MarketplaceLakehouse
from marketplace_platform.serving_store import MaterializedStateStore, StateMaterializer
from marketplace_platform.simulator import MarketplaceSimulator, SimConfig
from marketplace_platform.state_engine import MarketplaceStreamProcessor
from marketplace_platform.transport import LocalWalEventBus


def test_driver_h3_cell_transfers_never_double_count_and_terminal_requests_clear(
    isolated_config: MarketplaceConfig,
) -> None:
    """Acceptance Criteria B, C, D: atomic H3 transfers & immediate terminal request removal."""
    bus = LocalWalEventBus(isolated_config.wal_dir)
    bus.ensure_topics()
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    t0 = datetime(2026, 11, 24, 18, 0, 0, tzinfo=UTC)

    # Move a single driver across 5 distinct H3 cells in NYC
    coords = [
        (40.7549, -73.9840),
        (40.7680, -73.9720),
        (40.7420, -73.9910),
        (40.7233, -73.9985),
        (40.7100, -74.0080),
    ]
    cells = [to_h3(lat, lon, 7) for lat, lon in coords]
    assert len(set(cells)) >= 4

    for idx, (lat, lon) in enumerate(coords, 1):
        ts = t0 + timedelta(seconds=idx * 10)
        ev_type = "driver_available" if idx == 1 else "driver_location_updated"
        topic = "marketplace.driver.availability" if idx == 1 else "marketplace.driver.locations"
        ev = {
            "event_id": f"drv-move-{idx}",
            "event_type": ev_type,
            "event_time": ts.isoformat(),
            "producer_time": ts.isoformat(),
            "ingestion_time": ts.isoformat(),
            "schema_version": "1.0.0",
            "rider_id": None,
            "driver_id": "DRV-SINGLE-1",
            "request_id": None,
            "trip_id": None,
            "region_id": "us_east",
            "city_id": "nyc",
            "latitude": lat,
            "longitude": lon,
            "correlation_id": "c1",
            "trace_id": "t1",
            "sequence_number": idx,
            "payload_json": json.dumps({"target_state": "AVAILABLE"}),
            "metadata": {},
        }
        bus.produce(topic, "DRV-SINGLE-1", reg.serialize(topic, ev), timestamp_ms=int(ts.timestamp() * 1000))

    # Create a rider request then cancel it -> active demand MUST drop to 0
    for seq, (ev_type, target_st) in enumerate(
        [("ride_requested", "SEARCHING"), ("ride_request_cancelled", "CANCELLED")], 1
    ):
        ts = t0 + timedelta(seconds=seq * 15)
        ev = {
            "event_id": f"req-cancel-{seq}",
            "event_type": ev_type,
            "event_time": ts.isoformat(),
            "producer_time": ts.isoformat(),
            "ingestion_time": ts.isoformat(),
            "schema_version": "1.0.0",
            "rider_id": "RDR-1",
            "driver_id": None,
            "request_id": "REQ-CANCEL-1",
            "trip_id": None,
            "region_id": "us_east",
            "city_id": "nyc",
            "latitude": 40.7549,
            "longitude": -73.9840,
            "correlation_id": "c1",
            "trace_id": "t1",
            "sequence_number": seq,
            "payload_json": json.dumps({"service_type": "standard", "target_state": target_st, "cancelled_by": "rider"}),
            "metadata": {},
        }
        bus.produce(
            "marketplace.rider.requests",
            "REQ-CANCEL-1",
            reg.serialize("marketplace.rider.requests", ev),
            timestamp_ms=int(ts.timestamp() * 1000),
        )

    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    res = proc.process_all()
    proc.assert_state_invariants()

    # Exactly 1 driver in final cell, 0 in all earlier cells!
    assert sum(proc.supply_by_cell.values()) == 1
    assert proc.supply_by_cell.get(cells[-1]) == 1
    assert res["cell_transfers_processed"] >= 3
    # Cancelled request immediately cleared from active demand!
    assert sum(proc.demand_by_cell.values()) == 0


def test_full_pipeline_iceberg_timetravel_evolution_compaction_and_reconciliation(
    isolated_config: MarketplaceConfig,
) -> None:
    """Acceptance Criteria E–O: end-to-end stream, Iceberg time travel, evolution, compaction, reconciliation, DQ."""
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    sim = MarketplaceSimulator(isolated_config, bus, reg, SimConfig.from_profile("smoke", seed=42))
    sim_res = sim.run()
    assert sim_res["total_events_produced"] > 1000

    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    st_res = proc.process_all()
    assert st_res["dlq_quarantined"] >= 3
    assert st_res["pricing_decisions_emitted"] > 0

    store = MaterializedStateStore(isolated_config)
    mat = StateMaterializer(bus, reg, store)
    mat_res = mat.materialize_from_durable_log()
    assert mat_res["total_live_keys"] > 0

    lh = MarketplaceLakehouse(isolated_config)
    b1 = lh.ingest_bronze_from_bus(bus, reg)
    assert b1["raw_events"] > 1000
    snap_1 = lh.load_table("bronze.raw_events").current_snapshot().snapshot_id

    # Ingest a second micro-batch to verify Iceberg time travel + compaction (2 files -> 1 file)
    lh.ingest_bronze_from_bus(bus, reg, replay_batch_id="batch-2")
    tbl = lh.load_table("bronze.raw_events")
    snap_2 = tbl.current_snapshot().snapshot_id
    assert snap_1 != snap_2

    # Iceberg time-travel verification
    rows_s1 = len(lh.read_arrow("bronze.raw_events", snapshot_id=snap_1))
    rows_s2 = len(lh.read_arrow("bronze.raw_events", snapshot_id=snap_2))
    assert rows_s2 == rows_s1 * 2

    # Iceberg compaction verification (zero data loss, reduces file count)
    comp = lh.compact_table("bronze.raw_events")
    assert comp["action"] == "compacted"
    assert comp["files_before"] >= 2
    assert comp["files_after"] == 1
    assert comp["records_preserved"] == rows_s2

    # Iceberg schema evolution verification (add nullable column safely)
    with lh.load_table("bronze.raw_events").update_schema() as upd:
        upd.add_column("geo_source", StringType(), doc="Optional GPS source tag")
    assert "geo_source" in {f.name for f in lh.load_table("bronze.raw_events").schema().fields}

    # CDC policy SCD2 + Silver/Gold curation + 8/8 DQ checks
    scd = lh.materialize_cdc_policy_scd2()
    assert scd["policy_scd2_rows"] == 6
    sg = lh.curate_silver_and_gold()
    assert sg["gold_h3_cells"] > 0
    assert sg["srm_check"]["status"] == "PASS"

    dq = lh.run_data_quality(processor=proc)
    assert dq["overall_status"] == "PASS"
    assert dq["summary"]["passed"] == 8

    # Inject drift into live serving store & verify reconciliation detects + repairs it!
    pricing_cells = store.list_by_prefix("mkt:v1:pricing:h3:", limit=5)
    target = pricing_cells[0]
    corrupted = dict(target)
    corrupted["pricing_multiplier"] = 9.99
    store.put_versioned(f"mkt:v1:pricing:h3:{target['h3_cell']}", 999999, corrupted)

    recon = lh.reconcile_serving_store(store, bus, reg)
    assert recon["drifted_cells_detected"] >= 1
    assert recon["status"] == "REPAIRED"

    # Re-check reconciliation -> now CONSISTENT
    recon2 = lh.reconcile_serving_store(store, bus, reg)
    assert recon2["drifted_cells_detected"] == 0
    assert recon2["status"] == "CONSISTENT"


def test_api_endpoints_rbac_and_safe_fallback(isolated_config: MarketplaceConfig) -> None:
    """Acceptance Criteria P–S: all 12 API endpoints, RBAC authz, and safe 1.0x fallback."""
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    MarketplaceSimulator(isolated_config, bus, reg, SimConfig.from_profile("smoke", seed=99)).run()
    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    proc.process_all()
    store = MaterializedStateStore(isolated_config)
    StateMaterializer(bus, reg, store).materialize_from_durable_log()
    lh = MarketplaceLakehouse(isolated_config)
    lh.ingest_bronze_from_bus(bus, reg)
    lh.materialize_cdc_policy_scd2()
    lh.curate_silver_and_gold()
    lh.run_data_quality(processor=proc)

    api_dir = Path(isolated_config.project_root) / "apps" / "marketplace-api"
    if str(api_dir) not in sys.path:
        sys.path.insert(0, str(api_dir))
    import main as api_main  # type: ignore[import-not-found]

    # Point API runtime to isolated test storage
    api_main._rt.cfg = isolated_config
    api_main._rt.bus = bus
    api_main._rt.registry = reg
    api_main._rt.store = store
    api_main._rt.lakehouse = lh

    client = TestClient(api_main.app)
    for ep in (
        "/health",
        "/marketplace/summary",
        "/marketplace/demand",
        "/marketplace/supply",
        "/marketplace/imbalance",
        "/marketplace/pricing",
        "/marketplace/regions",
        "/marketplace/trips",
        "/marketplace/matching",
        "/marketplace/experiments",
        "/platform/metrics",
        "/platform/data-quality",
        "/api/map/h3-cells.geojson",
        "/metrics",
    ):
        resp = client.get(ep)
        assert resp.status_code == 200, f"Endpoint {ep} failed: {resp.text}"

    # Verify unknown H3 cell lookup triggers safe 1.0x fallback
    fb = client.get("/marketplace/pricing/872a10000ffffff").json()
    assert fb["pricing_multiplier"] == 1.0
    assert fb["serving_status"] == "SAFE_FALLBACK_MISS"

    # RBAC checks
    assert client.post("/platform/actions/reconcile").status_code == 403
    assert (
        client.post(
            "/platform/actions/reconcile",
            headers={"Authorization": "Bearer wrong-secret"},
        ).status_code
        == 401
    )
    ok = client.post(
        "/platform/actions/reconcile",
        headers={"Authorization": f"Bearer {isolated_config.api_operator_token}"},
    )
    assert ok.status_code == 200
    assert ok.json()["status"] == "CONSISTENT"
