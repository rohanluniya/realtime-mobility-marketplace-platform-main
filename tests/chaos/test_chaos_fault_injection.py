"""Automated Chaos & Fault-Injection Suite (Section 25) + Raw Artifact Recorder.

Tests 9 real marketplace failure modes and writes verified machine-readable results to
`results/chaos/chaos_results.json` for incident reporting and Control Tower display.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.geospatial import to_h3
from marketplace_platform.lakehouse import MarketplaceLakehouse
from marketplace_platform.serving_store import (
    MaterializedStateStore,
    ServingStoreUnavailableError,
    StateMaterializer,
)
from marketplace_platform.simulator import MarketplaceSimulator, SimConfig
from marketplace_platform.state_engine import MarketplaceStreamProcessor
from marketplace_platform.transport import BrokerUnavailableError, LocalWalEventBus

_CHAOS_RESULTS: list[dict[str, Any]] = []


def _record_scenario(name: str, passed: bool, duration_ms: float, metrics: dict[str, Any]) -> None:
    _CHAOS_RESULTS.append(
        {
            "scenario": name,
            "status": "PASS" if passed else "FAIL",
            "duration_ms": round(duration_ms, 2),
            "metrics": metrics,
        }
    )


def _persist_chaos_results() -> None:
    out_path = Path(__file__).resolve().parents[2] / "results" / "chaos" / "chaos_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    passed = sum(1 for r in _CHAOS_RESULTS if r["status"] == "PASS")
    payload = {
        "suite": "marketplace_chaos_and_fault_injection",
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "passed": passed,
        "failed": len(_CHAOS_RESULTS) - passed,
        "total": len(_CHAOS_RESULTS),
        "scenarios": _CHAOS_RESULTS,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _mk_event(
    reg: AvroContractRegistry,
    topic: str,
    *,
    event_id: str,
    event_type: str,
    ev_time: datetime,
    lat: float,
    lon: float,
    driver_id: str | None = None,
    rider_id: str | None = None,
    request_id: str | None = None,
    seq: int = 1,
    payload: dict[str, Any] | None = None,
) -> bytes:
    rec = {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": ev_time.isoformat(),
        "producer_time": ev_time.isoformat(),
        "ingestion_time": ev_time.isoformat(),
        "schema_version": "1.0.0",
        "rider_id": rider_id,
        "driver_id": driver_id,
        "request_id": request_id,
        "trip_id": None,
        "region_id": "us_east",
        "city_id": "nyc",
        "latitude": lat,
        "longitude": lon,
        "correlation_id": "chaos-corr",
        "trace_id": "chaos-trace",
        "sequence_number": seq,
        "payload_json": json.dumps(payload or {}),
        "metadata": {"chaos": "true"},
    }
    return reg.serialize(topic, rec)


def test_chaos_01_broker_outage_and_recovery(isolated_config: MarketplaceConfig) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    bus.ensure_topics()
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    ev = _mk_event(
        reg,
        "marketplace.driver.availability",
        event_id="brk-1",
        event_type="driver_available",
        ev_time=datetime(2026, 11, 24, 18, 0, tzinfo=UTC),
        lat=40.7549,
        lon=-73.9840,
        driver_id="DRV-BRK-1",
        payload={"target_state": "AVAILABLE"},
    )

    bus.set_broker_online(False)
    with pytest.raises(BrokerUnavailableError):
        bus.produce("marketplace.driver.availability", "DRV-BRK-1", ev)

    bus.set_broker_online(True)
    rec = bus.produce("marketplace.driver.availability", "DRV-BRK-1", ev)
    assert rec.offset >= 0
    recs = bus.read_range("marketplace.driver.availability")
    assert len(recs) == 1
    _record_scenario(
        "broker_outage_and_recovery",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {"partition": rec.partition, "offset": rec.offset, "records_recovered": len(recs)},
    )


def test_chaos_02_stream_processor_crash_and_checkpoint_parity(
    isolated_config: MarketplaceConfig, tmp_path: Path
) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    MarketplaceSimulator(isolated_config, bus, reg, SimConfig.from_profile("smoke", seed=77)).run()

    # Reference clean run
    ref_cfg = MarketplaceConfig(
        local_data_dir=tmp_path / "ref",
        wal_dir=isolated_config.wal_dir,
        checkpoint_dir=tmp_path / "ref_ckpt",
        warehouse_dir=tmp_path / "ref_wh",
        kv_store_path=tmp_path / "ref.sqlite",
        lineage_dir=tmp_path / "ref_lin",
        sqlite_catalog_path=tmp_path / "ref_cat.db",
    )
    ref_cfg.ensure_local_dirs()
    ref_bus = LocalWalEventBus(isolated_config.wal_dir)
    ref_proc = MarketplaceStreamProcessor(ref_cfg, ref_bus, reg)
    ref_proc.CONSUMER_GROUP = "ref-group"
    ref_proc.process_all(checkpoint_every=200)
    ref_supply = dict(ref_proc.supply_by_cell)
    ref_mults = {c: st.last_multiplier for c, st in ref_proc.prior_pricing.items()}

    # Crashing run: crash mid-stream after 450 records (checkpoint saved at 200 & 400)
    crash_proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    with pytest.raises(RuntimeError, match="Simulated marketplace Flink TaskManager crash"):
        crash_proc.process_all(fail_after_records=450, checkpoint_every=200)

    # Fresh TaskManager instance restores from checkpoint and finishes remaining events
    recovered_proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    assert recovered_proc.restore_checkpoint() is True
    recovered_proc.process_all(checkpoint_every=500)
    recovered_proc.assert_state_invariants()

    assert dict(recovered_proc.supply_by_cell) == ref_supply
    rec_mults = {c: st.last_multiplier for c, st in recovered_proc.prior_pricing.items()}
    assert rec_mults == ref_mults
    _record_scenario(
        "stream_processor_crash_and_checkpoint_parity",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {"cells_verified": len(ref_mults), "exact_multiplier_parity": True, "exact_supply_parity": True},
    )


def test_chaos_03_serving_store_outage_safe_fallback_and_full_rebuild(
    isolated_config: MarketplaceConfig,
) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    MarketplaceSimulator(isolated_config, bus, reg, SimConfig.from_profile("smoke", seed=88)).run()
    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    proc.process_all()

    store = MaterializedStateStore(isolated_config)
    mat = StateMaterializer(bus, reg, store)
    first_mat = mat.materialize_from_durable_log()
    before_prices = {
        p["h3_cell"]: p["pricing_multiplier"]
        for p in store.list_by_prefix("mkt:v1:pricing:h3:", limit=500)
    }
    sample_cell = next(iter(before_prices))

    # Simulate serving KV outage -> direct reads fail, pricing lookup returns safe 1.0x fallback
    store.set_online(False)
    with pytest.raises(ServingStoreUnavailableError):
        store.get(f"mkt:v1:pricing:h3:{sample_cell}")
    fb = store.get_pricing_with_safe_fallback(sample_cell)
    assert fb["pricing_multiplier"] == 1.0
    assert fb["serving_status"] == "SAFE_FALLBACK_OUTAGE"

    # Bring store online, wipe completely, and rebuild 100% from durable Kafka/WAL changelog
    store.set_online(True)
    rebuilt = mat.materialize_from_durable_log(wipe_first=True)
    after_prices = {
        p["h3_cell"]: p["pricing_multiplier"]
        for p in store.list_by_prefix("mkt:v1:pricing:h3:", limit=500)
    }
    assert after_prices == before_prices
    assert rebuilt["total_live_keys"] == first_mat["total_live_keys"]
    _record_scenario(
        "serving_store_outage_safe_fallback_and_full_rebuild",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {
            "fallback_multiplier": fb["pricing_multiplier"],
            "keys_rebuilt": rebuilt["total_live_keys"],
            "rebuild_ms": rebuilt["duration_ms"],
            "exact_parity": True,
        },
    )


def test_chaos_04_out_of_order_driver_location_rejected(isolated_config: MarketplaceConfig) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    bus.ensure_topics()
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    t_base = datetime(2026, 11, 24, 18, 0, tzinfo=UTC)

    cell_new = to_h3(40.6413, -73.7781, 7)  # JFK
    cell_old = to_h3(40.7549, -73.9840, 7)  # Midtown
    assert cell_new != cell_old

    # Seq 5 arrives first at JFK
    ev_new = _mk_event(
        reg,
        "marketplace.driver.locations",
        event_id="ooo-new",
        event_type="driver_location_updated",
        ev_time=t_base + timedelta(seconds=50),
        lat=40.6413,
        lon=-73.7781,
        driver_id="DRV-OOO-1",
        seq=5,
    )
    # Stale Seq 2 arrives later (with later wall/event timestamp!) claiming driver is still in Midtown
    ev_stale = _mk_event(
        reg,
        "marketplace.driver.locations",
        event_id="ooo-stale",
        event_type="driver_location_updated",
        ev_time=t_base + timedelta(seconds=60),
        lat=40.7549,
        lon=-73.9840,
        driver_id="DRV-OOO-1",
        seq=2,
    )
    bus.produce("marketplace.driver.locations", "DRV-OOO-1", ev_new, timestamp_ms=int((t_base + timedelta(seconds=50)).timestamp() * 1000))
    bus.produce("marketplace.driver.locations", "DRV-OOO-1", ev_stale, timestamp_ms=int((t_base + timedelta(seconds=60)).timestamp() * 1000))

    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    res = proc.process_all()
    proc.assert_state_invariants()
    assert res["stale_out_of_order_ignored"] >= 1
    assert proc.supply_by_cell.get(cell_new) == 1
    assert proc.supply_by_cell.get(cell_old, 0) == 0
    _record_scenario(
        "out_of_order_driver_location_rejected",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {"stale_ignored": res["stale_out_of_order_ignored"], "authoritative_cell": cell_new},
    )


def test_chaos_05_duplicate_event_burst_idempotency(isolated_config: MarketplaceConfig) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    bus.ensure_topics()
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    ts = datetime(2026, 11, 24, 18, 0, tzinfo=UTC)
    ev = _mk_event(
        reg,
        "marketplace.driver.availability",
        event_id="dup-evt-1",
        event_type="driver_available",
        ev_time=ts,
        lat=40.7549,
        lon=-73.9840,
        driver_id="DRV-DUP-1",
        seq=1,
        payload={"target_state": "AVAILABLE"},
    )
    for _ in range(10):
        bus.produce("marketplace.driver.availability", "DRV-DUP-1", ev, timestamp_ms=int(ts.timestamp() * 1000))

    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    res = proc.process_all()
    proc.assert_state_invariants()
    assert res["duplicates_suppressed"] == 9
    assert sum(proc.supply_by_cell.values()) == 1
    _record_scenario(
        "duplicate_event_burst_idempotency",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {"duplicates_injected": 9, "duplicates_suppressed": res["duplicates_suppressed"]},
    )


def test_chaos_06_reservation_and_heartbeat_timers(isolated_config: MarketplaceConfig) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    bus.ensure_topics()
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    t_start = datetime(2026, 11, 24, 18, 0, tzinfo=UTC)

    # Driver A enters RESERVED at t=0 (should auto-release to AVAILABLE once watermark advances > 45s)
    bus.produce(
        "marketplace.driver.availability",
        "DRV-RES",
        _mk_event(
            reg,
            "marketplace.driver.availability",
            event_id="tmr-res",
            event_type="driver_reserved",
            ev_time=t_start,
            lat=40.7549,
            lon=-73.9840,
            driver_id="DRV-RES",
            seq=1,
            payload={"target_state": "RESERVED"},
        ),
        timestamp_ms=int(t_start.timestamp() * 1000),
    )
    # Driver B enters AVAILABLE at t=0 (should be evicted to OFFLINE once watermark advances > 90s)
    bus.produce(
        "marketplace.driver.availability",
        "DRV-STALE",
        _mk_event(
            reg,
            "marketplace.driver.availability",
            event_id="tmr-stale",
            event_type="driver_available",
            ev_time=t_start,
            lat=40.6413,
            lon=-73.7781,
            driver_id="DRV-STALE",
            seq=1,
            payload={"target_state": "AVAILABLE"},
        ),
        timestamp_ms=int(t_start.timestamp() * 1000),
    )
    # Driver C heartbeats at t+250s -> watermark advances +220s, exceeding both 45s reservation & 180s heartbeat TTLs
    t_adv = t_start + timedelta(seconds=250)
    bus.produce(
        "marketplace.driver.availability",
        "DRV-FRESH",
        _mk_event(
            reg,
            "marketplace.driver.availability",
            event_id="tmr-fresh",
            event_type="driver_available",
            ev_time=t_adv,
            lat=40.7233,
            lon=-73.9985,
            driver_id="DRV-FRESH",
            seq=1,
            payload={"target_state": "AVAILABLE"},
        ),
        timestamp_ms=int(t_adv.timestamp() * 1000),
    )

    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    proc.process_all()
    proc.assert_state_invariants()
    assert proc.drivers["DRV-RES"].state == "AVAILABLE"
    assert proc.drivers["DRV-STALE"].state == "OFFLINE"
    assert proc.drivers["DRV-FRESH"].state == "AVAILABLE"
    _record_scenario(
        "reservation_and_heartbeat_timers",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {"reserved_released": True, "stale_evicted": True},
    )


def test_chaos_07_stale_serving_store_version_rejected(isolated_config: MarketplaceConfig) -> None:
    t0 = time.perf_counter()
    store = MaterializedStateStore(isolated_config)
    assert store.put_versioned("mkt:v1:pricing:h3:testcell", 10, {"pricing_multiplier": 1.75}) is True
    assert store.put_versioned("mkt:v1:pricing:h3:testcell", 9, {"pricing_multiplier": 1.10}) is False
    assert store.put_versioned("mkt:v1:pricing:h3:testcell", 10, {"pricing_multiplier": 1.20}) is False
    hit = store.get("mkt:v1:pricing:h3:testcell")
    assert hit is not None and hit["payload"]["pricing_multiplier"] == 1.75
    _record_scenario(
        "stale_serving_store_version_rejected",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {"stored_version": hit["version"], "preserved_multiplier": 1.75},
    )


def test_chaos_08_serving_drift_detection_and_repair(isolated_config: MarketplaceConfig) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    MarketplaceSimulator(isolated_config, bus, reg, SimConfig.from_profile("smoke", seed=55)).run()
    MarketplaceStreamProcessor(isolated_config, bus, reg).process_all()
    store = MaterializedStateStore(isolated_config)
    StateMaterializer(bus, reg, store).materialize_from_durable_log()
    lh = MarketplaceLakehouse(isolated_config)
    lh.ingest_bronze_from_bus(bus, reg)
    lh.curate_silver_and_gold()

    # Corrupt 3 cells
    cells = store.list_by_prefix("mkt:v1:pricing:h3:", limit=3)
    for c in cells:
        bad = dict(c)
        bad["pricing_multiplier"] = 4.99
        store.put_versioned(f"mkt:v1:pricing:h3:{c['h3_cell']}", 888888, bad)

    recon = lh.reconcile_serving_store(store, bus, reg)
    assert recon["drifted_cells_detected"] == len(cells)
    assert recon["status"] == "REPAIRED"
    assert lh.reconcile_serving_store(store, bus, reg)["status"] == "CONSISTENT"
    _record_scenario(
        "serving_drift_detection_and_repair",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {"injected_drift": len(cells), "repaired": recon["cells_repaired"]},
    )


def test_chaos_09_hot_h3_cell_skew_and_poison_quarantine(isolated_config: MarketplaceConfig) -> None:
    t0 = time.perf_counter()
    bus = LocalWalEventBus(isolated_config.wal_dir)
    bus.ensure_topics()
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    ts = datetime(2026, 11, 24, 18, 0, tzinfo=UTC)

    # 120 concentrated rider requests into JFK cell + 1 driver + 15 poison messages
    jfk_lat, jfk_lon = 40.6413, -73.7781
    jfk_cell = to_h3(jfk_lat, jfk_lon, 7)
    bus.produce(
        "marketplace.driver.availability",
        "DRV-JFK",
        _mk_event(
            reg,
            "marketplace.driver.availability",
            event_id="jfk-drv",
            event_type="driver_available",
            ev_time=ts,
            lat=jfk_lat,
            lon=jfk_lon,
            driver_id="DRV-JFK",
            payload={"target_state": "AVAILABLE"},
        ),
        timestamp_ms=int(ts.timestamp() * 1000),
    )
    for i in range(120):
        bus.produce(
            "marketplace.rider.requests",
            f"REQ-JFK-{i}",
            _mk_event(
                reg,
                "marketplace.rider.requests",
                event_id=f"jfk-req-{i}",
                event_type="ride_requested",
                ev_time=ts + timedelta(milliseconds=i * 10),
                lat=jfk_lat,
                lon=jfk_lon,
                rider_id=f"RDR-{i}",
                request_id=f"REQ-JFK-{i}",
                payload={"service_type": "standard", "target_state": "SEARCHING"},
            ),
            timestamp_ms=int(ts.timestamp() * 1000),
        )
    for p in range(15):
        bus.produce("marketplace.rider.requests", f"poison-{p}", b"\xffBAD_POISON_PAYLOAD", timestamp_ms=int(ts.timestamp() * 1000))

    proc = MarketplaceStreamProcessor(isolated_config, bus, reg)
    res = proc.process_all()
    proc.assert_state_invariants()
    assert res["dlq_quarantined"] == 15
    assert proc.prior_pricing[jfk_cell].last_multiplier <= 3.0
    _record_scenario(
        "hot_h3_cell_skew_and_poison_quarantine",
        True,
        (time.perf_counter() - t0) * 1000.0,
        {
            "hot_cell_requests": 120,
            "poison_quarantined": res["dlq_quarantined"],
            "clamped_multiplier": proc.prior_pricing[jfk_cell].last_multiplier,
        },
    )
    _persist_chaos_results()
