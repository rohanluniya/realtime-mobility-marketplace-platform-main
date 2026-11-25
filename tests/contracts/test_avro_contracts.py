"""Contract tests for Avro event schemas, wire framing, evolution compatibility, and malformed rejection."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry, SchemaValidationError


def test_avro_roundtrip_all_core_topics() -> None:
    cfg = MarketplaceConfig()
    reg = AvroContractRegistry(cfg.project_root / "schemas" / "avro")
    now = datetime(2026, 11, 24, 18, 0, 0, tzinfo=UTC).isoformat()

    ev = {
        "event_id": "ev-001",
        "event_type": "ride_requested",
        "event_time": now,
        "producer_time": now,
        "ingestion_time": now,
        "schema_version": "1.0.0",
        "rider_id": "RDR-1",
        "driver_id": None,
        "request_id": "REQ-1",
        "trip_id": None,
        "region_id": "us_east",
        "city_id": "nyc",
        "latitude": 40.7549,
        "longitude": -73.9840,
        "correlation_id": "corr-1",
        "trace_id": "trace-1",
        "sequence_number": 1,
        "payload_json": '{"service_type":"standard"}',
        "metadata": {"synthetic": "true"},
    }
    wire = reg.serialize("marketplace.rider.requests", ev)
    assert wire[0] == 0x00
    sid, decoded = reg.deserialize(wire, expected_topic="marketplace.rider.requests")
    assert sid == 1
    assert decoded["event_id"] == "ev-001"
    assert decoded["request_id"] == "REQ-1"


def test_avro_v1_to_v2_backward_and_forward_compatibility() -> None:
    import json

    cfg = MarketplaceConfig()
    schemas_dir = cfg.project_root / "schemas" / "avro"
    reg = AvroContractRegistry(schemas_dir)
    v1 = json.loads((schemas_dir / "marketplace_event.avsc").read_text(encoding="utf-8"))
    v2 = json.loads((schemas_dir / "v2" / "marketplace_event.avsc").read_text(encoding="utf-8"))
    now = datetime(2026, 11, 24, 18, 0, 0, tzinfo=UTC).isoformat()

    sample_v1 = {
        "event_id": "ev-compat-1",
        "event_type": "ride_requested",
        "event_time": now,
        "producer_time": now,
        "ingestion_time": now,
        "schema_version": "1.0.0",
        "rider_id": "RDR-1",
        "driver_id": None,
        "request_id": "REQ-1",
        "trip_id": None,
        "region_id": "us_east",
        "city_id": "nyc",
        "latitude": 40.7549,
        "longitude": -73.9840,
        "correlation_id": "corr-1",
        "trace_id": "trace-1",
        "sequence_number": 1,
        "payload_json": "{}",
        "metadata": {},
    }
    sample_v2 = {
        **sample_v1,
        "schema_version": "2.0.0",
        "h3_cell_res7": "872a1072cffffff",
        "experiment_cohort": "treatment",
    }

    b_ok, b_msg = reg.check_compatibility(v1, v2, sample_v1)
    f_ok, f_msg = reg.check_compatibility(v2, v1, sample_v2)
    assert b_ok, b_msg
    assert f_ok, f_msg


def test_avro_rejects_corrupt_wire_and_missing_required_fields() -> None:
    cfg = MarketplaceConfig()
    reg = AvroContractRegistry(cfg.project_root / "schemas" / "avro")

    with pytest.raises(SchemaValidationError):
        reg.deserialize(b"not-a-valid-avro-wire-frame", expected_topic="marketplace.rider.requests")

    with pytest.raises(SchemaValidationError):
        reg.serialize("marketplace.rider.requests", {"event_id": "missing-fields"})
