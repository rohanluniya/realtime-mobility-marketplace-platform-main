"""Avro event contract registry, 12-topic topology definitions, and schema compatibility checks."""

from __future__ import annotations

import io
import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fastavro

MAGIC_BYTE = 0x00


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    key_domain: str
    schema_file: str
    schema_id: int
    retention_ms: int
    compression: str = "zstd"
    cleanup_policy: str = "delete"


TOPIC_SPECS: dict[str, TopicSpec] = {
    "marketplace.rider.requests": TopicSpec(
        "marketplace.rider.requests", 12, "request_id", "marketplace_event.avsc", 1, 7 * 86_400_000
    ),
    "marketplace.driver.locations": TopicSpec(
        "marketplace.driver.locations", 12, "driver_id", "marketplace_event.avsc", 1, 3 * 86_400_000
    ),
    "marketplace.driver.availability": TopicSpec(
        "marketplace.driver.availability", 12, "driver_id", "marketplace_event.avsc", 1, 7 * 86_400_000
    ),
    "marketplace.trip.lifecycle": TopicSpec(
        "marketplace.trip.lifecycle", 12, "trip_id", "marketplace_event.avsc", 1, 30 * 86_400_000
    ),
    "marketplace.dispatch.offers": TopicSpec(
        "marketplace.dispatch.offers", 12, "request_id", "marketplace_event.avsc", 1, 14 * 86_400_000
    ),
    "marketplace.pricing.policies": TopicSpec(
        "marketplace.pricing.policies",
        3,
        "region_id",
        "marketplace_event.avsc",
        1,
        30 * 86_400_000,
        cleanup_policy="compact,delete",
    ),
    "marketplace.regional.events": TopicSpec(
        "marketplace.regional.events", 6, "region_id", "marketplace_event.avsc", 1, 14 * 86_400_000
    ),
    "marketplace.demand.aggregates": TopicSpec(
        "marketplace.demand.aggregates", 12, "h3_cell", "cell_aggregate_event.avsc", 2, 7 * 86_400_000
    ),
    "marketplace.supply.aggregates": TopicSpec(
        "marketplace.supply.aggregates", 12, "h3_cell", "cell_aggregate_event.avsc", 2, 7 * 86_400_000
    ),
    "marketplace.pricing.decisions": TopicSpec(
        "marketplace.pricing.decisions", 12, "h3_cell", "pricing_decision_event.avsc", 3, 30 * 86_400_000
    ),
    "marketplace.matching.outcomes": TopicSpec(
        "marketplace.matching.outcomes", 12, "request_id", "matching_outcome_event.avsc", 4, 30 * 86_400_000
    ),
    "marketplace.dead-letter": TopicSpec(
        "marketplace.dead-letter", 3, "source_key", "dlq_event.avsc", 5, 30 * 86_400_000
    ),
}


class SchemaValidationError(ValueError):
    """Raised when a payload fails Avro wire framing or schema validation."""


class AvroContractRegistry:
    def __init__(self, schema_dir: Path | None = None) -> None:
        self.schema_dir = schema_dir or (Path(__file__).resolve().parent.parent / "schemas" / "avro")
        self._raw_by_id: dict[int, dict[str, Any]] = {}
        self._parsed_by_id: dict[int, Any] = {}
        self._load_all()

    def _load_all(self) -> None:
        for spec in TOPIC_SPECS.values():
            if spec.schema_id not in self._parsed_by_id:
                raw = json.loads((self.schema_dir / spec.schema_file).read_text(encoding="utf-8"))
                self._raw_by_id[spec.schema_id] = raw
                self._parsed_by_id[spec.schema_id] = fastavro.parse_schema(raw)

    def get_raw_schema(self, topic: str) -> dict[str, Any]:
        return self._raw_by_id[TOPIC_SPECS[topic].schema_id]

    def serialize(self, topic: str, record: dict[str, Any]) -> bytes:
        spec = TOPIC_SPECS[topic]
        parsed = self._parsed_by_id[spec.schema_id]
        buf = io.BytesIO()
        buf.write(struct.pack(">BI", MAGIC_BYTE, spec.schema_id))
        try:
            fastavro.schemaless_writer(buf, parsed, record)
        except Exception as exc:
            raise SchemaValidationError(f"Avro encode failed for {topic}: {exc}") from exc
        return buf.getvalue()

    def deserialize(
        self, payload: bytes, *, expected_topic: str | None = None
    ) -> tuple[int, dict[str, Any]]:
        if len(payload) < 5 or payload[0] != MAGIC_BYTE:
            raise SchemaValidationError("Corrupt Avro wire header")
        schema_id = struct.unpack(">I", payload[1:5])[0]
        if schema_id not in self._parsed_by_id:
            raise SchemaValidationError(f"Unknown schema_id={schema_id}")
        if expected_topic is not None and schema_id != TOPIC_SPECS[expected_topic].schema_id:
            raise SchemaValidationError(
                f"Schema mismatch for {expected_topic}: got {schema_id}"
            )
        buf = io.BytesIO(payload[5:])
        try:
            decoded = fastavro.schemaless_reader(buf, self._parsed_by_id[schema_id])
        except Exception as exc:
            raise SchemaValidationError(f"Avro decode failed: {exc}") from exc
        if not isinstance(decoded, dict):
            raise SchemaValidationError("Decoded payload is not a record")
        return schema_id, decoded

    def check_compatibility(
        self,
        writer_schema: dict[str, Any],
        reader_schema: dict[str, Any],
        sample: dict[str, Any],
    ) -> tuple[bool, str]:
        try:
            w = fastavro.parse_schema(writer_schema)
            r = fastavro.parse_schema(reader_schema)
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, w, sample)
            buf.seek(0)
            decoded = fastavro.schemaless_reader(buf, w, r)
            return (isinstance(decoded, dict), "compatible")
        except Exception as exc:
            return False, str(exc)
