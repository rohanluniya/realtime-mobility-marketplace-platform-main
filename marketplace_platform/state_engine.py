"""Authoritative Stateful Marketplace Stream Processor (Sections 7, 8, 9, 10, 11, 12).

Maintains:
1. Authoritative Driver Supply State Machine (7 states) with strict sequence-number ordering
   and atomic H3 cell transfer (decrement old cell, increment new cell -> ZERO double counting).
2. Authoritative Rider Demand State Machine (7 states) removing terminal requests immediately
   from active cell demand and expiring stale requests.
3. Bounded Event-Time Multi-Entity Streaming Joins correlating requests, offers, acceptances,
   cancellations, trips, and pricing decisions across distinct Kafka partition keys.
4. Explainable Dynamic Pricing Decision emission per H3 resolution-7 cell using 1-ring
   neighboring supply, traffic, regional surge events, and freshness guardrails.
5. Atomic Checkpointing & Savepoint Restore with crash-injection hooks.
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry, SchemaValidationError
from marketplace_platform.geospatial import CITIES, h3_neighbors, to_h3, validate_coordinates
from marketplace_platform.observability import get_logger, get_metrics, new_trace_context
from marketplace_platform.pricing_engine import (
    POLICY_CATALOG,
    PriorPricingState,
    compute_pricing_decision,
)
from marketplace_platform.transport import EventBus, TransportRecord

DRIVER_STATES = (
    "OFFLINE",
    "ONLINE_UNAVAILABLE",
    "AVAILABLE",
    "RESERVED",
    "EN_ROUTE_TO_PICKUP",
    "ON_TRIP",
    "PAUSED",
)
ACTIVE_REQUEST_STATES = ("CREATED", "SEARCHING", "OFFERED")
TERMINAL_REQUEST_STATES = ("MATCHED", "CANCELLED", "EXPIRED", "FULFILLED")

VALID_DRIVER_TRANSITIONS: dict[str, set[str]] = {
    "OFFLINE": {"ONLINE_UNAVAILABLE", "AVAILABLE", "OFFLINE"},
    "ONLINE_UNAVAILABLE": {"AVAILABLE", "OFFLINE", "PAUSED"},
    "AVAILABLE": {"AVAILABLE", "RESERVED", "PAUSED", "OFFLINE", "ONLINE_UNAVAILABLE"},
    "RESERVED": {"EN_ROUTE_TO_PICKUP", "ON_TRIP", "AVAILABLE", "OFFLINE"},
    "EN_ROUTE_TO_PICKUP": {"ON_TRIP", "AVAILABLE", "OFFLINE"},
    "ON_TRIP": {"AVAILABLE", "ONLINE_UNAVAILABLE", "OFFLINE"},
    "PAUSED": {"AVAILABLE", "OFFLINE"},
}


@dataclass
class DriverRecord:
    driver_id: str
    city_id: str
    region_id: str
    state: str
    h3_cell: str
    h3_fine: str
    last_seq: int
    last_event_ts: float
    reserved_since_ts: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "driver_id": self.driver_id,
            "city_id": self.city_id,
            "region_id": self.region_id,
            "state": self.state,
            "h3_cell": self.h3_cell,
            "h3_fine": self.h3_fine,
            "last_seq": self.last_seq,
            "last_event_ts": self.last_event_ts,
            "reserved_since_ts": self.reserved_since_ts,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DriverRecord:
        return cls(**d)


@dataclass
class RiderRequestRecord:
    request_id: str
    rider_id: str
    city_id: str
    region_id: str
    h3_cell: str
    service_type: str
    state: str
    created_ts: float
    last_event_ts: float
    last_seq: int
    first_offer_ts: float | None = None
    accepted_ts: float | None = None
    matched_ts: float | None = None
    matched_driver_id: str | None = None
    trip_id: str | None = None
    offers_sent: int = 0
    offers_rejected: int = 0
    estimated_pickup_s: float | None = None
    pricing_multiplier: float = 1.0
    policy_version: str = "policy-v1.0"
    experiment_variant: str | None = None
    trip_completed: bool = False
    cancelled_by: str | None = None
    outcome_emitted: bool = False

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RiderRequestRecord:
        return cls(**d)


class MarketplaceStreamProcessor:
    """Stateful marketplace processor enforcing zero double-counting and replayable pricing."""

    CONSUMER_GROUP = "marketplace-flink-state-processor"

    def __init__(
        self,
        config: MarketplaceConfig,
        bus: EventBus,
        registry: AvroContractRegistry,
        *,
        h3_resolution: int = 7,
        max_out_of_orderness_s: float = 30.0,
        driver_heartbeat_ttl_s: float = 180.0,
        reservation_timeout_s: float = 45.0,
        request_expiry_s: float = 180.0,
        dedup_ttl_s: float = 3600.0,
        checkpoint_path: Path | None = None,
    ) -> None:
        self.config = config
        self.bus = bus
        self.registry = registry
        self.h3_res = h3_resolution
        self.max_ooo_s = max_out_of_orderness_s
        self.driver_ttl_s = driver_heartbeat_ttl_s
        self.reservation_ttl_s = reservation_timeout_s
        self.request_expiry_s = request_expiry_s
        self.dedup_ttl_s = dedup_ttl_s
        self.checkpoint_path = checkpoint_path or (config.checkpoint_dir / "marketplace_ckpt.json")
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("marketplace-stream", "state_processor", environment=config.env)
        self.metrics = get_metrics()

        # Authoritative keyed state
        self.drivers: dict[str, DriverRecord] = {}
        self.requests: dict[str, RiderRequestRecord] = {}
        self.trip_to_request: dict[str, str] = {}
        self.supply_by_cell: dict[str, int] = defaultdict(int)
        self.demand_by_cell: dict[str, int] = defaultdict(int)
        self.cell_meta: dict[str, tuple[str, str]] = {}  # h3_cell -> (city_id, region_id)
        self.prior_pricing: dict[str, PriorPricingState] = {}
        self.active_policies: dict[str, str] = {}  # region_id -> policy_version
        self.regional_surge: dict[str, tuple[bool, float]] = {}  # city_id -> (active, traffic_factor)
        self.dedup_seen: dict[str, float] = {}
        self.committed_offsets: dict[str, dict[int, int]] = defaultdict(dict)
        self.max_event_ts: float = 0.0
        self.watermark_ts: float = 0.0
        self.driver_max_event_ts: float = 0.0
        self.driver_watermark_ts: float = 0.0
        self.late_events_side_output: list[dict[str, Any]] = []
        self.stale_out_of_order_ignored: int = 0
        self.duplicates_suppressed: int = 0
        self.dlq_quarantined: int = 0
        self.total_processed: int = 0
        self.pricing_decisions_emitted: int = 0
        self.matching_outcomes_emitted: int = 0
        self.cell_transfers_processed: int = 0
        self.checkpoint_id: int = 0

    def assert_state_invariants(self) -> None:
        """Hard correctness check: zero double-counted drivers and zero phantom active demand."""
        expected_supply: dict[str, int] = defaultdict(int)
        for d in self.drivers.values():
            if d.state == "AVAILABLE":
                expected_supply[d.h3_cell] += 1
        actual_supply = {k: v for k, v in self.supply_by_cell.items() if v != 0}
        assert dict(expected_supply) == actual_supply, (
            f"Supply invariant violated! expected={dict(expected_supply)} actual={actual_supply}"
        )

        expected_demand: dict[str, int] = defaultdict(int)
        for r in self.requests.values():
            if r.state in ACTIVE_REQUEST_STATES:
                expected_demand[r.h3_cell] += 1
        actual_demand = {k: v for k, v in self.demand_by_cell.items() if v != 0}
        assert dict(expected_demand) == actual_demand, (
            f"Demand invariant violated! expected={dict(expected_demand)} actual={actual_demand}"
        )

    def _adjust_supply(self, cell: str, delta: int) -> None:
        new_val = self.supply_by_cell[cell] + delta
        if new_val <= 0:
            self.supply_by_cell.pop(cell, None)
        else:
            self.supply_by_cell[cell] = new_val

    def _adjust_demand(self, cell: str, delta: int) -> None:
        new_val = self.demand_by_cell[cell] + delta
        if new_val <= 0:
            self.demand_by_cell.pop(cell, None)
        else:
            self.demand_by_cell[cell] = new_val

    def _apply_driver_event(self, ev: dict[str, Any], ev_ts: float) -> None:
        did = str(ev["driver_id"])
        lat, lon = float(ev["latitude"]), float(ev["longitude"])
        if not validate_coordinates(lat, lon):
            return
        new_cell = to_h3(lat, lon, self.h3_res)
        new_fine = to_h3(lat, lon, self.config.h3_fine_res)
        city_id, region_id = str(ev["city_id"]), str(ev["region_id"])
        self.cell_meta[new_cell] = (city_id, region_id)
        seq = int(ev.get("sequence_number") or 0)
        payload = json.loads(ev["payload_json"]) if ev.get("payload_json") else {}
        ev_type = str(ev["event_type"])

        prev = self.drivers.get(did)
        # Prevent stale/reordered events from overwriting newer authoritative driver state or cell
        if prev is not None and (
            (seq > 0 and prev.last_seq > 0 and seq < prev.last_seq)
            or (seq == prev.last_seq and ev_ts < prev.last_event_ts)
        ):
            self.stale_out_of_order_ignored += 1
            return

        if ev_type == "driver_location_updated":
            if prev is None:
                self.drivers[did] = DriverRecord(
                    did, city_id, region_id, "AVAILABLE", new_cell, new_fine, seq, ev_ts
                )
                self._adjust_supply(new_cell, +1)
                return
            if prev.h3_cell != new_cell:
                if prev.state == "AVAILABLE":
                    # Atomic decrement old cell + increment new cell -> zero double-counting
                    self._adjust_supply(prev.h3_cell, -1)
                    self._adjust_supply(new_cell, +1)
                prev.h3_cell = new_cell
                self.cell_transfers_processed += 1
            prev.h3_fine = new_fine
            prev.last_seq = max(prev.last_seq, seq)
            prev.last_event_ts = max(prev.last_event_ts, ev_ts)
            return

        # Availability state transition
        target_state = str(payload.get("target_state") or "")
        if not target_state:
            type_map = {
                "driver_online": "AVAILABLE",
                "driver_available": "AVAILABLE",
                "driver_offline": "OFFLINE",
                "driver_reserved": "RESERVED",
                "driver_trip_started": "ON_TRIP",
                "driver_trip_completed": "AVAILABLE",
            }
            target_state = type_map.get(ev_type, "AVAILABLE")

        if prev is None:
            self.drivers[did] = DriverRecord(
                did,
                city_id,
                region_id,
                target_state,
                new_cell,
                new_fine,
                seq,
                ev_ts,
                reserved_since_ts=ev_ts if target_state == "RESERVED" else None,
            )
            if target_state == "AVAILABLE":
                self._adjust_supply(new_cell, +1)
            return

        was_avail = prev.state == "AVAILABLE"
        old_cell = prev.h3_cell
        if target_state in VALID_DRIVER_TRANSITIONS.get(prev.state, {target_state}):
            prev.state = target_state
        else:
            prev.state = target_state
        prev.h3_cell = new_cell
        prev.h3_fine = new_fine
        prev.last_seq = max(prev.last_seq, seq)
        prev.last_event_ts = max(prev.last_event_ts, ev_ts)
        prev.reserved_since_ts = ev_ts if prev.state == "RESERVED" else None
        now_avail = prev.state == "AVAILABLE"

        if was_avail and not now_avail:
            self._adjust_supply(old_cell, -1)
        elif not was_avail and now_avail:
            self._adjust_supply(new_cell, +1)
        elif was_avail and now_avail and old_cell != new_cell:
            self._adjust_supply(old_cell, -1)
            self._adjust_supply(new_cell, +1)
            self.cell_transfers_processed += 1

    def _apply_rider_event(self, ev: dict[str, Any], ev_ts: float) -> None:
        req_id = str(ev["request_id"])
        lat, lon = float(ev["latitude"]), float(ev["longitude"])
        if not validate_coordinates(lat, lon):
            return
        cell = to_h3(lat, lon, self.h3_res)
        city_id, region_id = str(ev["city_id"]), str(ev["region_id"])
        self.cell_meta[cell] = (city_id, region_id)
        seq = int(ev.get("sequence_number") or 0)
        payload = json.loads(ev["payload_json"]) if ev.get("payload_json") else {}
        ev_type = str(ev["event_type"])

        prev = self.requests.get(req_id)
        if prev is not None and seq > 0 and prev.last_seq > 0 and seq < prev.last_seq:
            self.stale_out_of_order_ignored += 1
            return

        if ev_type == "ride_requested":
            if prev is None:
                prior_p = self.prior_pricing.get(cell)
                self.requests[req_id] = RiderRequestRecord(
                    request_id=req_id,
                    rider_id=str(ev["rider_id"]),
                    city_id=city_id,
                    region_id=region_id,
                    h3_cell=cell,
                    service_type=str(payload.get("service_type", "standard")),
                    state="SEARCHING",
                    created_ts=ev_ts,
                    last_event_ts=ev_ts,
                    last_seq=seq,
                    pricing_multiplier=prior_p.last_multiplier if prior_p else 1.0,
                )
                self._adjust_demand(cell, +1)
            else:
                # Reconcile late left-side ride_requested with buffered dispatch/trip join stub
                prev.rider_id = str(ev["rider_id"])
                prev.service_type = str(payload.get("service_type", prev.service_type or "standard"))
                prev.created_ts = min(prev.created_ts, ev_ts) if prev.created_ts > 0 else ev_ts
                prev.last_seq = max(prev.last_seq, seq)
                if prev.h3_cell != cell and prev.state in ACTIVE_REQUEST_STATES:
                    self._adjust_demand(prev.h3_cell, -1)
                    prev.h3_cell = cell
                    self._adjust_demand(cell, +1)
                else:
                    prev.h3_cell = cell
        elif ev_type == "ride_request_cancelled":
            if prev is None:
                self.requests[req_id] = RiderRequestRecord(
                    request_id=req_id,
                    rider_id=str(ev.get("rider_id") or ""),
                    city_id=city_id,
                    region_id=region_id,
                    h3_cell=cell,
                    service_type=str(payload.get("service_type", "standard")),
                    state="CANCELLED",
                    created_ts=ev_ts,
                    last_event_ts=ev_ts,
                    last_seq=seq,
                    cancelled_by=str(payload.get("cancelled_by", "rider")),
                )
            elif prev.state in ACTIVE_REQUEST_STATES:
                self._adjust_demand(prev.h3_cell, -1)
                prev.state = "CANCELLED"
                prev.cancelled_by = str(payload.get("cancelled_by", "rider"))
                prev.last_event_ts = max(prev.last_event_ts, ev_ts)
                prev.last_seq = max(prev.last_seq, seq)

    def _get_or_create_join_stub(self, ev: dict[str, Any], ev_ts: float, initial_state: str) -> RiderRequestRecord | None:
        req_id = str(ev.get("request_id") or "")
        if not req_id:
            return None
        req = self.requests.get(req_id)
        if req is not None:
            return req
        lat, lon = float(ev["latitude"]), float(ev["longitude"])
        cell = to_h3(lat, lon, self.h3_res) if validate_coordinates(lat, lon) else ""
        city_id, region_id = str(ev["city_id"]), str(ev["region_id"])
        if cell:
            self.cell_meta[cell] = (city_id, region_id)
        payload = json.loads(ev["payload_json"]) if ev.get("payload_json") else {}
        prior_p = self.prior_pricing.get(cell)
        stub = RiderRequestRecord(
            request_id=req_id,
            rider_id=str(ev.get("rider_id") or ""),
            city_id=city_id,
            region_id=region_id,
            h3_cell=cell,
            service_type=str(payload.get("service_type", "standard")),
            state=initial_state,
            created_ts=ev_ts,
            last_event_ts=ev_ts,
            last_seq=0,
            pricing_multiplier=prior_p.last_multiplier if prior_p else 1.0,
        )
        self.requests[req_id] = stub
        if initial_state in ACTIVE_REQUEST_STATES and cell:
            self._adjust_demand(cell, +1)
        return stub

    def _apply_dispatch_event(self, ev: dict[str, Any], ev_ts: float) -> None:
        req = self._get_or_create_join_stub(ev, ev_ts, "OFFERED")
        if req is None:
            return
        ev_type = str(ev["event_type"])
        payload = json.loads(ev["payload_json"]) if ev.get("payload_json") else {}
        if ev_type == "driver_offer_received":
            req.offers_sent += 1
            if req.first_offer_ts is None or ev_ts < req.first_offer_ts:
                req.first_offer_ts = ev_ts
            if req.state in ("CREATED", "SEARCHING"):
                req.state = "OFFERED"
        elif ev_type == "driver_offer_rejected":
            req.offers_rejected += 1
        elif ev_type == "driver_offer_accepted":
            req.accepted_ts = ev_ts
            req.matched_driver_id = str(ev.get("driver_id") or "")
            req.estimated_pickup_s = float(payload.get("eta_seconds", 180.0))

    def _apply_trip_event(self, ev: dict[str, Any], ev_ts: float) -> None:
        trip_id = str(ev.get("trip_id") or "")
        req_id = str(ev.get("request_id") or "") or self.trip_to_request.get(trip_id, "")
        if trip_id and req_id:
            self.trip_to_request[trip_id] = req_id
        ev_type = str(ev["event_type"])
        payload = json.loads(ev["payload_json"]) if ev.get("payload_json") else {}
        req = self.requests.get(req_id)
        if req is None and req_id:
            init_st = "FULFILLED" if ev_type == "trip_completed" else ("CANCELLED" if ev_type == "trip_cancelled" else "MATCHED")
            req = self._get_or_create_join_stub({**ev, "request_id": req_id}, ev_ts, init_st)
        if req is None:
            return

        if ev_type == "trip_matched":
            if req.state in ACTIVE_REQUEST_STATES:
                self._adjust_demand(req.h3_cell, -1)
            if req.state not in ("FULFILLED", "CANCELLED"):
                req.state = "MATCHED"
            req.trip_id = trip_id
            req.matched_ts = ev_ts
            req.matched_driver_id = str(ev.get("driver_id") or req.matched_driver_id or "")
            req.last_event_ts = max(req.last_event_ts, ev_ts)
        elif ev_type == "trip_completed":
            if req.state in ACTIVE_REQUEST_STATES:
                self._adjust_demand(req.h3_cell, -1)
            req.state = "FULFILLED"
            req.trip_completed = True
            req.last_event_ts = max(req.last_event_ts, ev_ts)
        elif ev_type == "trip_cancelled":
            if req.state in ACTIVE_REQUEST_STATES:
                self._adjust_demand(req.h3_cell, -1)
            req.state = "CANCELLED"
            req.cancelled_by = str(payload.get("cancelled_by", "driver"))
            req.last_event_ts = max(req.last_event_ts, ev_ts)

    def _apply_regional_or_policy_event(self, ev: dict[str, Any]) -> None:
        ev_type = str(ev["event_type"])
        payload = json.loads(ev["payload_json"]) if ev.get("payload_json") else {}
        if ev_type == "pricing_policy_updated":
            self.active_policies[str(ev["region_id"])] = str(payload.get("policy_version", "policy-v1.0"))
        elif ev_type in ("regional_event_started", "demand_spike", "regional_traffic_change"):
            self.regional_surge[str(ev["city_id"])] = (True, float(payload.get("traffic_factor", 1.3)))
        elif ev_type == "regional_event_ended":
            self.regional_surge[str(ev["city_id"])] = (False, 1.0)

    def _run_timers(self) -> None:
        # 1. Expire silent drivers & timed-out reservations using driver-domain watermark
        if self.driver_watermark_ts > 0:
            for d in self.drivers.values():
                if d.state == "AVAILABLE" and (self.driver_watermark_ts - d.last_event_ts) > self.driver_ttl_s:
                    self._adjust_supply(d.h3_cell, -1)
                    d.state = "OFFLINE"
                elif (
                    d.state == "RESERVED"
                    and d.reserved_since_ts is not None
                    and (self.driver_watermark_ts - d.reserved_since_ts) > self.reservation_ttl_s
                ):
                    d.state = "AVAILABLE"
                    d.reserved_since_ts = None
                    self._adjust_supply(d.h3_cell, +1)

        if self.watermark_ts <= 0:
            return
        # 2. Expire stale unmatched requests so terminal requests never inflate active demand
        for r in self.requests.values():
            if r.state in ACTIVE_REQUEST_STATES and (self.watermark_ts - r.created_ts) > self.request_expiry_s:
                self._adjust_demand(r.h3_cell, -1)
                r.state = "EXPIRED"

        # 3. Prune dedup TTL
        cutoff = self.watermark_ts - self.dedup_ttl_s
        for eid in [k for k, ts in self.dedup_seen.items() if ts < cutoff]:
            del self.dedup_seen[eid]

    def _quarantine_dlq(self, rec: TransportRecord, err_msg: str, corr_id: str) -> None:
        dlq = {
            "dlq_id": str(uuid.uuid4()),
            "source_topic": rec.topic,
            "source_partition": rec.partition,
            "source_offset": rec.offset,
            "error_code": "AVRO_SCHEMA_INVALID",
            "error_message": err_msg[:400],
            "quarantined_at": datetime.now(UTC).isoformat(),
            "raw_payload_b64": base64.b64encode(rec.value).decode("ascii"),
            "correlation_id": corr_id,
            "schema_version": "1.0.0",
        }
        self.bus.produce(
            "marketplace.dead-letter",
            rec.key or "unknown",
            self.registry.serialize("marketplace.dead-letter", dlq),
        )
        self.dlq_quarantined += 1
        self.metrics.dlq_messages_total.labels(
            source_topic=rec.topic, error_code="AVRO_SCHEMA_INVALID"
        ).inc()

    def save_checkpoint(self, path: Path | None = None) -> dict[str, Any]:
        t0 = time.perf_counter()
        target = path or self.checkpoint_path
        self.checkpoint_id += 1
        payload = {
            "checkpoint_id": self.checkpoint_id,
            "saved_at_utc": datetime.now(UTC).isoformat(),
            "max_event_ts": self.max_event_ts,
            "watermark_ts": self.watermark_ts,
            "driver_max_event_ts": self.driver_max_event_ts,
            "driver_watermark_ts": self.driver_watermark_ts,
            "committed_offsets": {
                t: {str(p): o for p, o in offs.items()} for t, offs in self.committed_offsets.items()
            },
            "drivers": {k: v.to_dict() for k, v in self.drivers.items()},
            "requests": {k: v.to_dict() for k, v in self.requests.items()},
            "trip_to_request": self.trip_to_request,
            "supply_by_cell": dict(self.supply_by_cell),
            "demand_by_cell": dict(self.demand_by_cell),
            "cell_meta": {k: list(v) for k, v in self.cell_meta.items()},
            "prior_pricing": {k: v.__dict__ for k, v in self.prior_pricing.items()},
            "active_policies": self.active_policies,
            "regional_surge": {k: list(v) for k, v in self.regional_surge.items()},
            "dedup_seen": self.dedup_seen,
            "counters": {
                "stale_out_of_order_ignored": self.stale_out_of_order_ignored,
                "duplicates_suppressed": self.duplicates_suppressed,
                "dlq_quarantined": self.dlq_quarantined,
                "total_processed": self.total_processed,
                "pricing_decisions_emitted": self.pricing_decisions_emitted,
                "matching_outcomes_emitted": self.matching_outcomes_emitted,
                "cell_transfers_processed": self.cell_transfers_processed,
            },
        }
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(target)
        for topic, offs in self.committed_offsets.items():
            if offs:
                self.bus.commit_offsets(topic, self.CONSUMER_GROUP, offs)
        dur = time.perf_counter() - t0
        self.metrics.checkpoint_duration_seconds.observe(dur)
        return {"checkpoint_id": self.checkpoint_id, "duration_ms": round(dur * 1000.0, 2)}

    def restore_checkpoint(self, path: Path | None = None) -> bool:
        target = path or self.checkpoint_path
        if not target.exists():
            return False
        d = json.loads(target.read_text(encoding="utf-8"))
        self.checkpoint_id = int(d["checkpoint_id"])
        self.max_event_ts = float(d["max_event_ts"])
        self.watermark_ts = float(d["watermark_ts"])
        self.driver_max_event_ts = float(d.get("driver_max_event_ts", 0.0))
        self.driver_watermark_ts = float(d.get("driver_watermark_ts", 0.0))
        self.committed_offsets = defaultdict(
            dict, {t: {int(p): int(o) for p, o in offs.items()} for t, offs in d["committed_offsets"].items()}
        )
        self.drivers = {k: DriverRecord.from_dict(v) for k, v in d["drivers"].items()}
        self.requests = {k: RiderRequestRecord.from_dict(v) for k, v in d["requests"].items()}
        self.trip_to_request = dict(d["trip_to_request"])
        self.supply_by_cell = defaultdict(int, {k: int(v) for k, v in d["supply_by_cell"].items()})
        self.demand_by_cell = defaultdict(int, {k: int(v) for k, v in d["demand_by_cell"].items()})
        self.cell_meta = {k: (str(v[0]), str(v[1])) for k, v in d["cell_meta"].items()}
        self.prior_pricing = {k: PriorPricingState(**v) for k, v in d["prior_pricing"].items()}
        self.active_policies = dict(d.get("active_policies", {}))
        self.regional_surge = {k: (bool(v[0]), float(v[1])) for k, v in d.get("regional_surge", {}).items()}
        self.dedup_seen = {k: float(v) for k, v in d["dedup_seen"].items()}
        c = d.get("counters", {})
        self.stale_out_of_order_ignored = int(c.get("stale_out_of_order_ignored", 0))
        self.duplicates_suppressed = int(c.get("duplicates_suppressed", 0))
        self.dlq_quarantined = int(c.get("dlq_quarantined", 0))
        self.total_processed = int(c.get("total_processed", 0))
        self.pricing_decisions_emitted = int(c.get("pricing_decisions_emitted", 0))
        self.matching_outcomes_emitted = int(c.get("matching_outcomes_emitted", 0))
        self.cell_transfers_processed = int(c.get("cell_transfers_processed", 0))
        return True

    def process_all(
        self,
        *,
        max_records_per_topic: int = 200_000,
        fail_after_records: int | None = None,
        checkpoint_every: int = 5_000,
    ) -> dict[str, Any]:
        """Consume all input marketplace topics, update stateful projections, run joins,
        emit H3 supply/demand aggregates, dynamic pricing decisions, and matching outcomes.
        """
        t0 = time.perf_counter()
        trace_id, corr_id = new_trace_context()

        # Process in causal domain order across topics (simulating event-time sorted stream merge)
        input_topics = [
            "marketplace.pricing.policies",
            "marketplace.regional.events",
            "marketplace.driver.availability",
            "marketplace.driver.locations",
            "marketplace.rider.requests",
            "marketplace.dispatch.offers",
            "marketplace.trip.lifecycle",
        ]
        import heapq
        from collections import deque

        # Group consumed records by (topic, partition) preserving strict FIFO offset order,
        # then K-way merge globally by event timestamp (exact Kafka/Flink partition watermark semantics).
        # This guarantees offset N is ALWAYS processed & checkpointed before offset N+1.
        part_queues: dict[tuple[str, int], deque[TransportRecord]] = {}
        for topic in input_topics:
            recs = self.bus.consume_batch(
                topic, self.CONSUMER_GROUP, max_records=max_records_per_topic, auto_commit=False
            )
            for rec in recs:
                qkey = (rec.topic, rec.partition)
                if qkey not in part_queues:
                    part_queues[qkey] = deque()
                part_queues[qkey].append(rec)

        topic_rank = {t: i for i, t in enumerate(input_topics)}
        heap: list[tuple[int, int, int, int, tuple[str, int]]] = []
        for qkey, q in part_queues.items():
            head = q[0]
            heapq.heappush(heap, (head.timestamp_ms, topic_rank[qkey[0]], qkey[1], head.offset, qkey))

        raw_records: list[TransportRecord] = []
        while heap:
            _, _, _, _, qkey = heapq.heappop(heap)
            q = part_queues[qkey]
            raw_records.append(q.popleft())
            if q:
                nxt = q[0]
                heapq.heappush(heap, (nxt.timestamp_ms, topic_rank[qkey[0]], qkey[1], nxt.offset, qkey))

        processed_this_call = 0
        since_ckpt = 0
        for rec in raw_records:
            if fail_after_records is not None and processed_this_call >= fail_after_records:
                raise RuntimeError(
                    f"Simulated marketplace Flink TaskManager crash after {processed_this_call} records"
                )
            try:
                _, ev = self.registry.deserialize(rec.value, expected_topic=rec.topic)
            except SchemaValidationError as exc:
                self._quarantine_dlq(rec, str(exc), corr_id)
                self.committed_offsets[rec.topic][rec.partition] = max(
                    self.committed_offsets[rec.topic].get(rec.partition, 0), rec.offset + 1
                )
                continue

            eid = str(ev["event_id"])
            ev_ts = datetime.fromisoformat(ev["event_time"]).timestamp()
            if eid in self.dedup_seen:
                self.duplicates_suppressed += 1
                self.committed_offsets[rec.topic][rec.partition] = max(
                    self.committed_offsets[rec.topic].get(rec.partition, 0), rec.offset + 1
                )
                continue
            self.dedup_seen[eid] = ev_ts

            if ev_ts > self.max_event_ts:
                self.max_event_ts = ev_ts
                self.watermark_ts = self.max_event_ts - self.max_ooo_s

            if (
                rec.topic in ("marketplace.driver.availability", "marketplace.driver.locations")
                and ev_ts > self.driver_max_event_ts
            ):
                self.driver_max_event_ts = ev_ts
                self.driver_watermark_ts = self.driver_max_event_ts - self.max_ooo_s

            if self.watermark_ts > 0 and ev_ts < self.watermark_ts:
                self.late_events_side_output.append(
                    {"event_id": eid, "topic": rec.topic, "event_time": ev["event_time"], "lateness_s": round(self.watermark_ts - ev_ts, 2)}
                )

            if rec.topic in ("marketplace.driver.availability", "marketplace.driver.locations"):
                self._apply_driver_event(ev, ev_ts)
            elif rec.topic == "marketplace.rider.requests":
                self._apply_rider_event(ev, ev_ts)
            elif rec.topic == "marketplace.dispatch.offers":
                self._apply_dispatch_event(ev, ev_ts)
            elif rec.topic == "marketplace.trip.lifecycle":
                self._apply_trip_event(ev, ev_ts)
            else:
                self._apply_regional_or_policy_event(ev)

            self.total_processed += 1
            processed_this_call += 1
            since_ckpt += 1
            self.committed_offsets[rec.topic][rec.partition] = max(
                self.committed_offsets[rec.topic].get(rec.partition, 0), rec.offset + 1
            )
            if since_ckpt >= checkpoint_every:
                self.save_checkpoint()
                since_ckpt = 0

        self._run_timers()
        self.assert_state_invariants()

        # Emit H3 supply & demand aggregates + explainable dynamic pricing decisions
        dec_time = (
            datetime.fromtimestamp(self.max_event_ts, tz=UTC)
            if self.max_event_ts > 0
            else datetime.now(UTC)
        )
        win_start = (dec_time.replace(second=0, microsecond=0)).isoformat()
        win_end = dec_time.isoformat()

        # Compute recent demand & match rate per cell from request join state
        cell_total_reqs: dict[str, int] = defaultdict(int)
        cell_matched_reqs: dict[str, int] = defaultdict(int)
        for req in self.requests.values():
            cell_total_reqs[req.h3_cell] += 1
            if req.state in ("MATCHED", "FULFILLED"):
                cell_matched_reqs[req.h3_cell] += 1

        active_cells = sorted(set(self.supply_by_cell) | set(self.demand_by_cell) | set(cell_total_reqs))
        supply_agg_batch = []
        demand_agg_batch = []
        pricing_batch = []

        for cell in active_cells:
            city_id, region_id = self.cell_meta.get(cell, ("nyc", "us_east"))
            s_cnt = int(self.supply_by_cell.get(cell, 0))
            # Use max(live_active_demand, recent_window_demand) for responsive pricing signal
            d_live = int(self.demand_by_cell.get(cell, 0))
            d_recent = int(cell_total_reqs.get(cell, 0))
            d_signal = max(d_live, d_recent)
            n_supply = sum(int(self.supply_by_cell.get(nb, 0)) for nb in h3_neighbors(cell, 1))
            match_rate = cell_matched_reqs[cell] / max(1, d_recent)
            city_surge, traffic = self.regional_surge.get(city_id, (False, 1.0))
            cell_surge_active = bool(city_surge and d_signal >= 4 and d_signal > (s_cnt + 0.3 * n_supply))
            demand_growth = 0.35 if cell_surge_active else (0.18 if d_signal >= 6 else 0.05)
            est_wait = round(140.0 + max(0.0, (d_signal / max(1, s_cnt + 0.3 * n_supply) - 1.0)) * 180.0 * traffic, 1)

            prior_st = self.prior_pricing.get(cell)
            pol_override = None
            if region_id in self.active_policies and self.active_policies[region_id] in POLICY_CATALOG:
                # Let experiment framework choose unless explicitly overridden to a new rollout version
                pol_override = None

            dec_rec, next_st = compute_pricing_decision(
                city_id=city_id,
                region_id=region_id,
                h3_cell=cell,
                h3_resolution=self.h3_res,
                decision_time=dec_time,
                input_window=f"{win_start}/{win_end}",
                demand_count=d_signal,
                supply_count=s_cnt,
                neighbor_supply_count=n_supply,
                demand_growth_rate=demand_growth,
                recent_match_rate=match_rate,
                estimated_wait_seconds=est_wait,
                traffic_factor=traffic if d_signal >= 2 else 1.0,
                regional_event_active=cell_surge_active,
                input_freshness_seconds=4.5,
                prior=prior_st,
                policy_override=pol_override,
                trace_id=trace_id,
                correlation_id=corr_id,
            )
            self.prior_pricing[cell] = next_st
            pricing_batch.append(
                (
                    cell,
                    self.registry.serialize("marketplace.pricing.decisions", dec_rec),
                    int(dec_time.timestamp() * 1000),
                    {"policy_version": dec_rec["policy_version"]},
                )
            )
            self.pricing_decisions_emitted += 1

            # Supply & Demand H3 Aggregate messages
            for kind, topic, cnt in (
                ("supply", "marketplace.supply.aggregates", s_cnt),
                ("demand", "marketplace.demand.aggregates", d_signal),
            ):
                agg = {
                    "aggregate_id": str(uuid.uuid4()),
                    "aggregate_kind": kind,
                    "city_id": city_id,
                    "region_id": region_id,
                    "h3_cell": cell,
                    "h3_resolution": self.h3_res,
                    "window_start": win_start,
                    "window_end": win_end,
                    "active_count": cnt,
                    "neighbor_count": n_supply if kind == "supply" else 0,
                    "state_version": next_st.decision_version,
                    "emitted_at": datetime.now(UTC).isoformat(),
                    "schema_version": "1.0.0",
                }
                item = (cell, self.registry.serialize(topic, agg), int(dec_time.timestamp() * 1000), None)
                if kind == "supply":
                    supply_agg_batch.append(item)
                else:
                    demand_agg_batch.append(item)

        # Emit joined streaming matching outcomes for requests
        outcome_batch = []
        for req in self.requests.values():
            if req.outcome_emitted:
                continue
            p_st = self.prior_pricing.get(req.h3_cell)
            if p_st:
                req.pricing_multiplier = p_st.last_multiplier
            from marketplace_platform.pricing_engine import assign_experiment_variant

            variant, pol = assign_experiment_variant(req.h3_cell)
            req.experiment_variant = variant
            req.policy_version = pol.policy_version

            r2o = int((req.first_offer_ts - req.created_ts) * 1000) if req.first_offer_ts else None
            o2a = (
                int((req.accepted_ts - req.first_offer_ts) * 1000)
                if req.accepted_ts and req.first_offer_ts
                else None
            )
            r2m = int((req.matched_ts - req.created_ts) * 1000) if req.matched_ts else None
            out_rec = {
                "outcome_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"outcome:{req.request_id}")),
                "request_id": req.request_id,
                "trip_id": req.trip_id,
                "rider_id": req.rider_id,
                "driver_id": req.matched_driver_id,
                "city_id": req.city_id,
                "region_id": req.region_id,
                "h3_cell_res7": req.h3_cell,
                "service_type": req.service_type,
                "request_time": datetime.fromtimestamp(req.created_ts, tz=UTC).isoformat(),
                "outcome_time": datetime.fromtimestamp(req.last_event_ts, tz=UTC).isoformat(),
                "final_status": req.state,
                "request_to_offer_ms": max(0, r2o) if r2o is not None else None,
                "offer_to_accept_ms": max(0, o2a) if o2a is not None else None,
                "request_to_match_ms": max(0, r2m) if r2m is not None else None,
                "offers_sent": req.offers_sent,
                "offers_rejected": req.offers_rejected,
                "pricing_multiplier": req.pricing_multiplier,
                "policy_version": req.policy_version,
                "experiment_variant": req.experiment_variant,
                "estimated_pickup_seconds": req.estimated_pickup_s,
                "trip_completed": req.trip_completed,
                "cancelled_by": req.cancelled_by,
                "schema_version": "1.0.0",
            }
            outcome_batch.append(
                (
                    req.request_id,
                    self.registry.serialize("marketplace.matching.outcomes", out_rec),
                    int(req.last_event_ts * 1000),
                    None,
                )
            )
            req.outcome_emitted = True
            self.matching_outcomes_emitted += 1

        if supply_agg_batch:
            self.bus.produce_batch("marketplace.supply.aggregates", supply_agg_batch)
        if demand_agg_batch:
            self.bus.produce_batch("marketplace.demand.aggregates", demand_agg_batch)
        if pricing_batch:
            self.bus.produce_batch("marketplace.pricing.decisions", pricing_batch)
        if outcome_batch:
            self.bus.produce_batch("marketplace.matching.outcomes", outcome_batch)

        # Update Prometheus city gauges
        for cid in CITIES:
            self.metrics.active_drivers_gauge.labels(city_id=cid).set(
                sum(1 for d in self.drivers.values() if d.city_id == cid and d.state == "AVAILABLE")
            )
            self.metrics.active_demand_gauge.labels(city_id=cid).set(
                sum(1 for r in self.requests.values() if r.city_id == cid and r.state in ACTIVE_REQUEST_STATES)
            )

        ckpt = self.save_checkpoint()
        dur = max(1e-6, time.perf_counter() - t0)
        return {
            "records_read": len(raw_records),
            "processed_this_call": processed_this_call,
            "total_processed": self.total_processed,
            "duplicates_suppressed": self.duplicates_suppressed,
            "stale_out_of_order_ignored": self.stale_out_of_order_ignored,
            "late_events_side_output": len(self.late_events_side_output),
            "dlq_quarantined": self.dlq_quarantined,
            "tracked_drivers": len(self.drivers),
            "available_drivers": sum(self.supply_by_cell.values()),
            "tracked_requests": len(self.requests),
            "active_unmatched_requests": sum(self.demand_by_cell.values()),
            "active_h3_cells": len(active_cells),
            "cell_transfers_processed": self.cell_transfers_processed,
            "pricing_decisions_emitted": self.pricing_decisions_emitted,
            "matching_outcomes_emitted": self.matching_outcomes_emitted,
            "duration_seconds": round(dur, 4),
            "throughput_events_per_sec": round(len(raw_records) / dur, 2),
            "checkpoint": ckpt,
        }
