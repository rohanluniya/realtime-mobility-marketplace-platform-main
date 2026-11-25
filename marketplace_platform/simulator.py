"""Deterministic multi-city synthetic mobility marketplace simulator.

Explicitly distinguishes virtual participant population (up to 1,000,000+ identities)
from concurrently active drivers and riders. Simulates all rider, driver, trip,
dispatch offer, and regional marketplace events plus realistic movement across H3 cells,
hotspots (airports, stadiums, rush hour, nightlife), cancellations, duplicates,
out-of-order driver location updates, late arrivals, and poison messages.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.geospatial import CITIES, estimate_eta_seconds, haversine_km, to_h3
from marketplace_platform.observability import get_logger, get_metrics, new_trace_context
from marketplace_platform.transport import EventBus

_NS = uuid.UUID("9c8b7a6d-5e4f-4321-8765-abcdef012345")
SERVICE_TYPES = ("standard", "comfort", "xl")

LOAD_PROFILES: dict[str, dict[str, Any]] = {
    "smoke": {
        "virtual_drivers": 100,
        "virtual_riders": 200,
        "active_drivers": 100,
        "active_riders": 200,
        "ticks": 6,
        "step_seconds": 20,
        "poison_count": 3,
    },
    "local": {
        "virtual_drivers": 1_000,
        "virtual_riders": 5_000,
        "active_drivers": 1_000,
        "active_riders": 2_000,
        "ticks": 5,
        "step_seconds": 20,
        "poison_count": 5,
    },
    "stress": {
        "virtual_drivers": 10_000,
        "virtual_riders": 50_000,
        "active_drivers": 5_000,
        "active_riders": 10_000,
        "ticks": 3,
        "step_seconds": 20,
        "poison_count": 10,
    },
    "virtual-scale": {
        "virtual_drivers": 200_000,
        "virtual_riders": 800_000,
        "active_drivers": 1_500,
        "active_riders": 3_000,
        "ticks": 4,
        "step_seconds": 20,
        "poison_count": 5,
    },
}


@dataclass
class SimConfig:
    virtual_drivers: int = 100
    virtual_riders: int = 200
    active_drivers: int = 100
    active_riders: int = 200
    ticks: int = 6
    step_seconds: int = 20
    seed: int = 2026
    start_time_utc: datetime = field(
        default_factory=lambda: datetime(2026, 11, 24, 17, 0, 0, tzinfo=UTC)
    )
    duplicate_rate: float = 0.02
    out_of_order_rate: float = 0.03
    hot_zone_concentration: float = 0.72
    poison_count: int = 3
    ground_truth_path: Path | None = None

    @classmethod
    def from_profile(cls, name: str, **overrides: Any) -> SimConfig:
        if name not in LOAD_PROFILES:
            raise ValueError(f"Unknown profile '{name}'. Choose from {sorted(LOAD_PROFILES)}")
        p = dict(LOAD_PROFILES[name])
        p.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**p)


def det_event_id(seed: int, domain: str, entity_id: str, seq: int) -> str:
    return str(uuid.uuid5(_NS, f"{seed}:{domain}:{entity_id}:{seq}"))


@dataclass
class _SimDriver:
    driver_id: str
    city_id: str
    region_id: str
    lat: float
    lon: float
    state: str = "AVAILABLE"
    seq: int = 0
    offline_ticks: int = 0


class MarketplaceSimulator:
    """Deterministic multi-city ride-hailing marketplace event simulator."""

    def __init__(
        self,
        config: MarketplaceConfig,
        bus: EventBus,
        registry: AvroContractRegistry,
        sim_cfg: SimConfig,
    ) -> None:
        self.config = config
        self.bus = bus
        self.registry = registry
        self.sim_cfg = sim_cfg
        self.rng = random.Random(sim_cfg.seed)
        self.logger = get_logger("marketplace-simulator", "generator", environment=config.env)
        self.metrics = get_metrics()

    def _sample_coord(self, city_id: str, *, surge_bias: bool = False) -> tuple[float, float, str]:
        city = CITIES[city_id]
        if self.rng.random() < (0.86 if surge_bias else self.sim_cfg.hot_zone_concentration):
            weights = [z.demand_weight for z in city.hot_zones]
            hz = self.rng.choices(city.hot_zones, weights=weights, k=1)[0]
            lat = hz.lat + self.rng.gauss(0.0, hz.radius_deg * 0.45)
            lon = hz.lon + self.rng.gauss(0.0, hz.radius_deg * 0.45)
            return round(lat, 6), round(lon, 6), hz.zone_id
        min_lat, min_lon, max_lat, max_lon = city.bbox
        return (
            round(self.rng.uniform(min_lat, max_lat), 6),
            round(self.rng.uniform(min_lon, max_lon), 6),
            "suburban_grid",
        )

    def _pack(
        self,
        *,
        topic: str,
        key: str,
        event_id: str,
        event_type: str,
        event_time: datetime,
        producer_time: datetime,
        city_id: str,
        region_id: str,
        lat: float,
        lon: float,
        trace_id: str,
        corr_id: str,
        rider_id: str | None = None,
        driver_id: str | None = None,
        request_id: str | None = None,
        trip_id: str | None = None,
        seq: int | None = None,
        payload: dict[str, Any] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> tuple[str, bytes, int, dict[str, str]]:
        rec = {
            "event_id": event_id,
            "event_type": event_type,
            "event_time": event_time.isoformat(),
            "producer_time": producer_time.isoformat(),
            "ingestion_time": (
                producer_time + timedelta(milliseconds=self.rng.randint(20, 180))
            ).isoformat(),
            "schema_version": "1.0.0",
            "rider_id": rider_id,
            "driver_id": driver_id,
            "request_id": request_id,
            "trip_id": trip_id,
            "region_id": region_id,
            "city_id": city_id,
            "latitude": lat,
            "longitude": lon,
            "correlation_id": corr_id,
            "trace_id": trace_id,
            "sequence_number": seq,
            "payload_json": json.dumps(payload or {}, separators=(",", ":")),
            "metadata": metadata or {"synthetic": "true", "seed": str(self.sim_cfg.seed)},
        }
        raw = self.registry.serialize(topic, rec)
        return (key, raw, int(event_time.timestamp() * 1000), {"event_type": event_type})

    def run(self) -> dict[str, Any]:
        t0 = time.perf_counter()
        cfg = self.sim_cfg
        trace_id, corr_id = new_trace_context()
        run_id = f"mkt-sim-{cfg.seed}-{int(time.time())}"
        self.bus.ensure_topics()

        city_ids = list(CITIES.keys())
        # Select active driver IDs deterministically out of virtual_drivers population
        if cfg.active_drivers == cfg.virtual_drivers:
            drv_indices = list(range(1, cfg.active_drivers + 1))
        else:
            drv_indices = [1, *sorted(self.rng.sample(range(2, cfg.virtual_drivers + 1), cfg.active_drivers - 1))]

        drivers: list[_SimDriver] = []
        avail_batch: list[tuple[str, bytes, int, dict[str, str]]] = []
        loc_batch: list[tuple[str, bytes, int, dict[str, str]]] = []
        req_batch: list[tuple[str, bytes, int, dict[str, str]]] = []
        offer_batch: list[tuple[str, bytes, int, dict[str, str]]] = []
        trip_batch: list[tuple[str, bytes, int, dict[str, str]]] = []
        reg_batch: list[tuple[str, bytes, int, dict[str, str]]] = []
        policy_batch: list[tuple[str, bytes, int, dict[str, str]]] = []

        counts: dict[str, int] = {
            "rider_events": 0,
            "driver_events": 0,
            "trip_events": 0,
            "dispatch_events": 0,
            "regional_events": 0,
            "duplicates_injected": 0,
            "out_of_order_injected": 0,
            "poison_injected": 0,
            "bytes_produced": 0,
        }

        # Emit initial pricing policy updates + regional surge events (airport/stadium rush)
        for idx, cid in enumerate(city_ids):
            city = CITIES[cid]
            ev_t = cfg.start_time_utc
            policy_batch.append(
                self._pack(
                    topic="marketplace.pricing.policies",
                    key=city.region_id,
                    event_id=det_event_id(cfg.seed, "policy", cid, 1),
                    event_type="pricing_policy_updated",
                    event_time=ev_t,
                    producer_time=ev_t,
                    city_id=cid,
                    region_id=city.region_id,
                    lat=city.center_lat,
                    lon=city.center_lon,
                    trace_id=trace_id,
                    corr_id=corr_id,
                    seq=1,
                    payload={"policy_version": "policy-v1.0", "max_multiplier": 3.0, "min_multiplier": 1.0},
                )
            )
            # Stadium / airport regional surge event
            hz = city.hot_zones[1]  # airport
            reg_batch.append(
                self._pack(
                    topic="marketplace.regional.events",
                    key=city.region_id,
                    event_id=det_event_id(cfg.seed, "regional", cid, idx * 10 + 1),
                    event_type="regional_event_started",
                    event_time=ev_t,
                    producer_time=ev_t,
                    city_id=cid,
                    region_id=city.region_id,
                    lat=hz.lat,
                    lon=hz.lon,
                    trace_id=trace_id,
                    corr_id=corr_id,
                    payload={
                        "zone_id": hz.zone_id,
                        "event_name": f"{hz.name} Peak Arrival Wave",
                        "traffic_factor": 1.35,
                        "weather": "clear" if cid != "london" else "light_rain",
                    },
                )
            )
            reg_batch.append(
                self._pack(
                    topic="marketplace.regional.events",
                    key=city.region_id,
                    event_id=det_event_id(cfg.seed, "regional", cid, idx * 10 + 2),
                    event_type="demand_spike",
                    event_time=ev_t + timedelta(seconds=15),
                    producer_time=ev_t + timedelta(seconds=15),
                    city_id=cid,
                    region_id=city.region_id,
                    lat=city.hot_zones[2].lat,
                    lon=city.hot_zones[2].lon,
                    trace_id=trace_id,
                    corr_id=corr_id,
                    payload={"zone_id": city.hot_zones[2].zone_id, "severity": "HIGH", "traffic_factor": 1.45},
                )
            )
            counts["regional_events"] += 2

        # Initialize drivers online + available across cities
        for idx in drv_indices:
            did = f"DRV-{idx:06d}"
            cid = city_ids[idx % len(city_ids)]
            city = CITIES[cid]
            lat, lon, _ = self._sample_coord(cid, surge_bias=False)
            drv = _SimDriver(driver_id=did, city_id=cid, region_id=city.region_id, lat=lat, lon=lon)
            drivers.append(drv)
            drv.seq += 1
            avail_batch.append(
                self._pack(
                    topic="marketplace.driver.availability",
                    key=did,
                    event_id=det_event_id(cfg.seed, "drv", did, drv.seq),
                    event_type="driver_online",
                    event_time=cfg.start_time_utc,
                    producer_time=cfg.start_time_utc,
                    city_id=cid,
                    region_id=city.region_id,
                    lat=lat,
                    lon=lon,
                    driver_id=did,
                    trace_id=trace_id,
                    corr_id=corr_id,
                    seq=drv.seq,
                    payload={"target_state": "AVAILABLE", "service_type": SERVICE_TYPES[idx % 3]},
                )
            )
            counts["driver_events"] += 1

        req_counter = 0
        delayed_driver_locs: list[tuple[int, tuple[str, bytes, int, dict[str, str]]]] = []
        ground_truth: dict[str, int] = {
            "requests_created": 0,
            "requests_matched": 0,
            "requests_cancelled_rider": 0,
            "requests_unfulfilled": 0,
            "trips_completed": 0,
            "trips_cancelled_driver": 0,
            "driver_cell_transfers": 0,
        }

        requests_per_tick = max(10, cfg.active_riders // max(1, cfg.ticks))

        for tick in range(cfg.ticks):
            tick_t = cfg.start_time_utc + timedelta(seconds=(tick + 1) * cfg.step_seconds)

            # Flush out-of-order delayed driver location updates (older sequence numbers arriving late)
            keep_delayed = []
            for rel_tick, item in delayed_driver_locs:
                if rel_tick <= tick:
                    loc_batch.append(item)
                    counts["driver_events"] += 1
                else:
                    keep_delayed.append((rel_tick, item))
            delayed_driver_locs = keep_delayed

            # 1. Driver movement across H3 cells + occasional offline/connectivity gaps
            for drv in drivers:
                if drv.offline_ticks > 0:
                    drv.offline_ticks -= 1
                    if drv.offline_ticks == 0:
                        drv.state = "AVAILABLE"
                        drv.seq += 1
                        avail_batch.append(
                            self._pack(
                                topic="marketplace.driver.availability",
                                key=drv.driver_id,
                                event_id=det_event_id(cfg.seed, "drv", drv.driver_id, drv.seq),
                                event_type="driver_available",
                                event_time=tick_t,
                                producer_time=tick_t,
                                city_id=drv.city_id,
                                region_id=drv.region_id,
                                lat=drv.lat,
                                lon=drv.lon,
                                driver_id=drv.driver_id,
                                trace_id=trace_id,
                                corr_id=corr_id,
                                seq=drv.seq,
                                payload={"target_state": "AVAILABLE"},
                            )
                        )
                        counts["driver_events"] += 1
                    continue

                if drv.state == "AVAILABLE" and self.rng.random() < 0.03:
                    drv.state = "OFFLINE"
                    drv.offline_ticks = 2
                    drv.seq += 1
                    avail_batch.append(
                        self._pack(
                            topic="marketplace.driver.availability",
                            key=drv.driver_id,
                            event_id=det_event_id(cfg.seed, "drv", drv.driver_id, drv.seq),
                            event_type="driver_offline",
                            event_time=tick_t,
                            producer_time=tick_t,
                            city_id=drv.city_id,
                            region_id=drv.region_id,
                            lat=drv.lat,
                            lon=drv.lon,
                            driver_id=drv.driver_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=drv.seq,
                            payload={"target_state": "OFFLINE"},
                        )
                    )
                    counts["driver_events"] += 1
                    continue

                # Move driver (~0.5–2.2 km) often crossing H3 resolution-7 cell boundaries!
                old_cell = to_h3(drv.lat, drv.lon, 7)
                drv.lat = round(drv.lat + self.rng.uniform(-0.012, 0.012), 6)
                drv.lon = round(drv.lon + self.rng.uniform(-0.014, 0.014), 6)
                new_cell = to_h3(drv.lat, drv.lon, 7)
                if new_cell != old_cell:
                    ground_truth["driver_cell_transfers"] += 1

                drv.seq += 1
                loc_item = self._pack(
                    topic="marketplace.driver.locations",
                    key=drv.driver_id,
                    event_id=det_event_id(cfg.seed, "drvloc", drv.driver_id, drv.seq),
                    event_type="driver_location_updated",
                    event_time=tick_t,
                    producer_time=tick_t,
                    city_id=drv.city_id,
                    region_id=drv.region_id,
                    lat=drv.lat,
                    lon=drv.lon,
                    driver_id=drv.driver_id,
                    trace_id=trace_id,
                    corr_id=corr_id,
                    seq=drv.seq,
                    payload={"speed_kmh": round(self.rng.uniform(12.0, 42.0), 1)},
                )
                if tick < cfg.ticks - 1 and self.rng.random() < cfg.out_of_order_rate:
                    delayed_driver_locs.append((tick + 1, loc_item))
                    counts["out_of_order_injected"] += 1
                else:
                    loc_batch.append(loc_item)
                    counts["driver_events"] += 1
                    if self.rng.random() < cfg.duplicate_rate:
                        loc_batch.append(loc_item)
                        counts["driver_events"] += 1
                        counts["duplicates_injected"] += 1

            # 2. Rider requests, dispatch offers, accept/reject, cancellations, and trip lifecycles
            available_by_city: dict[str, list[_SimDriver]] = {cid: [] for cid in city_ids}
            for d in drivers:
                if d.state == "AVAILABLE":
                    available_by_city[d.city_id].append(d)

            for _ in range(requests_per_tick):
                req_counter += 1
                rider_idx = ((req_counter * 17) % cfg.virtual_riders) + 1
                rider_id = f"RDR-{rider_idx:07d}"
                request_id = f"REQ-{cfg.seed}-{req_counter:07d}"
                cid = city_ids[req_counter % len(city_ids)]
                city = CITIES[cid]
                plat, plon, zone_id = self._sample_coord(cid, surge_bias=True)
                dlat = round(plat + self.rng.uniform(-0.04, 0.04), 6)
                dlon = round(plon + self.rng.uniform(-0.04, 0.04), 6)
                svc_type = self.rng.choices(SERVICE_TYPES, weights=[0.64, 0.24, 0.12], k=1)[0]
                req_t = tick_t + timedelta(milliseconds=self.rng.randint(10, 4000))

                req_item = self._pack(
                    topic="marketplace.rider.requests",
                    key=request_id,
                    event_id=det_event_id(cfg.seed, "req", request_id, 1),
                    event_type="ride_requested",
                    event_time=req_t,
                    producer_time=req_t,
                    city_id=cid,
                    region_id=city.region_id,
                    lat=plat,
                    lon=plon,
                    rider_id=rider_id,
                    request_id=request_id,
                    trace_id=trace_id,
                    corr_id=corr_id,
                    seq=1,
                    payload={
                        "service_type": svc_type,
                        "dropoff_lat": dlat,
                        "dropoff_lon": dlon,
                        "zone_id": zone_id,
                        "target_state": "SEARCHING",
                    },
                )
                req_batch.append(req_item)
                counts["rider_events"] += 1
                ground_truth["requests_created"] += 1
                if self.rng.random() < cfg.duplicate_rate:
                    req_batch.append(req_item)
                    counts["rider_events"] += 1
                    counts["duplicates_injected"] += 1

                # Rider cancels before matching (~11%)
                if self.rng.random() < 0.11:
                    cancel_t = req_t + timedelta(seconds=self.rng.randint(8, 35))
                    req_batch.append(
                        self._pack(
                            topic="marketplace.rider.requests",
                            key=request_id,
                            event_id=det_event_id(cfg.seed, "req", request_id, 2),
                            event_type="ride_request_cancelled",
                            event_time=cancel_t,
                            producer_time=cancel_t,
                            city_id=cid,
                            region_id=city.region_id,
                            lat=plat,
                            lon=plon,
                            rider_id=rider_id,
                            request_id=request_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=2,
                            payload={"cancelled_by": "rider", "reason": "wait_too_long", "target_state": "CANCELLED"},
                        )
                    )
                    counts["rider_events"] += 1
                    ground_truth["requests_cancelled_rider"] += 1
                    continue

                # Attempt dispatch to an available driver in same city
                cand_pool = available_by_city[cid]
                if not cand_pool:
                    ground_truth["requests_unfulfilled"] += 1
                    continue

                # Sample up to 24 available drivers and sort nearest-first for realistic pickup ETAs
                sample_drvs = self.rng.sample(cand_pool, min(24, len(cand_pool)))
                sample_drvs.sort(key=lambda d: haversine_km(d.lat, d.lon, plat, plon))
                matched_drv: _SimDriver | None = None
                offer_t = req_t + timedelta(milliseconds=self.rng.randint(400, 2200))
                for offer_idx, cand in enumerate(sample_drvs[:2], 1):
                    dist_km = haversine_km(cand.lat, cand.lon, plat, plon)
                    eta_s = estimate_eta_seconds(dist_km, traffic_factor=1.2)
                    offer_batch.append(
                        self._pack(
                            topic="marketplace.dispatch.offers",
                            key=request_id,
                            event_id=det_event_id(cfg.seed, "off", request_id, offer_idx * 10 + 1),
                            event_type="driver_offer_received",
                            event_time=offer_t,
                            producer_time=offer_t,
                            city_id=cid,
                            region_id=city.region_id,
                            lat=plat,
                            lon=plon,
                            rider_id=rider_id,
                            driver_id=cand.driver_id,
                            request_id=request_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=offer_idx * 2 - 1,
                            payload={"pickup_distance_km": round(dist_km, 2), "eta_seconds": eta_s},
                        )
                    )
                    counts["dispatch_events"] += 1
                    resp_t = offer_t + timedelta(milliseconds=self.rng.randint(900, 6500))
                    # Acceptance probability decays with pickup distance
                    accept_prob = max(0.28, 0.88 - dist_km * 0.08)
                    if self.rng.random() < accept_prob:
                        offer_batch.append(
                            self._pack(
                                topic="marketplace.dispatch.offers",
                                key=request_id,
                                event_id=det_event_id(cfg.seed, "off", request_id, offer_idx * 10 + 2),
                                event_type="driver_offer_accepted",
                                event_time=resp_t,
                                producer_time=resp_t,
                                city_id=cid,
                                region_id=city.region_id,
                                lat=plat,
                                lon=plon,
                                rider_id=rider_id,
                                driver_id=cand.driver_id,
                                request_id=request_id,
                                trace_id=trace_id,
                                corr_id=corr_id,
                                seq=offer_idx * 2,
                                payload={"eta_seconds": eta_s},
                            )
                        )
                        counts["dispatch_events"] += 1
                        matched_drv = cand
                        offer_t = resp_t
                        break
                    offer_batch.append(
                        self._pack(
                            topic="marketplace.dispatch.offers",
                            key=request_id,
                            event_id=det_event_id(cfg.seed, "off", request_id, offer_idx * 10 + 3),
                            event_type="driver_offer_rejected",
                            event_time=resp_t,
                            producer_time=resp_t,
                            city_id=cid,
                            region_id=city.region_id,
                            lat=plat,
                            lon=plon,
                            rider_id=rider_id,
                            driver_id=cand.driver_id,
                            request_id=request_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=offer_idx * 2,
                            payload={"reason": "pickup_too_far"},
                        )
                    )
                    counts["dispatch_events"] += 1
                    offer_t = resp_t + timedelta(milliseconds=500)

                if matched_drv is None:
                    ground_truth["requests_unfulfilled"] += 1
                    continue

                # Matched -> emit trip lifecycle events & driver transitions
                trip_id = f"TRP-{cfg.seed}-{req_counter:07d}"
                ground_truth["requests_matched"] += 1
                trip_dist = max(0.8, haversine_km(plat, plon, dlat, dlon))
                base_fare = round(3.5 + trip_dist * 2.15, 2)

                # Driver reserved -> on trip
                matched_drv.seq += 1
                avail_batch.append(
                    self._pack(
                        topic="marketplace.driver.availability",
                        key=matched_drv.driver_id,
                        event_id=det_event_id(cfg.seed, "drv", matched_drv.driver_id, matched_drv.seq),
                        event_type="driver_reserved",
                        event_time=offer_t,
                        producer_time=offer_t,
                        city_id=cid,
                        region_id=city.region_id,
                        lat=matched_drv.lat,
                        lon=matched_drv.lon,
                        driver_id=matched_drv.driver_id,
                        request_id=request_id,
                        trip_id=trip_id,
                        trace_id=trace_id,
                        corr_id=corr_id,
                        seq=matched_drv.seq,
                        payload={"target_state": "RESERVED"},
                    )
                )
                counts["driver_events"] += 1

                trip_batch.append(
                    self._pack(
                        topic="marketplace.trip.lifecycle",
                        key=trip_id,
                        event_id=det_event_id(cfg.seed, "trp", trip_id, 1),
                        event_type="trip_matched",
                        event_time=offer_t,
                        producer_time=offer_t,
                        city_id=cid,
                        region_id=city.region_id,
                        lat=plat,
                        lon=plon,
                        rider_id=rider_id,
                        driver_id=matched_drv.driver_id,
                        request_id=request_id,
                        trip_id=trip_id,
                        trace_id=trace_id,
                        corr_id=corr_id,
                        seq=1,
                        payload={"service_type": svc_type, "base_fare_usd": base_fare, "distance_km": round(trip_dist, 2)},
                    )
                )
                counts["trip_events"] += 1

                # 6% driver cancellation after match; 94% complete trip and return driver to AVAILABLE
                if self.rng.random() < 0.06:
                    c_t = offer_t + timedelta(seconds=25)
                    trip_batch.append(
                        self._pack(
                            topic="marketplace.trip.lifecycle",
                            key=trip_id,
                            event_id=det_event_id(cfg.seed, "trp", trip_id, 2),
                            event_type="trip_cancelled",
                            event_time=c_t,
                            producer_time=c_t,
                            city_id=cid,
                            region_id=city.region_id,
                            lat=plat,
                            lon=plon,
                            rider_id=rider_id,
                            driver_id=matched_drv.driver_id,
                            request_id=request_id,
                            trip_id=trip_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=2,
                            payload={"cancelled_by": "driver", "reason": "traffic_gridlock"},
                        )
                    )
                    counts["trip_events"] += 1
                    ground_truth["trips_cancelled_driver"] += 1
                else:
                    start_t = offer_t + timedelta(seconds=self.rng.randint(90, 240))
                    end_t = start_t + timedelta(seconds=self.rng.randint(300, 1200))
                    trip_batch.append(
                        self._pack(
                            topic="marketplace.trip.lifecycle",
                            key=trip_id,
                            event_id=det_event_id(cfg.seed, "trp", trip_id, 2),
                            event_type="trip_started",
                            event_time=start_t,
                            producer_time=start_t,
                            city_id=cid,
                            region_id=city.region_id,
                            lat=plat,
                            lon=plon,
                            rider_id=rider_id,
                            driver_id=matched_drv.driver_id,
                            request_id=request_id,
                            trip_id=trip_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=2,
                            payload={"service_type": svc_type},
                        )
                    )
                    trip_batch.append(
                        self._pack(
                            topic="marketplace.trip.lifecycle",
                            key=trip_id,
                            event_id=det_event_id(cfg.seed, "trp", trip_id, 3),
                            event_type="trip_completed",
                            event_time=end_t,
                            producer_time=end_t,
                            city_id=cid,
                            region_id=city.region_id,
                            lat=dlat,
                            lon=dlon,
                            rider_id=rider_id,
                            driver_id=matched_drv.driver_id,
                            request_id=request_id,
                            trip_id=trip_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=3,
                            payload={
                                "service_type": svc_type,
                                "distance_km": round(trip_dist, 2),
                                "duration_seconds": int((end_t - start_t).total_seconds()),
                                "base_fare_usd": base_fare,
                            },
                        )
                    )
                    counts["trip_events"] += 2
                    ground_truth["trips_completed"] += 1
                    # Return driver to AVAILABLE at dropoff location
                    matched_drv.lat, matched_drv.lon = dlat, dlon
                    matched_drv.seq += 1
                    avail_batch.append(
                        self._pack(
                            topic="marketplace.driver.availability",
                            key=matched_drv.driver_id,
                            event_id=det_event_id(cfg.seed, "drv", matched_drv.driver_id, matched_drv.seq),
                            event_type="driver_available",
                            event_time=end_t + timedelta(seconds=5),
                            producer_time=end_t + timedelta(seconds=5),
                            city_id=cid,
                            region_id=city.region_id,
                            lat=dlat,
                            lon=dlon,
                            driver_id=matched_drv.driver_id,
                            trace_id=trace_id,
                            corr_id=corr_id,
                            seq=matched_drv.seq,
                            payload={"target_state": "AVAILABLE"},
                        )
                    )
                    counts["driver_events"] += 1

        # Flush remaining delayed out-of-order driver locations
        for _, item in delayed_driver_locs:
            loc_batch.append(item)
            counts["driver_events"] += 1

        # Inject poison payloads into rider requests to prove DLQ quarantine
        if cfg.poison_count > 0:
            now_ms = int(cfg.start_time_utc.timestamp() * 1000)
            for i in range(cfg.poison_count):
                req_batch.append(
                    (f"POISON-{i:03d}", b"\xffCORRUPT_MKT_AVRO_" + str(i).encode(), now_ms, {"poison": "true"})
                )
            counts["poison_injected"] = cfg.poison_count

        # Produce batches to respective Kafka/WAL topics
        topic_batches = [
            ("marketplace.pricing.policies", policy_batch),
            ("marketplace.regional.events", reg_batch),
            ("marketplace.driver.availability", avail_batch),
            ("marketplace.driver.locations", loc_batch),
            ("marketplace.rider.requests", req_batch),
            ("marketplace.dispatch.offers", offer_batch),
            ("marketplace.trip.lifecycle", trip_batch),
        ]
        total_bytes = 0
        total_events = 0
        for topic, batch in topic_batches:
            if batch:
                self.bus.produce_batch(topic, batch)
                total_events += len(batch)
                total_bytes += sum(len(item[1]) for item in batch)
                self.metrics.events_produced_total.labels(topic=topic, event_type="batch").inc(len(batch))

        counts["bytes_produced"] = total_bytes
        dur = max(1e-6, time.perf_counter() - t0)
        summary: dict[str, Any] = {
            "pipeline_run_id": run_id,
            "seed": cfg.seed,
            "virtual_participants": cfg.virtual_drivers + cfg.virtual_riders,
            "virtual_drivers": cfg.virtual_drivers,
            "virtual_riders": cfg.virtual_riders,
            "active_drivers": cfg.active_drivers,
            "active_riders": cfg.active_riders,
            "ticks": cfg.ticks,
            "transport_backend": self.bus.backend_name,
            "duration_seconds": round(dur, 4),
            "total_events_produced": total_events,
            "throughput_events_per_sec": round(total_events / dur, 2),
            "throughput_mb_per_sec": round(total_bytes / (1024**2 * dur), 2),
            "counts": counts,
            "ground_truth": ground_truth,
        }
        if cfg.ground_truth_path:
            cfg.ground_truth_path.parent.mkdir(parents=True, exist_ok=True)
            cfg.ground_truth_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        self.logger.info(
            "simulator.complete",
            f"Generated {total_events} marketplace events across {cfg.active_drivers} active drivers & {cfg.active_riders} active riders",
            trace_id=trace_id,
            correlation_id=corr_id,
            pipeline_run_id=run_id,
            context={"throughput_eps": summary["throughput_events_per_sec"], "ground_truth": ground_truth},
        )
        return summary
