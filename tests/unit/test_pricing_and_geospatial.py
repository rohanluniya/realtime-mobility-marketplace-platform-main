"""Unit tests for dynamic pricing guardrails, H3 geospatial primitives, SRM check, and secret scrubbing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from marketplace_platform.geospatial import (
    CITIES,
    estimate_eta_seconds,
    h3_center,
    h3_neighbors,
    h3_parent,
    haversine_km,
    to_h3,
    validate_coordinates,
)
from marketplace_platform.observability import scrub_secrets
from marketplace_platform.pricing_engine import (
    POLICY_CATALOG,
    PriorPricingState,
    assign_experiment_variant,
    compute_chi_square_srm,
    compute_pricing_decision,
)


def test_h3_multi_resolution_and_neighbors() -> None:
    nyc = CITIES["nyc"]
    fine = to_h3(nyc.center_lat, nyc.center_lon, 9)
    med = to_h3(nyc.center_lat, nyc.center_lon, 7)
    coarse = to_h3(nyc.center_lat, nyc.center_lon, 5)
    assert h3_parent(fine, 7) == med
    assert h3_parent(med, 5) == coarse

    neighbors = h3_neighbors(med, 1)
    assert len(neighbors) == 6
    assert med not in neighbors

    lat, lon = h3_center(med)
    assert validate_coordinates(lat, lon)
    assert not validate_coordinates(0.0, 0.0)
    assert not validate_coordinates(95.0, -73.9)


def test_haversine_and_eta() -> None:
    dist = haversine_km(40.7549, -73.9840, 40.6413, -73.7781)  # Midtown -> JFK
    assert 18.0 <= dist <= 24.0
    eta_normal = estimate_eta_seconds(dist, traffic_factor=1.0)
    eta_congested = estimate_eta_seconds(dist, traffic_factor=1.5)
    assert eta_congested > eta_normal


def test_pricing_bounds_cap_and_explainability() -> None:
    cell = to_h3(40.6413, -73.7781, 7)
    t0 = datetime(2026, 11, 24, 18, 0, 0, tzinfo=UTC)
    policy = POLICY_CATALOG["policy-v1.0"]
    dec, st = compute_pricing_decision(
        city_id="nyc",
        region_id="us_east",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0,
        input_window="2026-11-24T18:00:00Z/2026-11-24T18:01:00Z",
        demand_count=120,
        supply_count=1,
        neighbor_supply_count=0,
        demand_growth_rate=0.60,
        recent_match_rate=0.35,
        estimated_wait_seconds=900.0,
        traffic_factor=1.5,
        regional_event_active=True,
        input_freshness_seconds=3.0,
        policy_override=policy,
    )
    assert dec["pricing_multiplier"] == policy.max_multiplier
    assert "POLICY_MAX_CAP_CLAMPED" in dec["reason_codes"]
    assert "HIGH_DEMAND_SUPPLY_RATIO" in dec["reason_codes"]
    assert st.decision_version == 1


def test_pricing_smoothing_hysteresis_min_interval_and_step_cap() -> None:
    cell = to_h3(37.7850, -122.4010, 7)
    t0 = datetime(2026, 11, 24, 18, 0, 0, tzinfo=UTC)
    policy = POLICY_CATALOG["policy-v2.0-smoothed"]

    # Initial equilibrium decision
    dec1, st1 = compute_pricing_decision(
        city_id="sf",
        region_id="us_west",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0,
        input_window="w1",
        demand_count=4,
        supply_count=6,
        neighbor_supply_count=12,
        demand_growth_rate=0.05,
        recent_match_rate=0.88,
        estimated_wait_seconds=150.0,
        traffic_factor=1.0,
        regional_event_active=False,
        input_freshness_seconds=2.0,
        policy_override=policy,
    )
    assert dec1["pricing_multiplier"] == 1.0

    # Immediate spike within min_update_interval -> held at prior multiplier
    dec2, st2 = compute_pricing_decision(
        city_id="sf",
        region_id="us_west",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0 + timedelta(seconds=5),
        input_window="w2",
        demand_count=45,
        supply_count=2,
        neighbor_supply_count=2,
        demand_growth_rate=0.50,
        recent_match_rate=0.40,
        estimated_wait_seconds=600.0,
        traffic_factor=1.4,
        regional_event_active=True,
        input_freshness_seconds=2.0,
        prior=st1,
        policy_override=policy,
    )
    assert dec2["pricing_multiplier"] == dec1["pricing_multiplier"]
    assert "MIN_INTERVAL_HOLD" in dec2["reason_codes"]

    # After min interval passes -> step delta cap prevents sudden jump > max_step_delta
    dec3, _ = compute_pricing_decision(
        city_id="sf",
        region_id="us_west",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0 + timedelta(seconds=120),
        input_window="w3",
        demand_count=60,
        supply_count=1,
        neighbor_supply_count=0,
        demand_growth_rate=0.60,
        recent_match_rate=0.35,
        estimated_wait_seconds=700.0,
        traffic_factor=1.4,
        regional_event_active=True,
        input_freshness_seconds=2.0,
        prior=PriorPricingState(
            last_multiplier=1.0, last_decision_ts=t0.timestamp(), decision_version=1
        ),
        policy_override=policy,
    )
    assert dec3["pricing_multiplier"] <= round(1.0 + policy.max_step_delta + 1e-6, 2)
    assert "STEP_DELTA_CLAMPED" in dec3["reason_codes"]


def test_pricing_stale_and_missing_safe_fallbacks_and_determinism() -> None:
    cell = to_h3(51.5136, -0.0890, 7)
    t0 = datetime(2026, 11, 24, 18, 0, 0, tzinfo=UTC)

    stale_dec, _ = compute_pricing_decision(
        city_id="london",
        region_id="eu_west",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0,
        input_window="w_stale",
        demand_count=50,
        supply_count=1,
        neighbor_supply_count=0,
        demand_growth_rate=0.4,
        recent_match_rate=0.4,
        estimated_wait_seconds=600.0,
        traffic_factor=1.3,
        regional_event_active=True,
        input_freshness_seconds=400.0,
    )
    assert stale_dec["pricing_multiplier"] == 1.0
    assert stale_dec["decision_status"] == "FALLBACK_STALE"

    miss_dec, _ = compute_pricing_decision(
        city_id="london",
        region_id="eu_west",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0,
        input_window="w_miss",
        demand_count=None,
        supply_count=5,
        neighbor_supply_count=10,
        demand_growth_rate=0.0,
        recent_match_rate=0.8,
        estimated_wait_seconds=120.0,
        traffic_factor=1.0,
        regional_event_active=False,
        input_freshness_seconds=2.0,
    )
    assert miss_dec["pricing_multiplier"] == 1.0
    assert miss_dec["decision_status"] == "FALLBACK_MISSING"

    # Deterministic replay parity: identical inputs -> identical decision_id and multiplier
    d_a, _ = compute_pricing_decision(
        city_id="london",
        region_id="eu_west",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0,
        input_window="w_det",
        demand_count=18,
        supply_count=3,
        neighbor_supply_count=6,
        demand_growth_rate=0.3,
        recent_match_rate=0.6,
        estimated_wait_seconds=380.0,
        traffic_factor=1.2,
        regional_event_active=False,
        input_freshness_seconds=4.0,
    )
    d_b, _ = compute_pricing_decision(
        city_id="london",
        region_id="eu_west",
        h3_cell=cell,
        h3_resolution=7,
        decision_time=t0,
        input_window="w_det",
        demand_count=18,
        supply_count=3,
        neighbor_supply_count=6,
        demand_growth_rate=0.3,
        recent_match_rate=0.6,
        estimated_wait_seconds=380.0,
        traffic_factor=1.2,
        regional_event_active=False,
        input_freshness_seconds=4.0,
    )
    assert d_a["decision_id"] == d_b["decision_id"]
    assert d_a["pricing_multiplier"] == d_b["pricing_multiplier"]


def test_experiment_h3_assignment_and_srm_check() -> None:
    cell = to_h3(40.7549, -73.9840, 7)
    v1, _ = assign_experiment_variant(cell)
    v2, _ = assign_experiment_variant(cell)
    assert v1 == v2
    assert v1 in ("control", "treatment")

    ok_srm = compute_chi_square_srm(145, 155)
    assert ok_srm["status"] == "PASS"
    assert not ok_srm["srm_detected"]

    bad_srm = compute_chi_square_srm(220, 80)
    assert bad_srm["status"] == "FAIL_SRM"
    assert bad_srm["srm_detected"]


def test_secret_scrubbing() -> None:
    raw = {
        "api_key": "super-secret-value",
        "token": "Bearer abc123",
        "city_id": "nyc",
        "nested": {"password": "my-db-password", "demand": 12},
    }
    clean = scrub_secrets(raw)
    assert clean["api_key"] == "[REDACTED]"
    assert clean["token"] == "[REDACTED]"
    assert clean["nested"]["password"] == "[REDACTED]"
    assert clean["city_id"] == "nyc"
    assert clean["nested"]["demand"] == 12
