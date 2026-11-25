"""Explainable, bounded, replayable dynamic pricing recommendation engine and
deterministic H3-cluster pricing experimentation framework (Sections 11 & 18).

Explicit disclaimer: This is a synthetic marketplace pricing model built for data engineering
demonstration, not a reproduction of any real mobility company's proprietary algorithm.
Protected personal attributes are structurally excluded from all pricing inputs.
"""

from __future__ import annotations

import hashlib
import math
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from marketplace_platform.observability import get_metrics

_PRICING_NS = uuid.UUID("44556677-8899-4abc-9def-112233445566")
MODEL_VERSION = "dyn-price-v2.1.0"
DEFAULT_EXPERIMENT_ID = "exp_neighbor_smoothed_pricing_2026"


@dataclass(frozen=True)
class PricingPolicy:
    policy_version: str = "policy-v1.0"
    baseline_multiplier: float = 1.0
    min_multiplier: float = 1.0
    max_multiplier: float = 3.0
    smoothing_alpha: float = 0.45
    hysteresis_deadband: float = 0.08
    min_update_interval_seconds: float = 30.0
    max_step_delta: float = 0.35
    max_freshness_seconds: float = 120.0
    neighbor_supply_weight: float = 0.25


POLICY_CATALOG: dict[str, PricingPolicy] = {
    "policy-v1.0": PricingPolicy(
        policy_version="policy-v1.0",
        max_multiplier=3.0,
        smoothing_alpha=0.55,
        neighbor_supply_weight=0.15,
    ),
    "policy-v2.0-smoothed": PricingPolicy(
        policy_version="policy-v2.0-smoothed",
        max_multiplier=2.6,
        smoothing_alpha=0.35,
        neighbor_supply_weight=0.35,
    ),
}


@dataclass
class PriorPricingState:
    last_multiplier: float = 1.0
    last_decision_ts: float = 0.0
    decision_version: int = 0


def assign_experiment_variant(
    h3_cell: str,
    experiment_id: str = DEFAULT_EXPERIMENT_ID,
) -> tuple[str, PricingPolicy]:
    """Deterministic cluster randomization by H3 cell (50/50 control vs. treatment).

    Cluster randomization at H3 cell / zone level reduces (though cannot fully eliminate)
    geographic spillover and driver-repositioning interference across adjacent cells.
    """
    digest = hashlib.sha256(f"{experiment_id}:{h3_cell}".encode()).hexdigest()
    bucket = int(digest[:8], 16) % 100
    if bucket < 50:
        return "control", POLICY_CATALOG["policy-v1.0"]
    return "treatment", POLICY_CATALOG["policy-v2.0-smoothed"]


def compute_chi_square_srm(control_n: int, treatment_n: int, expected_ratio: float = 0.5) -> dict[str, Any]:
    """Sample Ratio Mismatch (SRM) chi-squared goodness-of-fit test (1 df, p<0.01 -> chi2 > 6.635)."""
    total = control_n + treatment_n
    if total == 0:
        return {"total": 0, "chi2_stat": 0.0, "srm_detected": False, "status": "NO_DATA"}
    exp_c = total * expected_ratio
    exp_t = total * (1.0 - expected_ratio)
    chi2 = ((control_n - exp_c) ** 2) / max(1e-9, exp_c) + ((treatment_n - exp_t) ** 2) / max(1e-9, exp_t)
    # Approximate two-tailed p-value for 1 df via complementary error function
    p_val = math.erfc(math.sqrt(chi2 / 2.0))
    srm = chi2 > 6.635
    return {
        "total": total,
        "control_count": control_n,
        "treatment_count": treatment_n,
        "observed_treatment_share": round(treatment_n / total, 4),
        "chi2_stat": round(chi2, 4),
        "p_value": round(p_val, 5),
        "srm_detected": srm,
        "status": "FAIL_SRM" if srm else "PASS",
    }


def compute_pricing_decision(
    *,
    city_id: str,
    region_id: str,
    h3_cell: str,
    h3_resolution: int,
    decision_time: datetime,
    input_window: str,
    demand_count: int | None,
    supply_count: int | None,
    neighbor_supply_count: int,
    demand_growth_rate: float,
    recent_match_rate: float,
    estimated_wait_seconds: float,
    traffic_factor: float,
    regional_event_active: bool,
    input_freshness_seconds: float,
    prior: PriorPricingState | None = None,
    policy_override: PricingPolicy | None = None,
    experiment_id: str = DEFAULT_EXPERIMENT_ID,
    trace_id: str = "trace-pricing",
    correlation_id: str = "corr-pricing",
) -> tuple[dict[str, Any], PriorPricingState]:
    """Deterministically compute an explainable, bounded, smoothed pricing recommendation."""
    t0 = time.perf_counter()
    metrics = get_metrics()
    prev = prior or PriorPricingState()
    variant, exp_policy = assign_experiment_variant(h3_cell, experiment_id)
    policy = policy_override or exp_policy
    dec_ts = decision_time.timestamp()
    reason_codes: list[str] = []
    status = "ACTIVE"

    # Guardrail 1: Missing input fallback
    if demand_count is None or supply_count is None:
        raw_mult = policy.baseline_multiplier
        final_mult = policy.baseline_multiplier
        reason_codes.append("MISSING_DATA_FALLBACK")
        status = "FALLBACK_MISSING"
        d_cnt = demand_count or 0
        s_cnt = supply_count or 0
    # Guardrail 2: Stale signal fallback
    elif input_freshness_seconds > policy.max_freshness_seconds:
        raw_mult = policy.baseline_multiplier
        final_mult = policy.baseline_multiplier
        reason_codes.append("STALE_DATA_FALLBACK")
        status = "FALLBACK_STALE"
        d_cnt = demand_count
        s_cnt = supply_count
    else:
        d_cnt = max(0, int(demand_count))
        s_cnt = max(0, int(supply_count))
        effective_supply = max(1.0, float(s_cnt) + policy.neighbor_supply_weight * max(0, neighbor_supply_count))
        imbalance_ratio = d_cnt / effective_supply

        # Explainable additive components around baseline 1.0x
        mult = policy.baseline_multiplier
        if imbalance_ratio > 1.2:
            mult += min(1.35, (imbalance_ratio - 1.2) * 0.42)
            reason_codes.append("HIGH_DEMAND_SUPPLY_RATIO")
        elif imbalance_ratio < 0.7:
            reason_codes.append("BALANCED_OR_SURPLUS_SUPPLY")

        if neighbor_supply_count <= 2 and d_cnt >= 3:
            mult += 0.15
            reason_codes.append("LOW_NEIGHBOR_SUPPLY")

        if demand_growth_rate > 0.25:
            mult += min(0.25, demand_growth_rate * 0.30)
            reason_codes.append("DEMAND_GROWTH_ACCELERATION")

        if estimated_wait_seconds > 300.0:
            mult += min(0.30, ((estimated_wait_seconds - 300.0) / 300.0) * 0.20)
            reason_codes.append("ELEVATED_WAIT_TIME")

        if recent_match_rate < 0.65 and d_cnt >= 2:
            mult += 0.15
            reason_codes.append("LOW_MATCH_LIQUIDITY")

        if regional_event_active:
            mult += 0.20
            reason_codes.append("REGIONAL_SURGE_EVENT")

        if traffic_factor > 1.25:
            mult += 0.10
            reason_codes.append("CONGESTION_TRAFFIC_FACTOR")

        raw_mult = round(mult, 4)

        # Guardrail 3: Exponential smoothing against prior cell multiplier
        smoothed = (
            raw_mult
            if prev.decision_version == 0
            else (policy.smoothing_alpha * raw_mult) + ((1.0 - policy.smoothing_alpha) * prev.last_multiplier)
        )
        if prev.decision_version > 0 and abs(smoothed - raw_mult) >= 0.03:
            reason_codes.append("SMOOTHED_DAMPING")

        # Guardrail 4: Minimum update interval & Hysteresis deadband
        elapsed_since_last = dec_ts - prev.last_decision_ts
        if prev.decision_version > 0 and elapsed_since_last < policy.min_update_interval_seconds:
            final_mult = prev.last_multiplier
            reason_codes.append("MIN_INTERVAL_HOLD")
        elif prev.decision_version > 0 and abs(smoothed - prev.last_multiplier) < policy.hysteresis_deadband:
            final_mult = prev.last_multiplier
            reason_codes.append("HYSTERESIS_HOLD")
        else:
            # Guardrail 5: Max step delta per update
            delta = smoothed - prev.last_multiplier
            if prev.decision_version > 0 and abs(delta) > policy.max_step_delta:
                smoothed = prev.last_multiplier + math.copysign(policy.max_step_delta, delta)
                reason_codes.append("STEP_DELTA_CLAMPED")
            final_mult = smoothed

        # Guardrail 6: Policy floor/ceiling bounds
        if final_mult > policy.max_multiplier:
            final_mult = policy.max_multiplier
            reason_codes.append("POLICY_MAX_CAP_CLAMPED")
        elif final_mult < policy.min_multiplier:
            final_mult = policy.min_multiplier
            reason_codes.append("POLICY_MIN_FLOOR_CLAMPED")

        final_mult = round(final_mult, 2)
        if not reason_codes:
            reason_codes.append("BASELINE_EQUILIBRIUM")

    next_version = prev.decision_version + 1
    det_key = f"{city_id}:{h3_cell}:{input_window}:{policy.policy_version}:{next_version}"
    decision_id = str(uuid.uuid5(_PRICING_NS, det_key))
    dec_iso = decision_time.astimezone(UTC).isoformat()

    record: dict[str, Any] = {
        "decision_id": decision_id,
        "decision_version": next_version,
        "region_id": region_id,
        "city_id": city_id,
        "h3_cell": h3_cell,
        "h3_resolution": h3_resolution,
        "decision_time": dec_iso,
        "effective_time": dec_iso,
        "input_window": input_window,
        "demand_count": d_cnt,
        "supply_count": s_cnt,
        "neighbor_supply_count": max(0, int(neighbor_supply_count)),
        "demand_growth_rate": round(float(demand_growth_rate), 4),
        "recent_match_rate": round(float(recent_match_rate), 4),
        "estimated_wait_seconds": round(float(estimated_wait_seconds), 1),
        "traffic_factor": round(float(traffic_factor), 3),
        "regional_event_active": bool(regional_event_active),
        "input_freshness_seconds": round(float(input_freshness_seconds), 2),
        "raw_multiplier": round(float(raw_mult), 3),
        "pricing_multiplier": round(float(final_mult), 2),
        "policy_version": policy.policy_version,
        "model_version": MODEL_VERSION,
        "experiment_id": experiment_id,
        "experiment_variant": variant,
        "reason_codes": sorted(set(reason_codes)),
        "decision_status": status,
        "correlation_id": correlation_id,
        "trace_id": trace_id,
        "schema_version": "1.0.0",
    }

    next_state = PriorPricingState(
        last_multiplier=final_mult,
        last_decision_ts=dec_ts,
        decision_version=next_version,
    )
    metrics.pricing_decisions_total.labels(
        policy_version=policy.policy_version, decision_status=status
    ).inc()
    metrics.pricing_decision_latency_seconds.observe(time.perf_counter() - t0)
    return record, next_state
