# ADR-005: Deterministic, Explainable, Bounded Dynamic Pricing with Cluster-Level A/B Testing

## Status
Accepted

## Context
Opaque black-box pricing models make customer disputes, regulatory audits, and incident postmortems difficult, while unsmoothed surge formulas cause rapid multiplier oscillation that erodes rider trust. Additionally, randomizing pricing experiments per rider inside the same H3 cell causes severe marketplace network interference (control and treatment riders compete for the exact same nearby drivers).

## Decision
1. Compute dynamic multipliers deterministically from versioned inputs (`demand_count`, `supply_count`, `neighbor_supply_count`, `demand_growth_rate`, `recent_match_rate`, `estimated_wait_seconds`, `traffic_factor`, `regional_event_active`) emitting explicit `reason_codes` on every decision.
2. Enforce six mandatory guardrails: missing-data `1.0x` fallback, stale-signal `1.0x` fallback, exponential smoothing (`alpha`), hysteresis deadband + minimum update interval, max step-delta clamping, and policy floor/ceiling bounds (`[1.0x, max_multiplier]`).
3. Randomize pricing experiments deterministically by **H3 resolution-7 cell cluster** (`SHA-256(experiment_id:h3_cell)`) and run an automated **Sample Ratio Mismatch (SRM)** chi-squared gate on every Gold curation run.

## Consequences
- Every pricing decision is 100% replayable and auditable by `decision_id`, `policy_version`, and `reason_codes`.
- Network interference between control and treatment cohorts is minimized compared to user-level randomization.
