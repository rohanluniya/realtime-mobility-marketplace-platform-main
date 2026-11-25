# Explainable Dynamic Pricing Engine & Cluster Experimentation

Implemented in [`marketplace_platform/pricing_engine.py`](../marketplace_platform/pricing_engine.py).

## Effective Supply & Explainable Multiplier Formula

For each active H3 resolution-7 cell $c$ under policy $P$:

1. **Neighbor-Aware Effective Supply:**
   $$\text{effective\_supply}(c) = \max\left(1.0,\; \text{supply}(c) + w_{\text{nb}} \cdot \sum_{n \in \text{ring}_1(c)} \text{supply}(n)\right)$$
   Including 1-ring neighboring drivers (`6` adjacent hexagons) prevents false surge spikes when available cars sit 200 meters across an H3 boundary.
2. **Additive Transparent Components around `1.00x` Baseline:**
   - **Demand/Supply Imbalance:** If $\text{demand}(c) / \text{effective\_supply}(c) > 1.2$, adds $\min(1.35, (\text{ratio} - 1.2) \times 0.42)$ (`HIGH_DEMAND_SUPPLY_RATIO`).
   - **Low Neighbor Supply:** If $\text{neighbor\_supply} \le 2$ and $\text{demand} \ge 3$, adds `+0.15` (`LOW_NEIGHBOR_SUPPLY`).
   - **Demand Velocity Acceleration:** If growth rate $> 0.25$, adds up to `+0.25` (`DEMAND_GROWTH_ACCELERATION`).
   - **Elevated Pickup Wait Time:** If estimated wait $> 300\text{s}$, adds up to `+0.30` (`ELEVATED_WAIT_TIME`).
   - **Low Match Liquidity:** If recent match rate $< 65\%$ and $\text{demand} \ge 2$, adds `+0.15` (`LOW_MATCH_LIQUIDITY`).
   - **Localized Regional Surge / Traffic Congestion:** Adds `+0.20` (`REGIONAL_SURGE_EVENT`) and `+0.10` (`CONGESTION_TRAFFIC_FACTOR`) only when the cell experiences active demand pressure.

## Six Mandatory Production Guardrails

| Guardrail | Trigger Condition | Action & Audit Reason Code |
| --- | --- | --- |
| 1. Missing Signal Fallback | `demand_count is None` or `supply_count is None` | Emit `1.00x`, `FALLBACK_MISSING` (`MISSING_DATA_FALLBACK`) |
| 2. Stale Signal Fallback | `input_freshness_seconds > max_freshness_seconds` | Emit `1.00x`, `FALLBACK_STALE` (`STALE_DATA_FALLBACK`) |
| 3. Exponential Smoothing | Prior multiplier exists | $\alpha \cdot \text{raw} + (1 - \alpha) \cdot \text{prev}$ (`SMOOTHED_DAMPING`) |
| 4. Min Interval & Hysteresis | Elapsed $< \text{min\_interval}$ OR $|\Delta| < \text{deadband}$ | Hold prior multiplier (`MIN_INTERVAL_HOLD` / `HYSTERESIS_HOLD`) |
| 5. Step-Delta Limiter | $|\text{smoothed} - \text{prev}| > \text{max\_step\_delta}$ | Clamp single-step change (`STEP_DELTA_CLAMPED`) |
| 6. Policy Floor & Ceiling | Multiplier $< 1.00$ or $> \text{max\_multiplier}$ | Clamp to policy bounds (`POLICY_MIN_FLOOR_CLAMPED` / `POLICY_MAX_CAP_CLAMPED`) |

## Deterministic Replay & H3-Cluster A/B Experimentation

- **Deterministic Replay:** Every decision generates a UUIDv5 `decision_id` from `(city_id, h3_cell, input_window, policy_version, decision_version)`. Replaying identical inputs produces byte-for-byte identical `decision_id` and `pricing_multiplier` values (`test_pricing_stale_and_missing_safe_fallbacks_and_determinism`).
- **Cluster Randomization:** Experiment `exp_neighbor_smoothed_pricing_2026` hashes `(experiment_id, h3_cell)` via SHA-256 to assign entire H3 cells to `control` (`policy-v1.0`, cap `3.00x`, `alpha=0.55`, `neighbor_weight=0.15`) vs. `treatment` (`policy-v2.0-smoothed`, cap `2.60x`, `alpha=0.35`, `neighbor_weight=0.35`).
- **Sample Ratio Mismatch (SRM) Gate:** Every Gold curation executes a 1-df chi-squared goodness-of-fit test (`chi2 > 6.635` triggers `FAIL_SRM` and halts the Airflow experiment DAG).
