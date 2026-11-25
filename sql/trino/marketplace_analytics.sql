-- =============================================================================
-- Trino 484 Analytical SQL — Real-Time Mobility Marketplace Lakehouse (Iceberg V2)
-- Catalog: `iceberg` (backed by Lakekeeper v0.15.1 REST Catalog + RustFS 1.0.6 S3)
-- =============================================================================

-- 1. City-level marketplace liquidity & dynamic pricing executive summary
SELECT
    city_id,
    region_id,
    active_h3_cells,
    total_requests,
    matched_requests,
    completed_trips,
    available_drivers,
    ROUND(match_rate_pct, 2) AS match_rate_pct,
    ROUND(completion_rate_pct, 2) AS completion_rate_pct,
    ROUND(avg_pricing_multiplier, 3) AS avg_pricing_multiplier,
    ROUND(max_pricing_multiplier, 2) AS max_pricing_multiplier,
    ROUND(liquidity_score, 2) AS liquidity_score
FROM iceberg.gold.city_supply_demand_summary
ORDER BY liquidity_score DESC;

-- 2. Top 20 highest-imbalance H3 resolution-7 surge hotspots with neighbor supply context
SELECT
    city_id,
    h3_cell,
    h3_coarse_res5,
    demand_count,
    supply_count,
    neighbor_supply_count,
    ROUND(imbalance_ratio, 2) AS imbalance_ratio,
    ROUND(pricing_multiplier, 2) AS pricing_multiplier,
    ROUND(estimated_wait_seconds, 1) AS est_wait_sec,
    policy_version,
    experiment_variant,
    reason_codes_csv
FROM iceberg.gold.h3_cell_marketplace_state
WHERE pricing_multiplier > 1.25
ORDER BY pricing_multiplier DESC, imbalance_ratio DESC
LIMIT 20;

-- 3. Multi-resolution H3 rollup: Resolution 7 -> Resolution 5 metro district heat summary
SELECT
    city_id,
    h3_coarse_res5 AS district_h3_res5,
    COUNT(*) AS active_res7_cells,
    SUM(demand_count) AS district_demand,
    SUM(supply_count) AS district_supply,
    ROUND(AVG(pricing_multiplier), 3) AS avg_district_multiplier,
    ROUND(MAX(pricing_multiplier), 2) AS peak_district_multiplier
FROM iceberg.gold.h3_cell_marketplace_state
GROUP BY city_id, h3_coarse_res5
ORDER BY district_demand DESC
LIMIT 25;

-- 4. Matching conversion funnel by city and service tier
SELECT
    city_id,
    service_type,
    requests_total,
    requests_offered,
    requests_matched,
    trips_completed,
    rider_cancellations,
    driver_cancellations,
    unfulfilled_requests,
    ROUND(driver_acceptance_rate_pct, 2) AS driver_acceptance_pct,
    ROUND(match_success_rate_pct, 2) AS match_success_pct,
    ROUND(avg_request_to_offer_ms, 1) AS avg_req_to_offer_ms,
    ROUND(avg_request_to_match_ms, 1) AS avg_req_to_match_ms,
    ROUND(avg_estimated_pickup_s, 1) AS avg_pickup_eta_s
FROM iceberg.gold.matching_conversion_funnel
ORDER BY city_id, service_type;

-- 5. Pricing experiment A/B analysis with Sample Ratio Mismatch (SRM) validation
SELECT
    experiment_id,
    experiment_variant,
    policy_version,
    cells_exposed,
    requests_exposed,
    ROUND(match_rate_pct, 2) AS match_rate_pct,
    ROUND(completion_rate_pct, 2) AS completion_rate_pct,
    ROUND(cancellation_rate_pct, 2) AS cancellation_rate_pct,
    ROUND(avg_pricing_multiplier, 3) AS avg_pricing_multiplier,
    ROUND(avg_request_to_match_ms, 1) AS avg_match_latency_ms,
    srm_chi2_stat,
    srm_status
FROM iceberg.gold.pricing_experiment_results
ORDER BY experiment_id, experiment_variant;

-- 6. Pricing policy audit: join pricing decisions with CDC SCD Type 2 policy dimensions
SELECT
    f.city_id,
    f.policy_version,
    COUNT(*) AS decisions,
    MIN(f.pricing_multiplier) AS min_applied,
    MAX(f.pricing_multiplier) AS max_applied,
    MAX(d.max_multiplier) AS policy_cap,
    SUM(CASE WHEN f.pricing_multiplier > d.max_multiplier OR f.pricing_multiplier < d.min_multiplier THEN 1 ELSE 0 END) AS cap_violations
FROM iceberg.silver.fact_pricing_decision AS f
JOIN iceberg.silver.dim_pricing_policy_scd2 AS d
    ON f.region_id = d.region_id
    AND f.policy_version = d.policy_version
GROUP BY f.city_id, f.policy_version
ORDER BY f.city_id, f.policy_version;

-- 7. Apache Iceberg Time Travel & Snapshot Audit
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-records'] AS added_records,
    summary['total-records'] AS total_records
FROM iceberg.silver."fact_pricing_decision$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- Example point-in-time replay query (replace <SNAPSHOT_ID> with an ID from query 7):
-- SELECT city_id, COUNT(*), ROUND(AVG(pricing_multiplier), 3)
-- FROM iceberg.silver.fact_pricing_decision FOR VERSION AS OF <SNAPSHOT_ID>
-- GROUP BY city_id;
