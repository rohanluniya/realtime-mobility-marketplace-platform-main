# Lakehouse Dimensional & Medallion Data Model

The platform organizes **12 Apache Iceberg Format V2 tables** across four namespaces (`bronze`, `silver`, `gold`, `quality`), combining raw immutable event retention with a star schema and SCD Type 2 policy auditability.

## Namespace & Table Inventory

### Bronze Layer (Immutable Raw Append-Only Landing)
| Table | Partitioning | Grain / Key | Purpose |
| --- | --- | --- | --- |
| `bronze.raw_events` | `day(event_time)` | 1 row per validated Avro marketplace event (`event_id`) | Full replayable raw log across all 7 input topics |
| `bronze.pricing_decisions_raw` | `day(decision_time)` | 1 row per emitted H3 pricing decision (`decision_id`) | Immutable audit trail of raw dynamic pricing outputs |
| `bronze.matching_outcomes_raw` | `day(request_time)` | 1 row per correlated request outcome (`outcome_id`) | Raw joined request -> offer -> trip lifecycle outcomes |
| `bronze.dead_letter` | Unpartitioned | 1 row per quarantined payload (`dlq_id`) | Schema-invalid/poison payloads with base64 body & error code |

### Silver Layer (Deduplicated Facts & Conformed SCD Type 2 Dimensions)
| Table | Grain / Key | Modeling Pattern |
| --- | --- | --- |
| `silver.fact_pricing_decision` | `decision_id` (unique) | Deduplicated pricing facts enriched with `h3_coarse_res5`, policy version, experiment variant, and reason codes |
| `silver.fact_matching_outcome` | `request_id` (unique) | Conformed multi-entity join fact with `request_to_offer_ms`, `offer_to_accept_ms`, `request_to_match_ms`, pickup ETA, and cancellation attribution |
| `silver.dim_pricing_policy_scd2` | `policy_sk` (`region_id`, `policy_version`) | **SCD Type 2** table materialized from PostgreSQL/Debezium CDC capturing `effective_from`, `effective_to`, `is_current`, `cdc_lsn`, bounds, and smoothing weights |

### Gold Layer (Curated Business & Executive Analytical Marts)
| Table | Grain | Consumer Endpoints & Queries |
| --- | --- | --- |
| `gold.h3_cell_marketplace_state` | Latest snapshot per `h3_cell` | Serving-store reconciliation source of truth, Trino hotspot queries, H3 res-5 district rollups |
| `gold.city_supply_demand_summary` | `city_id` | `/marketplace/summary`, `/marketplace/regions`, liquidity score tracking |
| `gold.matching_conversion_funnel` | `(city_id, service_type)` | `/marketplace/matching`, `/marketplace/trips`, conversion & latency breakdown |
| `gold.pricing_experiment_results` | `(experiment_id, experiment_variant)` | `/marketplace/experiments`, A/B lift & chi-squared SRM validation |

### Quality Namespace
| Table | Grain | Purpose |
| --- | --- | --- |
| `quality.dq_check_results` | `(run_id, check_name)` | Historical ledger of all 8 automated invariant, uniqueness, policy-bound, and cross-layer reconciliation checks |
