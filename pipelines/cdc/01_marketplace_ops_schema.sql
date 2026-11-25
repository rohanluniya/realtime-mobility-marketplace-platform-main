-- =============================================================================
-- Operational Marketplace Metadata Database — PostgreSQL 17 + PostGIS 3.6
-- Source of truth for Debezium 3.7.1 Logical Replication CDC into Kafka & Iceberg
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS marketplace_ops;

-- 1. Cities & Regions (with PostGIS boundary polygons)
CREATE TABLE IF NOT EXISTS marketplace_ops.cities (
    city_id VARCHAR(32) PRIMARY KEY,
    region_id VARCHAR(32) NOT NULL,
    display_name VARCHAR(128) NOT NULL,
    timezone_name VARCHAR(64) NOT NULL,
    currency_code VARCHAR(8) NOT NULL,
    boundary GEOMETRY(Polygon, 4326),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Service Types & Fare Multipliers
CREATE TABLE IF NOT EXISTS marketplace_ops.service_types (
    service_type VARCHAR(32) PRIMARY KEY,
    display_name VARCHAR(64) NOT NULL,
    base_fare_usd NUMERIC(8,2) NOT NULL,
    per_km_usd NUMERIC(8,2) NOT NULL,
    per_minute_usd NUMERIC(8,2) NOT NULL,
    min_fare_usd NUMERIC(8,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Drivers & Vehicle Fleet Registry (PII-minimized synthetic metadata)
CREATE TABLE IF NOT EXISTS marketplace_ops.drivers (
    driver_id VARCHAR(64) PRIMARY KEY,
    city_id VARCHAR(32) NOT NULL REFERENCES marketplace_ops.cities(city_id),
    vehicle_class VARCHAR(32) NOT NULL,
    account_status VARCHAR(24) NOT NULL CHECK (account_status IN ('ACTIVE', 'SUSPENDED', 'OFFBOARDED')),
    rating_score NUMERIC(3,2) NOT NULL DEFAULT 4.90,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Dynamic Pricing Policy Rules (SCD Type 2 tracked in Lakehouse via Debezium CDC)
CREATE TABLE IF NOT EXISTS marketplace_ops.pricing_policies (
    policy_id SERIAL PRIMARY KEY,
    region_id VARCHAR(32) NOT NULL,
    policy_version VARCHAR(64) NOT NULL UNIQUE,
    min_multiplier NUMERIC(4,2) NOT NULL DEFAULT 1.00 CHECK (min_multiplier >= 1.00),
    max_multiplier NUMERIC(4,2) NOT NULL DEFAULT 3.00 CHECK (max_multiplier <= 5.00),
    smoothing_alpha NUMERIC(4,3) NOT NULL DEFAULT 0.450,
    hysteresis_deadband NUMERIC(4,3) NOT NULL DEFAULT 0.080,
    max_step_delta NUMERIC(4,2) NOT NULL DEFAULT 0.35,
    min_update_interval_seconds INTEGER NOT NULL DEFAULT 30,
    max_freshness_seconds INTEGER NOT NULL DEFAULT 90,
    neighbor_supply_weight NUMERIC(4,3) NOT NULL DEFAULT 0.350,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Regional Surge / Special Events (concerts, airport arrival waves, weather alerts)
CREATE TABLE IF NOT EXISTS marketplace_ops.regional_events (
    regional_event_id VARCHAR(64) PRIMARY KEY,
    city_id VARCHAR(32) NOT NULL REFERENCES marketplace_ops.cities(city_id),
    event_name VARCHAR(128) NOT NULL,
    event_category VARCHAR(32) NOT NULL,
    center_point GEOMETRY(Point, 4326) NOT NULL,
    radius_meters INTEGER NOT NULL,
    traffic_factor NUMERIC(4,2) NOT NULL DEFAULT 1.30,
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6. Pricing Experiment Configurations (H3 cluster-level randomization)
CREATE TABLE IF NOT EXISTS marketplace_ops.experiment_configs (
    experiment_id VARCHAR(96) PRIMARY KEY,
    description TEXT NOT NULL,
    randomization_unit VARCHAR(32) NOT NULL DEFAULT 'h3_res7_cell',
    control_policy_version VARCHAR(64) NOT NULL,
    treatment_policy_version VARCHAR(64) NOT NULL,
    treatment_allocation NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    status VARCHAR(24) NOT NULL DEFAULT 'RUNNING',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed reference data
INSERT INTO marketplace_ops.cities (city_id, region_id, display_name, timezone_name, currency_code) VALUES
    ('nyc', 'us_east', 'New York Metro', 'America/New_York', 'USD'),
    ('sf', 'us_west', 'San Francisco Bay', 'America/Los_Angeles', 'USD'),
    ('london', 'eu_west', 'Greater London', 'Europe/London', 'GBP')
ON CONFLICT (city_id) DO NOTHING;

INSERT INTO marketplace_ops.service_types (service_type, display_name, base_fare_usd, per_km_usd, per_minute_usd, min_fare_usd) VALUES
    ('standard', 'Standard', 3.50, 1.85, 0.35, 7.00),
    ('comfort', 'Comfort', 5.00, 2.35, 0.45, 10.00),
    ('xl', 'XL Group', 6.50, 2.90, 0.55, 13.00)
ON CONFLICT (service_type) DO NOTHING;

INSERT INTO marketplace_ops.pricing_policies (
    region_id, policy_version, min_multiplier, max_multiplier, smoothing_alpha, hysteresis_deadband, max_step_delta, neighbor_supply_weight
) VALUES
    ('us_east', 'policy-v1.0', 1.00, 3.00, 0.450, 0.080, 0.35, 0.350),
    ('us_west', 'policy-v2.0-smoothed', 1.00, 2.75, 0.300, 0.100, 0.25, 0.450)
ON CONFLICT (policy_version) DO NOTHING;

INSERT INTO marketplace_ops.experiment_configs (
    experiment_id, description, control_policy_version, treatment_policy_version, treatment_allocation, status
) VALUES (
    'exp_neighbor_smoothed_pricing_2026',
    'Evaluate higher 1-ring neighbor supply weight and stronger multiplier smoothing to reduce surge oscillation',
    'policy-v1.0',
    'policy-v2.0-smoothed',
    0.500,
    'RUNNING'
) ON CONFLICT (experiment_id) DO NOTHING;

-- Enable Debezium Logical Replication Publication
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'marketplace_cdc_pub') THEN
        CREATE PUBLICATION marketplace_cdc_pub FOR TABLE
            marketplace_ops.cities,
            marketplace_ops.service_types,
            marketplace_ops.drivers,
            marketplace_ops.pricing_policies,
            marketplace_ops.regional_events,
            marketplace_ops.experiment_configs;
    END IF;
END $$;
