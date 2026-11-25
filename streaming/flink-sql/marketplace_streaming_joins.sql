-- =============================================================================
-- Apache Flink 2.1.3 SQL — Real-Time Mobility Marketplace Streaming Joins,
-- Windowed H3 Supply-Demand Aggregations, and Temporal Pricing Policy Joins
-- =============================================================================
-- Target runtime:
--   flink:2.1.3-scala_2.12-java17
--   org.apache.flink:flink-sql-connector-kafka:5.0.0-2.1
--   org.apache.iceberg:iceberg-flink-runtime-2.1:1.12.0
-- =============================================================================

SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl' = '30 min';
SET 'table.exec.source.idle-timeout' = '15 s';
SET 'pipeline.name' = 'marketplace-streaming-joins-sql';

-- 1. Rider ride requests stream (keyed by request_id)
CREATE TABLE rider_requests_kafka (
    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP_LTZ(3),
    producer_time TIMESTAMP_LTZ(3),
    city_id STRING,
    region_id STRING,
    rider_id STRING,
    request_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    sequence_number BIGINT,
    payload_json STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'marketplace.rider.requests',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-marketplace-sql-joins',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

-- 2. Dispatch offer events stream (keyed by request_id)
CREATE TABLE dispatch_offers_kafka (
    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP_LTZ(3),
    city_id STRING,
    region_id STRING,
    rider_id STRING,
    driver_id STRING,
    request_id STRING,
    sequence_number BIGINT,
    payload_json STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'marketplace.dispatch.offers',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-marketplace-sql-joins',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

-- 3. Trip lifecycle events stream (keyed by trip_id)
CREATE TABLE trip_lifecycle_kafka (
    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP_LTZ(3),
    city_id STRING,
    region_id STRING,
    rider_id STRING,
    driver_id STRING,
    request_id STRING,
    trip_id STRING,
    payload_json STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'marketplace.trip.lifecycle',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-marketplace-sql-joins',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

-- 4. Pricing policy CDC changelog stream (versioned table for temporal joins)
CREATE TABLE pricing_policies_cdc (
    region_id STRING,
    policy_version STRING,
    min_multiplier DOUBLE,
    max_multiplier DOUBLE,
    smoothing_alpha DOUBLE,
    hysteresis_deadband DOUBLE,
    max_step_delta DOUBLE,
    updated_at TIMESTAMP_LTZ(3),
    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND,
    PRIMARY KEY (region_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'marketplace.pricing.policies',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 5. Output Kafka sink for correlated matching outcomes
CREATE TABLE matching_outcomes_sink (
    request_id STRING,
    city_id STRING,
    region_id STRING,
    rider_id STRING,
    driver_id STRING,
    trip_id STRING,
    request_time TIMESTAMP_LTZ(3),
    first_offer_time TIMESTAMP_LTZ(3),
    match_time TIMESTAMP_LTZ(3),
    request_to_offer_ms BIGINT,
    request_to_match_ms BIGINT,
    applied_policy_version STRING,
    policy_max_multiplier DOUBLE,
    PRIMARY KEY (request_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'marketplace.matching.outcomes',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- =============================================================================
-- Bounded Interval Join: Rider Request -> Dispatch Offer -> Trip Match
-- Enriched via Temporal Table Join against versioned Pricing Policy as of request_time
-- (Bounded state windows guarantee finite RocksDB state growth!)
-- =============================================================================
INSERT INTO matching_outcomes_sink
SELECT
    r.request_id,
    r.city_id,
    r.region_id,
    r.rider_id,
    o.driver_id,
    t.trip_id,
    r.event_time AS request_time,
    o.event_time AS first_offer_time,
    t.event_time AS match_time,
    TIMESTAMPDIFF(MILLISECOND, r.event_time, o.event_time) AS request_to_offer_ms,
    TIMESTAMPDIFF(MILLISECOND, r.event_time, t.event_time) AS request_to_match_ms,
    p.policy_version AS applied_policy_version,
    p.max_multiplier AS policy_max_multiplier
FROM rider_requests_kafka AS r
LEFT JOIN dispatch_offers_kafka AS o
    ON r.request_id = o.request_id
    AND o.event_type = 'driver_offer_accepted'
    AND o.event_time BETWEEN r.event_time AND r.event_time + INTERVAL '3' MINUTE
LEFT JOIN trip_lifecycle_kafka AS t
    ON r.request_id = t.request_id
    AND t.event_type = 'trip_matched'
    AND t.event_time BETWEEN r.event_time AND r.event_time + INTERVAL '5' MINUTE
LEFT JOIN pricing_policies_cdc FOR SYSTEM_TIME AS OF r.event_time AS p
    ON r.region_id = p.region_id
WHERE r.event_type = 'ride_requested';

-- =============================================================================
-- Sliding Window Demand Velocity (1-minute window, 15-second hop)
-- Feeds Lakehouse Iceberg catalog directly via iceberg-flink-runtime-2.1:1.12.0
-- =============================================================================
CREATE CATALOG marketplace_iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://lakekeeper:8181/catalog',
    'warehouse' = 'marketplace',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://rustfs:9000',
    's3.path-style-access' = 'true'
);

CREATE TABLE IF NOT EXISTS marketplace_iceberg.silver.flink_demand_velocity_1m (
    window_start TIMESTAMP_LTZ(3),
    window_end TIMESTAMP_LTZ(3),
    city_id STRING,
    region_id STRING,
    requests_created BIGINT,
    rider_cancellations BIGINT
);

INSERT INTO marketplace_iceberg.silver.flink_demand_velocity_1m
SELECT
    window_start,
    window_end,
    city_id,
    region_id,
    COUNT(*) FILTER (WHERE event_type = 'ride_requested') AS requests_created,
    COUNT(*) FILTER (WHERE event_type = 'ride_request_cancelled') AS rider_cancellations
FROM TABLE(
    HOP(TABLE rider_requests_kafka, DESCRIPTOR(event_time), INTERVAL '15' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, city_id, region_id;
