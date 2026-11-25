# Event Contracts, Kafka Topic Topology & Schema Evolution

## Kafka Topic Specifications (12 Authoritative Topics)

Defined in [`marketplace_platform/contracts.py`](../../marketplace_platform/contracts.py):

| Topic | Partitions | Partition Key | Avro Schema (`schema_id`) | Retention | Purpose |
| --- | --- | --- | --- | --- | --- |
| `marketplace.rider.requests` | 12 | `request_id` | `marketplace_event.avsc` (`1`) | 7d | Ride creation, cancellations, expirations |
| `marketplace.driver.locations` | 12 | `driver_id` | `marketplace_event.avsc` (`1`) | 3d | Continuous driver GPS pings & H3 cell crossings |
| `marketplace.driver.availability` | 12 | `driver_id` | `marketplace_event.avsc` (`1`) | 7d | 7-state availability transitions (`ONLINE`, `AVAILABLE`, `RESERVED`, `ON_TRIP`, `OFFLINE`) |
| `marketplace.dispatch.offers` | 12 | `request_id` | `marketplace_event.avsc` (`1`) | 7d | Offer sent, driver accept/reject & ETA |
| `marketplace.trip.lifecycle` | 12 | `trip_id` | `marketplace_event.avsc` (`1`) | 14d | Match, trip start, completion, post-match cancellation |
| `marketplace.regional.events` | 6 | `city_id` | `marketplace_event.avsc` (`1`) | 14d | Airport waves, stadium exits, weather/traffic events |
| `marketplace.pricing.policies` | 3 | `region_id` | `marketplace_event.avsc` (`1`) | 30d | Policy updates & Debezium CDC policy stream |
| `marketplace.supply.aggregates` | 12 | `h3_cell` | `cell_aggregate_event.avsc` (`4`) | 3d | Versioned H3 available-driver counts feeding KV serving |
| `marketplace.demand.aggregates` | 12 | `h3_cell` | `cell_aggregate_event.avsc` (`4`) | 3d | Versioned H3 demand intensity feeding KV serving |
| `marketplace.pricing.decisions` | 12 | `h3_cell` | `pricing_decision_event.avsc` (`2`) | 14d | Versioned explainable dynamic pricing recommendations |
| `marketplace.matching.outcomes` | 12 | `request_id` | `matching_outcome_event.avsc` (`3`) | 14d | Correlated multi-entity funnel & latency records |
| `marketplace.dead-letter` | 6 | original key | `dlq_event.avsc` (`5`) | 30d | Quarantined invalid/poison Avro payloads |

## Wire Format & Schema Evolution

- **Confluent/Apicurio Wire Framing:** Every message starts with magic byte `0x00`, followed by a big-endian 4-byte `schema_id`, followed by schemaless Avro binary encoding.
- **V1 ↔ V2 Full Compatibility:** [`schemas/avro/v2/marketplace_event.avsc`](../../schemas/avro/v2/marketplace_event.avsc) appends nullable optional fields (`h3_cell_res7`, `experiment_cohort`) with explicit `null` defaults. Both backward (`v1 writer -> v2 reader`) and forward (`v2 writer -> v1 reader`) decoding are verified automatically in [`tests/contracts/test_avro_contracts.py`](../../tests/contracts/test_avro_contracts.py).
- **Dead-Letter Isolation:** Any payload failing magic-byte validation, unknown schema ID lookup, or Avro decoding is routed to `marketplace.dead-letter` and persisted to `bronze.dead_letter` without stalling stream consumers (`test_chaos_09_hot_h3_cell_skew_and_poison_quarantine`).
