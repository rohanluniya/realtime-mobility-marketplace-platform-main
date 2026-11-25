# ADR-003: Apache Kafka 4.3.2 KRaft & Apicurio Registry 3.3.5 with Avro Wire Framing

## Status
Accepted

## Context
Marketplace state spans 12 distinct event domains (rider requests, driver GPS, availability, dispatch offers, trip lifecycles, pricing policies, regional surges, supply/demand aggregates, pricing decisions, matching outcomes, and DLQ). Unversioned JSON payloads risk silent consumer corruption when new fields are added.

## Decision
Use **Apache Kafka 4.3.2** in ZooKeeper-free KRaft mode paired with **Apicurio Registry 3.3.5** (Apache-2.0, Confluent-compatible v7 API) and compact Avro binary wire framing (`0x00` magic byte + 4-byte schema ID + schemaless Avro body). Enforce explicit domain partition keys (`driver_id`, `request_id`, `trip_id`, `h3_cell`, `region_id`).

## Consequences
- Strict per-entity ordering guarantees within each partition (e.g., all location and availability events for `DRV-001` hit the same partition).
- Backward and forward schema evolution verified automatically in CI (`tests/contracts/test_avro_contracts.py`), with malformed messages quarantined to `marketplace.dead-letter`.
