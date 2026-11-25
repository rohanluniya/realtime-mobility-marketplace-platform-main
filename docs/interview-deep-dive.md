# Staff Data Engineering Interview Deep-Dive Guide

Concise technical walkthroughs for the hardest design questions raised by this platform.

## 1. How do you guarantee a moving driver is never double-counted across H3 cells?
Key the streaming operator by `driver_id`, never by `h3_cell`. The keyed state holds `(state, current_h3_cell, last_seq, last_event_ts)`. Before mutating state, reject any event with `seq < last_seq`. When an `AVAILABLE` driver crosses from `old_cell` to `new_cell`, emit atomic paired deltas `(-1 old_cell, +1 new_cell)` inside the same keyed state transition (see `DriverSupplyKeyedProcessFunction` in `streaming/flink-datastream/driver_supply_state_job.py` and `MarketplaceStreamProcessor._apply_driver_event`).

## 2. How do you handle multi-topic out-of-order streaming joins without unbounded state?
Three topics (`rider.requests`, `dispatch.offers`, `trip.lifecycle`) arrive from independent partitions. First, enforce **bounded interval windows** (3 min for offers, 5 min for match confirmation in Flink SQL) plus `StateTtlConfig` so RocksDB memory stays bounded. Second, buffer out-of-order right-side events (`_get_or_create_join_stub()`) so an offer arriving 50 ms ahead of `ride_requested` is never dropped.

## 3. Why separate Valkey (serving) from Iceberg (lakehouse) instead of reading Iceberg directly or writing both atomically?
Distributed two-phase commits across Kafka and Valkey hurt availability and throughput. Instead, Kafka changelog topics are the durable bus; the serving materializer reads changelog topics and executes **monotonic version-aware CAS writes** (`incoming_version > stored_version` via Valkey Lua script). If Valkey dies, the API falls back safely to `1.00x` pricing, and rebuilding the entire serving store from durable logs takes `35.34 ms` (smoke) to `120.28 ms` (local). Scheduled reconciliation repairs any residual drift against Iceberg Gold.

## 4. Why randomize pricing experiments by H3 cell instead of by rider?
In a two-sided marketplace, riders in the same neighborhood compete for the same finite pool of drivers. Assigning Rider A to `1.0x` (control) and Rider B across the street to `1.6x` (treatment) creates severe **network interference (SUTVA violation)**—Rider A consumes the drivers that treatment was trying to allocate. Hashing entire H3 resolution-7 clusters (`SHA-256(experiment_id:h3_cell)`) isolates supply pools and validates assignment integrity with a 1-df chi-squared **Sample Ratio Mismatch (SRM)** check.

## 5. Why did 8-way skew salting slow down DuckDB in your benchmarks?
Salting adds a hash computation and a two-stage aggregation. On a single-node in-process engine scanning `195` H3 cells, there is no cross-node network shuffle to relieve, so salting ran `+6.1%` slower (`11.62 ms` vs `10.95 ms`). On a multi-node Spark cluster where JFK or Midtown accounts for 20× average partition volume, salting eliminates straggler executor tasks. Measuring and reporting both outcomes prevents blind cargo-cult optimization.
