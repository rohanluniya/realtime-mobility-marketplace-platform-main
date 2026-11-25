# Known Limitations & Production Scaling Path

Transparency about what was measured locally vs. what requires a multi-node cluster:

1. **Local Single-Node Verification (`local_native`) vs. Multi-Node Distributed Execution (`docker_distributed`):**
   - All live benchmarks, chaos drills, API checks, and Control Tower captures executed on a single Apple M4 Pro workstation using real Apache Iceberg V2 (`pyiceberg`), DuckDB 2.0, H3 4.5.0, Avro, FastAPI, `LocalWalEventBus`, and SQLite WAL serving KV (because Docker and unsigned host JVM binaries are restricted on the development workstation).
   - Distributed assets (`docker-compose.yml`, `Dockerfile.standalone`, `infra/kubernetes/marketplace-platform.yaml`, Flink SQL/DataStream, Spark 4.1.4, Trino 484, Debezium 3.7.1, Airflow 3.4.0, Prometheus/Grafana/Loki/OTel/Marquez) use live-verified pinned multi-arch tags and are statically validated in CI, but multi-broker Kafka and multi-TaskManager Flink throughput were not measured on a physical cluster here.
2. **Synthetic Marketplace Simulation:**
   - Rider requests, driver GPS trajectories, traffic factors, and regional surge events are deterministically generated. The `virtual-scale` profile proves handling of `1,000,000` virtual participant identities with `4,500` concurrently active entities—distinct from `1,000,000` simultaneously active mobile connections, which would require partitioned multi-broker Kafka, multi-node Flink RocksDB clusters, and sharded Valkey Cluster.
3. **Spatial Spillover in H3 Cluster Experiments:**
   - H3 resolution-7 randomization substantially reduces rider-level network interference compared to user-level splits, but drivers can still cross cluster boundaries between adjacent control and treatment cells. Production marketplaces layer **switchback (time x H3-cluster)** designs or coarsen randomization to H3 resolution 5/6 with buffer rings for high-precision causal lift measurement.
4. **In-Memory Rate Limiter:**
   - The FastAPI rate limiter (`apps/marketplace-api/security.py`) tracks counters per process; multi-replica Kubernetes deployments should back rate limiting with shared Valkey counters (`INCR` + `EXPIRE`).
