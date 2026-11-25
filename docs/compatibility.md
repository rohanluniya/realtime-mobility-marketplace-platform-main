# Technology Stack, Version Pinning & Compatibility Matrix

Every container image, Python wheel, and JVM connector in this repository is pinned to an explicit verified open-source release (zero `:latest` tags).

## Container Image Compatibility Matrix

| Component | Pinned Image / Artifact | License | Architectures | Notes |
| --- | --- | --- | --- | --- |
| Event Streaming | `apache/kafka:4.3.2` | Apache-2.0 | `amd64`, `arm64` | Native KRaft mode (no ZooKeeper) |
| Schema Registry | `apicurio/apicurio-registry:3.3.5` | Apache-2.0 | `amd64`, `arm64` | Confluent v7 API compatible |
| Low-Latency Serving KV | `valkey/valkey:9.2.0-alpine3.24` | BSD-3-Clause | `amd64`, `arm64` | Open-source Redis fork; Lua CAS scripts |
| Stream Processing | `flink:2.1.3-scala_2.12-java17` | Apache-2.0 | `amd64`, `arm64` | Paired with `flink-sql-connector-kafka:5.0.0-2.1` & `iceberg-flink-runtime-2.1:1.12.0` |
| Batch Lakehouse Engine | `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu` | Apache-2.0 | `amd64`, `arm64` | Paired with `iceberg-spark-runtime-4.1_2.13:1.12.0` & `openlineage-spark_2.13:1.55.0` |
| Object Storage (S3) | `rustfs/rustfs:1.0.6` | Apache-2.0 | `amd64`, `arm64` | Replaces archived MinIO CE |
| Iceberg REST Catalog | `quay.io/lakekeeper/catalog:v0.15.1` | Apache-2.0 | `amd64`, `arm64` | Native Iceberg V2 REST catalog |
| SQL Query Engine | `trinodb/trino:484` | Apache-2.0 | `amd64`, `arm64` | Native S3 + Iceberg REST connector |
| Operational DB & Geo | `imresamu/postgis:17-3.6.1-alpine3.23` | PostgreSQL / GPL-2.0 | `amd64`, `arm64` | Official `postgis/postgis` lacks native `arm64` |
| Logical Replication CDC | `quay.io/debezium/connect:3.7.1.Final` | Apache-2.0 | `amd64`, `arm64` | Uses `pgoutput` logical replication slot |
| Orchestration | `apache/airflow:slim-3.4.0-python3.12` | Apache-2.0 | `amd64`, `arm64` | Airflow 2.x reached EOL in April 2026 |
| Metrics & Alerting | `prom/prometheus:v3.16.0` | Apache-2.0 | `amd64`, `arm64` | Scrapes FastAPI `/metrics` & Flink |
| Dashboards & Logs | `grafana/grafana:13.0.14`, `grafana/loki:3.6.22` | AGPL-3.0 | `amd64`, `arm64` | Ops UI only (not linked into Python app) |
| Telemetry Collector | `otel/opentelemetry-collector-contrib:0.166.0-${OTEL_ARCH:-arm64}` | Apache-2.0 | `amd64`, `arm64` | OTLP gRPC/HTTP receiver |
| Data Lineage | `marquezproject/marquez:0.51.1` | Apache-2.0 | `amd64` (`platform: linux/amd64`) | Runs via Rosetta 2 on Apple Silicon |

## Python Dependency Isolation

- Host Python (`pyproject.toml` + `uv.lock`, Python 3.12.8) pins `pyiceberg[sql-sqlite,pyiceberg-core]==0.12.0`, `duckdb==2.0.0`, `pyarrow==25.0.1`, `h3==4.5.0`, `valkey==6.2.0`, `fastavro==1.13.1`, `fastapi==0.163.2`, `uvicorn==0.54.0`, and `confluent-kafka==2.16.1`.
- Because `apache-flink==2.1.3` pins `pyarrow<21`, distributed worker requirements are isolated inside `infra/docker/requirements-flink.txt`, `infra/docker/requirements-spark.txt`, and `infra/docker/requirements-airflow.txt`.
- Automated license compliance (`scripts/check_licenses.py`) audited **69 installed Python packages** with **0 copyleft or proprietary violations**, and `scripts/generate_sbom.py` exports a CycloneDX 1.5 SBOM to `results/security/sbom.cdx.json`.
