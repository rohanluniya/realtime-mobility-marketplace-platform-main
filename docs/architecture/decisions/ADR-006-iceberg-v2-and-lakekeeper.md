# ADR-006: Apache Iceberg Format V2 with Lakekeeper REST Catalog & RustFS Object Storage

## Status
Accepted

## Context
MinIO Community Edition ceased Docker Hub image publishing in late 2025 and archived its public repository in April 2026. The lakehouse requires an actively maintained, Apache-2.0 S3-compatible object store and an open Iceberg REST catalog supporting multi-engine access (PyIceberg, Spark 4.1.4, Flink 2.1.3, Trino 484).

## Decision
Standardize on **Apache Iceberg Format V2**, served via **Lakekeeper v0.15.1** (Iceberg REST catalog) over **RustFS 1.0.6** (Apache-2.0 S3-compatible storage) in Docker/Kubernetes, with a local SQLite Iceberg catalog (`pyiceberg[sql-sqlite,pyiceberg-core]==0.12.0`) for zero-Docker execution.

## Consequences
- True cross-engine ACID snapshots, time travel (`snapshot_id`), safe schema evolution, and small-file compaction (`-59.97%` storage reduction verified on `bronze.raw_events`).
- Zero proprietary catalog lock-in or deprecated container dependencies.
