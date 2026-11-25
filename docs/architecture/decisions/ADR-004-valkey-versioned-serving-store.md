# ADR-004: Valkey 9.2.0 Version-Aware Serving Store with Rebuildable Local Fallback

## Status
Accepted

## Context
Redis changed its license to RSALv2/SSPLv1 in 2024, creating licensing friction for strictly open-source data platforms. Furthermore, replay and late-arriving changelog messages can overwrite fresher serving state if KV writes are blind `SET` operations.

## Decision
Use **Valkey 9.2.0** (BSD-3-Clause, Linux Foundation) in distributed Docker/Kubernetes deployments with an atomic Lua compare-and-swap script enforcing `incoming_version > stored_version` and bounded TTLs. Provide a functionally identical SQLite WAL `MaterializedStateStore` fallback for zero-Docker local execution.

## Consequences
- 100% OSI-compliant permissive licensing (`BSD-3-Clause` server, `MIT` Python client `valkey==6.2.0`).
- Stale replays are deterministically rejected (`test_chaos_07_stale_serving_store_version_rejected`).
- Local point-lookup latency measures `0.057 ms` p50 / `0.070 ms` p95 across 1,000 live pricing queries.
