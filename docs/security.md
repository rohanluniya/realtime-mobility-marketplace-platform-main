# Security, Privacy, Access Control & Supply-Chain Governance

## 1. Privacy by Construction (Location Telemetry Protection)

Raw GPS coordinates of riders and drivers are sensitive location data. This platform enforces a strict privacy boundary:
- **Public & Dashboard Endpoints (`/marketplace/*`, `/api/map/h3-cells.geojson`):** Strictly return **H3 resolution-7 cell aggregates** and **city/service-tier summaries**. Individual `rider_id` or `driver_id` location histories are never returned by public serving APIs.
- **Synthetic Data Labeling:** Every API response carries the HTTP header `X-Data-Notice: Synthetic-Marketplace-Simulation`, and the Control Tower displays a permanent synthetic-data banner.

## 2. Authentication, RBAC & Rate Limiting

Implemented in [`apps/marketplace-api/security.py`](../apps/marketplace-api/security.py):
- **Role Hierarchy:** `public_aggregated (0)` < `analyst (1)` < `operator (2)` < `admin (3)`.
- **Constant-Time Token Comparison:** Bearer tokens and `X-API-Key` headers are validated with `hmac.compare_digest()` against environment-injected secrets (`MARKETPLACE_API_ADMIN_TOKEN`, `MARKETPLACE_API_OPERATOR_TOKEN`, `MARKETPLACE_API_ANALYST_TOKEN`).
- **Protected Operational Actions:** Mutating routes such as `POST /platform/actions/reconcile` require `operator` role or higher (unauthenticated -> `403 Forbidden`, invalid token -> `401 Unauthorized`, verified in `test_api_endpoints_rbac_and_safe_fallback`).
- **Rate Limiting:** Per-IP fixed-window limiter returns `429 Too Many Requests` on abuse.

## 3. Secret Hygiene & Audit Logging

- **Automatic Log Redaction:** `scrub_secrets()` in [`marketplace_platform/observability.py`](../marketplace_platform/observability.py) recursively redacts any dictionary key matching `(token|secret|password|authorization|api_key|credential|cookie|private_key)` before writing structured JSON logs (`test_secret_scrubbing`).
- **No Committed Secrets:** `.env.example` contains placeholder development values only; production Kubernetes manifests inject secrets via `secretKeyRef` and enforce non-root containers (`runAsNonRoot: true`, `runAsUser: 1000`) plus a Valkey `NetworkPolicy`.

## 4. Open-Source License Compliance & CycloneDX SBOM

- **License Audit (`scripts/check_licenses.py`):** Audited **69 installed Python packages** -> **0 copyleft or proprietary violations** (`results/security/license_audit.json`).
- **SBOM (`scripts/generate_sbom.py`):** Exports a CycloneDX 1.5 Bill of Materials (`results/security/sbom.cdx.json`) listing all 69 dependencies with Package URLs (`purl`).
