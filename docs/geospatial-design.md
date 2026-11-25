# Geospatial Indexing & Multi-City Design

Implemented in [`marketplace_platform/geospatial.py`](../marketplace_platform/geospatial.py).

## Three-Tier H3 Hexagonal Hierarchy

Uber's H3 discrete global grid provides uniform neighbor geometry (every hexagon has 6 equidistant edge neighbors, avoiding square-grid diagonal distortion):

| Tier | H3 Resolution | Avg Cell Area | Role in Platform |
| --- | --- | --- | --- |
| Fine | Resolution 9 | `~0.11 km²` | Exact pickup/dropoff indexing and nearest-driver dispatch features |
| **Pricing (Primary)** | **Resolution 7** | **`~5.16 km²`** | **Authoritative live supply-demand state, 1-ring neighbor lookup, and dynamic pricing** |
| Coarse | Resolution 5 | `~252.9 km²` | Executive metro district rollups (`/marketplace/regions`, Trino district SQL) |

## Simulated Metropolitan Topologies

Three distinct urban geographies with explicit bounding boxes and weighted demand hotspots:
- **New York City (`us_east`):** Midtown Commute Core (`downtown`), JFK Airport Terminal Hub (`airport`), Midtown Arena / Concert Surge (`stadium`), Lower Manhattan Nightlife (`nightlife`).
- **San Francisco (`us_west`):** SoMa & FiDi Core (`downtown`), SFO Airport Pickup Zone (`airport`), Mission Bay Arena (`stadium`), Mission Nightlife Corridor (`nightlife`).
- **London (`eu_west`):** City & Canary Wharf (`downtown`), Heathrow Airport Hub (`airport`), Wembley Stadium Zone (`stadium`), Soho & West End Nightlife (`nightlife`).

## Spatial Skew & Coordinate Safety

- **Coordinate Validation:** `validate_coordinates()` rejects out-of-bounds latitudes/longitudes and null-island `(0.0, 0.0)` telemetry before indexing.
- **Hot-Zone Partition Skew:** Airports and stadiums concentrate up to 10–20× normal demand into single Res-7 cells (measured peak `158` requests in one Res-7 cell during the `local` profile). In streaming, drivers are keyed by `driver_id` (not `h3_cell`) to avoid hot-cell state bottlenecks; in batch Spark (`pipelines/spark/marketplace_batch_and_reconciliation.py`), an 8-way salted aggregation prevents reducer stragglers on distributed clusters.
- **Offline Self-Hosted GeoJSON Maps:** `/api/map/h3-cells.geojson` and `/api/map/basemap.geojson` export real H3 hexagonal boundary rings using `h3.cell_to_boundary()` with cosine-latitude aspect correction in the Control Tower UI so geospatial heatmaps render crisply with zero external map-tile requests.
