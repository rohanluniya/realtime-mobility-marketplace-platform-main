# ADR-001: Multi-Resolution H3 Indexing with Resolution 7 for Dynamic Pricing

## Status
Accepted

## Context
Ride-hailing supply-demand pricing requires aggregating moving drivers and rider requests into spatial cells. Coarser hexagons blur localized airport/stadium hotspots into surrounding residential neighborhoods; finer hexagons suffer severe supply sparsity (most cells hold 0 drivers), noisy multipliers, and rapid state explosion.

## Decision
Adopt a three-tier Uber H3 hierarchy:
- **Resolution 9 (`~0.11 km²`):** Fine pickup/dropoff storage and dispatch proximity features.
- **Resolution 7 (`~5.16 km²`):** Primary live supply-demand balance and dynamic pricing decision unit, augmented by **1-ring neighbor supply** (6 surrounding hexagons).
- **Resolution 5 (`~252.9 km²`):** Metro district executive rollups and multi-city analytics.

## Evidence & Consequences
Measured on the identical 20,696-event local workload (`results/benchmarks/h3_resolution_tradeoffs.json`):
- **Res 6** yields only `59` cells but causes `74.58%` of cells to surge (`avg 1.950x`) due to spatial bleed.
- **Res 7** tracks `195` cells with `108.5 ms` KV materialization and `0.082 ms` p95 lookups.
- **Res 9** expands to `986` cells (`5.06×`), drives `92.70%` of cells to zero local supply, and slows KV materialization to `463.8 ms` (`4.28×` slower than Res 7).
