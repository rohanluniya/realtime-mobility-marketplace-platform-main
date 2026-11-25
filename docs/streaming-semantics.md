# Streaming Semantics, State Machines & Multi-Entity Joins

Implemented in [`marketplace_platform/state_engine.py`](../marketplace_platform/state_engine.py), [`streaming/flink-datastream/driver_supply_state_job.py`](../streaming/flink-datastream/driver_supply_state_job.py), and [`streaming/flink-sql/marketplace_streaming_joins.sql`](../streaming/flink-sql/marketplace_streaming_joins.sql).

## Authoritative Entity State Machines

### Driver Availability Machine (7 States)
`OFFLINE` ↔ `ONLINE_UNAVAILABLE` ↔ `AVAILABLE` ↔ `RESERVED` → `EN_ROUTE_TO_PICKUP` → `ON_TRIP` → `AVAILABLE` (plus `PAUSED`).
- Only drivers currently in `AVAILABLE` contribute to `supply_by_cell[h3_cell]`.
- Every `driver_location_updated` event checks `(sequence_number, event_time)` against the driver's authoritative record; older sequence numbers are dropped (`stale_out_of_order_ignored`) so delayed GPS packets never teleport a driver back to an older hexagon.
- Crossing from `old_cell` to `new_cell` while `AVAILABLE` atomically executes `supply[old_cell] -= 1` and `supply[new_cell] += 1`.

### Rider Request Machine (7 States)
`CREATED` → `SEARCHING` → `OFFERED` → `MATCHED` → `FULFILLED` (or `CANCELLED` / `EXPIRED`).
- Only active unmatched states (`CREATED`, `SEARCHING`, `OFFERED`) contribute to `demand_by_cell[h3_cell]`.
- Entering any terminal state (`MATCHED`, `CANCELLED`, `EXPIRED`, `FULFILLED`) immediately decrements `demand_by_cell[h3_cell]` so cancelled or matched rides never inflate live demand.
- `assert_state_invariants()` verifies after every processing cycle that cell supply equals the exact sum of `AVAILABLE` drivers per cell and cell demand equals the exact sum of active requests per cell.

## Ordering, Watermarks, Timers & Fault-Tolerant Checkpoints

1. **Per-Partition FIFO K-Way Event-Time Merge:** Within each `(topic, partition)`, records are strictly consumed in ascending offset order (`0, 1, 2, ...`), while a min-heap merges across partitions by `timestamp_ms`. This guarantees offset `N` is always processed and checkpointed before offset `N+1`, yielding **100% bit-for-bit checkpoint recovery parity** under mid-stream crashes (`test_chaos_02_stream_processor_crash_and_checkpoint_parity`).
2. **Per-Domain Watermark Isolation:**
   - Global request/trip watermark (`watermark_ts = max_event_ts - 30s`) governs unmatched request expiration (`request_expiry_s = 180s`) and deduplication TTL (`3600s`).
   - Driver-domain watermark (`driver_watermark_ts = driver_max_event_ts - 30s`) governs silent-driver heartbeat eviction (`driver_heartbeat_ttl_s = 180s`) and stalled reservation release (`reservation_timeout_s = 45s`) so long-running trip completion timestamps never falsely evict active idle drivers.
3. **Out-of-Order Multi-Topic Join Buffering:** Because `marketplace.rider.requests`, `marketplace.dispatch.offers`, and `marketplace.trip.lifecycle` have independent partitions, right-side offer/trip events that arrive ahead of `ride_requested` upsert a keyed join stub (`_get_or_create_join_stub()`) that hydrates cleanly when `ride_requested` arrives.
4. **Deduplication & Late Side-Output:** Duplicate `event_id` records are suppressed via TTL-bounded `dedup_seen` state; events arriving behind the watermark are captured into `late_events_side_output`.
