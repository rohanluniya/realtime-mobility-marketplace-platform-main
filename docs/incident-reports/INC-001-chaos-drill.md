# INC-001: Automated Chaos & Fault-Injection Game-Day Report

## Summary

Executed all **9 controlled chaos and fault-injection drills** in [`tests/chaos/test_chaos_fault_injection.py`](../../tests/chaos/test_chaos_fault_injection.py) covering transport broker loss, mid-stream TaskManager crashes, serving KV outages, stale GPS reorderings, duplicate floods, stuck driver reservations, stale serving writes, serving-vs-lakehouse drift, and airport hotspot skew with poison Avro bursts. All **9/9 scenarios passed** with zero authoritative data loss (`results/chaos/chaos_results.json`).

## Drill Results Table

| # | Scenario | Duration (ms) | Key Measured Verification | Status |
| --- | --- | --- | --- | --- |
| 01 | `broker_outage_and_recovery` | 16.92 | Producer fails fast with `BrokerUnavailableError` while offline; resumes cleanly with 0 lost messages on recovery | PASS |
| 02 | `stream_processor_crash_and_checkpoint_parity` | 180.69 | Mid-stream crash after 450 records -> restore checkpoint -> **exact supply & pricing parity across all 67 cells** | PASS |
| 03 | `serving_store_outage_safe_fallback_and_full_rebuild` | 167.36 | Outage triggers safe `1.00x` (`SAFE_FALLBACK_OUTAGE`); full wipe & rebuild of `180` keys completes in **`35.34 ms`** with exact parity | PASS |
| 04 | `out_of_order_driver_location_rejected` | 15.85 | Stale sequence-2 Midtown ping ignored after sequence-5 JFK ping; driver remains exclusively in `872a103b1ffffff` | PASS |
| 05 | `duplicate_event_burst_idempotency` | 14.58 | `9/9` duplicate driver events suppressed via `event_id` deduplication; cell supply remains exact (`1`) | PASS |
| 06 | `reservation_and_heartbeat_timers` | 12.95 | Stalled `RESERVED` driver auto-released to `AVAILABLE` (`>45s`); silent driver evicted to `OFFLINE` (`>180s`) | PASS |
| 07 | `stale_serving_store_version_rejected` | 1.37 | Out-of-order writes with version `9` and `10` rejected against stored version `10`, preserving `1.75x` | PASS |
| 08 | `serving_drift_detection_and_repair` | 509.04 | `3/3` artificially corrupted live pricing keys (`4.99x`) detected by Iceberg reconciliation and repaired to `CONSISTENT` | PASS |
| 09 | `hot_h3_cell_skew_and_poison_quarantine` | 32.79 | `120` concentrated JFK requests + `15` poison Avro frames -> `15/15` quarantined to DLQ, multiplier bounded (`2.95x <= 3.00x`) | PASS |

## Real Defect Caught & Remediated During Game Day

During initial execution of Scenario 02 (`stream_processor_crash_and_checkpoint_parity`), globally sorting consumed records by `event_time` across partitions allowed a later offset (`N+1`) with an earlier timestamp to advance `committed_offsets` ahead of an unprocessed earlier offset (`N`), causing multiplier drift on 4 cells after crash recovery. Replacing global sorting with a **per-partition FIFO K-way min-heap merge** eliminated the offset-skipping defect permanently and achieved 100% bit-for-bit recovery parity.
