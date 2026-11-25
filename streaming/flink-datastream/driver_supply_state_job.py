"""Apache Flink 2.1.3 PyFlink DataStream Job — Authoritative Driver Supply & Atomic H3 Cell Transfer.

Implements the core geospatial correctness guarantee (Section 10):
- Keys incoming driver availability and GPS streams by `driver_id`.
- Maintains per-driver authoritative state in RocksDB `ValueState` with bounded `StateTtlConfig`
  and event-time inactivity timers (heartbeat timeout -> OFFLINE eviction).
- Rejects stale/out-of-order updates via monotonic `(sequence_number, event_time)` comparison.
- Whenever an `AVAILABLE` driver crosses an H3 resolution-7 cell boundary, atomically emits:
    1) `-1` supply delta for `previous_h3_cell`
    2) `+1` supply delta for `new_h3_cell`
  eliminating double-counting across adjacent hexagonal cells.

Runtime dependencies live in `infra/docker/requirements-flink.txt` (`apache-flink==2.1.3`, `h3==4.5.0`).
"""

from __future__ import annotations

import json
import os
from typing import Any

import h3

try:
    from pyflink.common import Duration, Time, Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import (
        CheckpointingMode,
        RuntimeExecutionMode,
        StreamExecutionEnvironment,
    )
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )
    from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
    from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
except ImportError as exc:  # pragma: no cover - container-only PyFlink runtime
    raise SystemExit(
        "PyFlink 2.1.3 is required to run this DataStream job. "
        "Run inside the Flink container (`docker compose --profile streaming up`) "
        "or run the local native processor (`services/state-materializer/run_pipeline.py`)."
    ) from exc


H3_PRICING_RES = int(os.getenv("MARKETPLACE_H3_PRICING_RES", "7"))
DRIVER_HEARTBEAT_TIMEOUT_MS = int(os.getenv("MARKETPLACE_DRIVER_HEARTBEAT_TIMEOUT_MS", "90000"))
RESERVATION_TIMEOUT_MS = int(os.getenv("MARKETPLACE_RESERVATION_TIMEOUT_MS", "45000"))


class DriverSupplyKeyedProcessFunction(KeyedProcessFunction):
    """Stateful per-driver machine emitting exact (+1 / -1) H3 cell supply deltas."""

    def open(self, runtime_context: RuntimeContext) -> None:
        ttl_cfg = (
            StateTtlConfig.new_builder(Time.hours(2))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        desc = ValueStateDescriptor("driver_authoritative_state", Types.STRING())
        desc.enable_time_to_live(ttl_cfg)
        self.driver_state = runtime_context.get_state(desc)

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        ev: dict[str, Any] = json.loads(value)
        driver_id = str(ev["driver_id"])
        ev_type = str(ev["event_type"])
        seq = int(ev.get("sequence_number") or 0)
        ev_ts_ms = int(ctx.timestamp() or 0)
        lat, lon = float(ev["latitude"]), float(ev["longitude"])
        new_cell = h3.latlng_to_cell(lat, lon, H3_PRICING_RES)
        city_id = str(ev["city_id"])
        region_id = str(ev["region_id"])

        raw_prev = self.driver_state.value()
        prev: dict[str, Any] | None = json.loads(raw_prev) if raw_prev else None

        # Guardrail: drop stale / out-of-order driver location or availability updates
        if prev is not None and (
            (seq > 0 and prev["last_seq"] > 0 and seq < prev["last_seq"])
            or (seq == prev["last_seq"] and ev_ts_ms < prev["last_ts_ms"])
        ):
            return

        payload = json.loads(ev.get("payload_json") or "{}")
        if ev_type == "driver_location_updated":
            target_state = prev["state"] if prev is not None else "AVAILABLE"
        else:
            target_state = str(
                payload.get("target_state")
                or {
                    "driver_online": "AVAILABLE",
                    "driver_available": "AVAILABLE",
                    "driver_offline": "OFFLINE",
                    "driver_reserved": "RESERVED",
                    "driver_trip_started": "ON_TRIP",
                    "driver_trip_completed": "AVAILABLE",
                }.get(ev_type, "AVAILABLE")
            )

        was_avail = prev is not None and prev["state"] == "AVAILABLE"
        old_cell = prev["h3_cell"] if prev is not None else None
        now_avail = target_state == "AVAILABLE"

        # Atomic supply deltas: guaranteed zero double-counting across H3 cells
        if was_avail and not now_avail and old_cell:
            yield self._delta(old_cell, city_id, region_id, -1, driver_id, ev_ts_ms, "state_exit")
        elif not was_avail and now_avail:
            yield self._delta(new_cell, city_id, region_id, +1, driver_id, ev_ts_ms, "state_enter")
        elif was_avail and now_avail and old_cell and old_cell != new_cell:
            yield self._delta(old_cell, city_id, region_id, -1, driver_id, ev_ts_ms, "h3_transfer_out")
            yield self._delta(new_cell, city_id, region_id, +1, driver_id, ev_ts_ms, "h3_transfer_in")

        next_state = {
            "driver_id": driver_id,
            "city_id": city_id,
            "region_id": region_id,
            "state": target_state,
            "h3_cell": new_cell,
            "last_seq": max(seq, prev["last_seq"] if prev else 0),
            "last_ts_ms": max(ev_ts_ms, prev["last_ts_ms"] if prev else 0),
            "timer_ts_ms": ev_ts_ms + (RESERVATION_TIMEOUT_MS if target_state == "RESERVED" else DRIVER_HEARTBEAT_TIMEOUT_MS),
        }
        self.driver_state.update(json.dumps(next_state, separators=(",", ":")))

        if target_state in ("AVAILABLE", "RESERVED"):
            ctx.timer_service().register_event_time_timer(next_state["timer_ts_ms"])

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        raw_cur = self.driver_state.value()
        if not raw_cur:
            return
        cur = json.loads(raw_cur)
        if timestamp < int(cur.get("timer_ts_ms", 0)):
            return

        if cur["state"] == "AVAILABLE":
            # Stale driver heartbeat eviction
            yield self._delta(
                cur["h3_cell"],
                cur["city_id"],
                cur["region_id"],
                -1,
                cur["driver_id"],
                timestamp,
                "heartbeat_timeout_eviction",
            )
            cur["state"] = "OFFLINE"
            self.driver_state.update(json.dumps(cur, separators=(",", ":")))
        elif cur["state"] == "RESERVED":
            # Reservation timeout release back to AVAILABLE supply
            yield self._delta(
                cur["h3_cell"],
                cur["city_id"],
                cur["region_id"],
                +1,
                cur["driver_id"],
                timestamp,
                "reservation_timeout_release",
            )
            cur["state"] = "AVAILABLE"
            self.driver_state.update(json.dumps(cur, separators=(",", ":")))

    @staticmethod
    def _delta(
        h3_cell: str,
        city_id: str,
        region_id: str,
        delta: int,
        driver_id: str,
        ts_ms: int,
        reason: str,
    ) -> str:
        return json.dumps(
            {
                "h3_cell": h3_cell,
                "h3_resolution": H3_PRICING_RES,
                "city_id": city_id,
                "region_id": region_id,
                "supply_delta": delta,
                "driver_id": driver_id,
                "event_time_ms": ts_ms,
                "reason": reason,
            },
            separators=(",", ":"),
        )


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10_000)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(brokers)
        .set_topics("marketplace.driver.locations", "marketplace.driver.availability")
        .set_group_id("flink-driver-supply-state")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))

    stream = env.from_source(source, watermarks, "driver_events_kafka")
    deltas = (
        stream.key_by(lambda raw: str(json.loads(raw)["driver_id"]), key_type=Types.STRING())
        .process(DriverSupplyKeyedProcessFunction(), output_type=Types.STRING())
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(brokers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("marketplace.supply.aggregates")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    deltas.sink_to(sink)
    env.execute("marketplace-driver-supply-h3-state-engine")


if __name__ == "__main__":
    main()
