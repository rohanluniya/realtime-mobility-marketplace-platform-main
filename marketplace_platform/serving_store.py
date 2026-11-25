"""Low-Latency Materialized State Store (Valkey 9.2.0 + Rebuildable Local KV Fallback).

Architecture invariant (Section 13):
- Never assumes writing Kafka output and KV state is a single atomic transaction.
- Materializes serving views strictly from durable Kafka/WAL changelog topics
  (`marketplace.pricing.decisions`, `marketplace.supply.aggregates`, `marketplace.demand.aggregates`).
- Enforces monotonic version-aware writes (`incoming_version > stored_version`) so replayed
  or delayed messages can NEVER overwrite fresher serving state.
- Supports full `wipe_and_rebuild_from_log()` recovery and explicit stale/outage fallbacks.
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import AvroContractRegistry
from marketplace_platform.observability import get_logger, get_metrics
from marketplace_platform.transport import EventBus


class ServingStoreUnavailableError(RuntimeError):
    """Raised when serving state store is unavailable during chaos testing."""


class MaterializedStateStore:
    """Version-aware key-value serving store backed by Valkey 9.2.0 or SQLite WAL fallback."""

    def __init__(self, config: MarketplaceConfig) -> None:
        self.config = config
        self.mode = config.serving_store_mode
        self.logger = get_logger("state-materializer", "serving_store", environment=config.env)
        self.metrics = get_metrics()
        self._online = True
        self._valkey: Any = None
        self._db_path: Path = config.kv_store_path
        if self.mode == "valkey":
            import valkey

            self._valkey = valkey.Valkey(
                host=config.valkey_host,
                port=config.valkey_port,
                password=config.valkey_password or None,
                decode_responses=True,
                socket_timeout=2.0,
            )
        else:
            self._init_sqlite()

    def _init_sqlite(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._db_path) as con:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_state (
                    key TEXT PRIMARY KEY,
                    version INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at_epoch REAL NOT NULL,
                    expires_at_epoch REAL NOT NULL
                )
                """
            )
            con.commit()

    def set_online(self, online: bool) -> None:
        """Controlled fault-injection hook simulating Valkey / state-store outage."""
        self._online = online

    def _assert_online(self) -> None:
        if not self._online:
            self.metrics.serving_store_errors_total.labels(reason="store_unavailable").inc()
            raise ServingStoreUnavailableError("Materialized state store simulated offline")

    def put_versioned(
        self,
        key: str,
        version: int,
        payload: dict[str, Any],
        *,
        ttl_seconds: int = 3600,
    ) -> bool:
        """Write payload ONLY if incoming version > currently stored version.

        Returns True if applied, False if rejected as stale/duplicate.
        """
        t0 = time.perf_counter()
        self._assert_online()
        now = time.time()
        expires = now + ttl_seconds
        serialized = json.dumps(payload, separators=(",", ":"), default=str)

        if self._valkey is not None:
            # Atomic Lua compare-and-swap by version in Valkey
            lua = """
            local cur = redis.call('HGET', KEYS[1], 'version')
            if cur and tonumber(cur) >= tonumber(ARGV[1]) then
                return 0
            end
            redis.call('HSET', KEYS[1], 'version', ARGV[1], 'payload', ARGV[2], 'updated_at', ARGV[3])
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
            return 1
            """
            applied = int(self._valkey.eval(lua, 1, key, version, serialized, now, ttl_seconds)) == 1
        else:
            with sqlite3.connect(self._db_path) as con:
                row = con.execute("SELECT version FROM kv_state WHERE key = ?", (key,)).fetchone()
                if row is not None and int(row[0]) >= version:
                    applied = False
                else:
                    con.execute(
                        """
                        INSERT INTO kv_state(key, version, payload_json, updated_at_epoch, expires_at_epoch)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            version = excluded.version,
                            payload_json = excluded.payload_json,
                            updated_at_epoch = excluded.updated_at_epoch,
                            expires_at_epoch = excluded.expires_at_epoch
                        """,
                        (key, version, serialized, now, expires),
                    )
                    con.commit()
                    applied = True

        self.metrics.serving_store_latency_seconds.labels(operation="put_versioned").observe(
            time.perf_counter() - t0
        )
        if not applied:
            self.metrics.serving_store_errors_total.labels(reason="stale_version_rejected").inc()
        return applied

    def get(self, key: str, *, max_age_seconds: float | None = None) -> dict[str, Any] | None:
        t0 = time.perf_counter()
        self._assert_online()
        now = time.time()
        result: dict[str, Any] | None = None
        if self._valkey is not None:
            raw = self._valkey.hgetall(key)
            if raw:
                updated = float(raw.get("updated_at", 0.0))
                if max_age_seconds is None or (now - updated) <= max_age_seconds:
                    result = {
                        "version": int(raw["version"]),
                        "updated_at_epoch": updated,
                        "payload": json.loads(raw["payload"]),
                    }
        else:
            with sqlite3.connect(self._db_path) as con:
                row = con.execute(
                    "SELECT version, payload_json, updated_at_epoch, expires_at_epoch FROM kv_state WHERE key = ?",
                    (key,),
                ).fetchone()
                if row is not None and float(row[3]) > now:
                    updated = float(row[2])
                    if max_age_seconds is None or (now - updated) <= max_age_seconds:
                        result = {
                            "version": int(row[0]),
                            "updated_at_epoch": updated,
                            "payload": json.loads(row[1]),
                        }
        self.metrics.serving_store_latency_seconds.labels(operation="get").observe(
            time.perf_counter() - t0
        )
        return result

    def get_pricing_with_safe_fallback(self, h3_cell: str, *, max_age_seconds: float = 300.0) -> dict[str, Any]:
        """Low-latency pricing lookup returning safe 1.0x baseline fallback on outage or staleness."""
        try:
            hit = self.get(f"mkt:v1:pricing:h3:{h3_cell}", max_age_seconds=max_age_seconds)
            if hit is not None:
                return {**hit["payload"], "serving_status": "LIVE_HIT", "store_version": hit["version"]}
            return {
                "h3_cell": h3_cell,
                "pricing_multiplier": 1.0,
                "decision_status": "FALLBACK_MISSING_OR_STALE",
                "serving_status": "SAFE_FALLBACK_MISS",
                "reason_codes": ["SERVING_CACHE_MISS_FALLBACK"],
                "served_at_utc": datetime.now(UTC).isoformat(),
            }
        except ServingStoreUnavailableError:
            return {
                "h3_cell": h3_cell,
                "pricing_multiplier": 1.0,
                "decision_status": "FALLBACK_STORE_UNAVAILABLE",
                "serving_status": "SAFE_FALLBACK_OUTAGE",
                "reason_codes": ["SERVING_STORE_OUTAGE_FALLBACK"],
                "served_at_utc": datetime.now(UTC).isoformat(),
            }

    def list_by_prefix(self, prefix: str, limit: int = 500) -> list[dict[str, Any]]:
        self._assert_online()
        now = time.time()
        out: list[dict[str, Any]] = []
        if self._valkey is not None:
            for k in self._valkey.scan_iter(match=f"{prefix}*", count=200):
                item = self.get(k)
                if item:
                    out.append(item["payload"])
                    if len(out) >= limit:
                        break
        else:
            with sqlite3.connect(self._db_path) as con:
                rows = con.execute(
                    "SELECT payload_json FROM kv_state WHERE key LIKE ? AND expires_at_epoch > ? LIMIT ?",
                    (f"{prefix}%", now, limit),
                ).fetchall()
                out = [json.loads(r[0]) for r in rows]
        return out

    def wipe(self) -> None:
        self._assert_online()
        if self._valkey is not None:
            for k in self._valkey.scan_iter(match="mkt:v1:*"):
                self._valkey.delete(k)
        else:
            with sqlite3.connect(self._db_path) as con:
                con.execute("DELETE FROM kv_state")
                con.commit()

    def stats(self) -> dict[str, Any]:
        if not self._online:
            return {"mode": self.mode, "online": False, "keys": 0}
        if self._valkey is not None:
            cnt = sum(1 for _ in self._valkey.scan_iter(match="mkt:v1:*"))
        else:
            with sqlite3.connect(self._db_path) as con:
                cnt = int(con.execute("SELECT COUNT(*) FROM kv_state").fetchone()[0])
        return {"mode": self.mode, "online": True, "keys": cnt}


class StateMaterializer:
    """Consumes durable Kafka/WAL changelog topics and materializes low-latency serving state."""

    CONSUMER_GROUP = "marketplace-state-materializer"

    def __init__(
        self,
        bus: EventBus,
        registry: AvroContractRegistry,
        store: MaterializedStateStore,
    ) -> None:
        self.bus = bus
        self.registry = registry
        self.store = store

    def materialize_from_durable_log(self, *, wipe_first: bool = False) -> dict[str, Any]:
        """Materialize or completely rebuild serving store from durable changelog topics."""
        t0 = time.perf_counter()
        if wipe_first:
            self.store.wipe()

        applied = 0
        stale_rejected = 0

        for topic, prefix in (
            ("marketplace.supply.aggregates", "mkt:v1:supply:h3:"),
            ("marketplace.demand.aggregates", "mkt:v1:demand:h3:"),
        ):
            for rec in self.bus.read_range(topic):
                _, agg = self.registry.deserialize(rec.value, expected_topic=topic)
                ok = self.store.put_versioned(
                    f"{prefix}{agg['h3_cell']}",
                    int(agg["state_version"]),
                    agg,
                    ttl_seconds=3600,
                )
                if ok:
                    applied += 1
                else:
                    stale_rejected += 1

        for rec in self.bus.read_range("marketplace.pricing.decisions"):
            _, dec = self.registry.deserialize(rec.value, expected_topic="marketplace.pricing.decisions")
            ok = self.store.put_versioned(
                f"mkt:v1:pricing:h3:{dec['h3_cell']}",
                int(dec["decision_version"]),
                dec,
                ttl_seconds=3600,
            )
            if ok:
                applied += 1
            else:
                stale_rejected += 1

        return {
            "rebuilt_from_scratch": wipe_first,
            "keys_applied": applied,
            "stale_updates_rejected": stale_rejected,
            "total_live_keys": self.store.stats()["keys"],
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        }
