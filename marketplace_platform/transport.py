"""Partitioned event transport supporting Apache Kafka 4.3.2 (KRaft) and local WAL fallback."""

from __future__ import annotations

import json
import struct
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import mmh3

from marketplace_platform.config import MarketplaceConfig
from marketplace_platform.contracts import TOPIC_SPECS

_FRAME = struct.Struct(">QqIII")


class BrokerUnavailableError(RuntimeError):
    """Raised during controlled broker outage injection."""


def murmur2_partition(key: str, num_partitions: int) -> int:
    if num_partitions <= 1:
        return 0
    return int((mmh3.hash(key.encode("utf-8"), seed=0, signed=False) & 0x7FFFFFFF) % num_partitions)


@dataclass
class TransportRecord:
    topic: str
    partition: int
    offset: int
    key: str
    value: bytes
    timestamp_ms: int
    headers: dict[str, str] = field(default_factory=dict)


class EventBus(ABC):
    backend_name: str = "abstract"

    @abstractmethod
    def ensure_topics(self) -> None: ...

    @abstractmethod
    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        *,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> TransportRecord: ...

    @abstractmethod
    def produce_batch(
        self,
        topic: str,
        items: Sequence[tuple[str, bytes, int | None, dict[str, str] | None]],
    ) -> int: ...

    @abstractmethod
    def consume_batch(
        self,
        topic: str,
        consumer_group: str,
        *,
        max_records: int = 10_000,
        auto_commit: bool = False,
    ) -> list[TransportRecord]: ...

    @abstractmethod
    def commit_offsets(
        self, topic: str, consumer_group: str, partition_offsets: dict[int, int]
    ) -> None: ...

    @abstractmethod
    def get_consumer_lag(self, topic: str, consumer_group: str) -> dict[str, Any]: ...

    @abstractmethod
    def read_range(
        self,
        topic: str,
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        max_records: int = 500_000,
    ) -> list[TransportRecord]: ...

    @abstractmethod
    def topic_stats(self) -> dict[str, dict[str, Any]]: ...


class LocalWalEventBus(EventBus):
    """Disk-backed partitioned Write-Ahead Log fallback with identical Murmur2 partitioning & offsets."""

    backend_name = "local_wal"

    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir.resolve()
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self._offsets_dir = self.root_dir / "_offsets"
        self._offsets_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._online = True
        self._parts: dict[str, int] = {}
        self._next: dict[tuple[str, int], int] = {}
        self.ensure_topics()

    def set_broker_online(self, online: bool) -> None:
        with self._lock:
            self._online = online

    def _pfile(self, topic: str, part: int) -> Path:
        return self.root_dir / topic / f"partition-{part:03d}.wal"

    def _ofile(self, group: str, topic: str) -> Path:
        d = self._offsets_dir / group
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{topic}.json"

    def _count(self, path: Path) -> int:
        if not path.exists() or path.stat().st_size == 0:
            return 0
        c = 0
        with path.open("rb") as fh:
            while True:
                hdr = fh.read(_FRAME.size)
                if len(hdr) < _FRAME.size:
                    break
                _, _, kl, hl, vl = _FRAME.unpack(hdr)
                fh.seek(kl + hl + vl, 1)
                c += 1
        return c

    def ensure_topics(self) -> None:
        with self._lock:
            for name, spec in TOPIC_SPECS.items():
                (self.root_dir / name).mkdir(parents=True, exist_ok=True)
                self._parts[name] = spec.partitions
                for p in range(spec.partitions):
                    pf = self._pfile(name, p)
                    if not pf.exists():
                        pf.touch()
                    self._next[(name, p)] = self._count(pf)

    def _load_offsets(self, group: str, topic: str) -> dict[int, int]:
        f = self._ofile(group, topic)
        if not f.exists():
            return {}
        return {int(k): int(v) for k, v in json.loads(f.read_text(encoding="utf-8")).items()}

    def _save_offsets(self, group: str, topic: str, offsets: dict[int, int]) -> None:
        f = self._ofile(group, topic)
        tmp = f.with_suffix(".tmp")
        tmp.write_text(json.dumps({str(k): int(v) for k, v in offsets.items()}), encoding="utf-8")
        tmp.replace(f)

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        *,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> TransportRecord:
        with self._lock:
            if not self._online:
                raise BrokerUnavailableError("Local WAL broker offline (fault injection)")
            part = murmur2_partition(key, self._parts[topic])
            off = self._next[(topic, part)]
            ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
            kb = key.encode("utf-8")
            hb = json.dumps(headers or {}, separators=(",", ":")).encode("utf-8")
            with self._pfile(topic, part).open("ab") as fh:
                fh.write(_FRAME.pack(off, ts, len(kb), len(hb), len(value)) + kb + hb + value)
            self._next[(topic, part)] = off + 1
            return TransportRecord(topic, part, off, key, value, ts, headers or {})

    def produce_batch(
        self,
        topic: str,
        items: Sequence[tuple[str, bytes, int | None, dict[str, str] | None]],
    ) -> int:
        if not items:
            return 0
        with self._lock:
            if not self._online:
                raise BrokerUnavailableError("Local WAL broker offline (fault injection)")
            nparts = self._parts[topic]
            now_ms = int(time.time() * 1000)
            bufs: dict[int, bytearray] = {}
            for key, val, ts_opt, hdr_opt in items:
                p = murmur2_partition(key, nparts)
                off = self._next[(topic, p)]
                ts = ts_opt if ts_opt is not None else now_ms
                kb = key.encode("utf-8")
                hb = json.dumps(hdr_opt, separators=(",", ":")).encode("utf-8") if hdr_opt else b"{}"
                b = bufs.setdefault(p, bytearray())
                b += _FRAME.pack(off, ts, len(kb), len(hb), len(val)) + kb + hb + val
                self._next[(topic, p)] = off + 1
            for p, data in bufs.items():
                with self._pfile(topic, p).open("ab") as fh:
                    fh.write(data)
            return len(items)

    def _read_part(self, topic: str, part: int, start_off: int, limit: int) -> list[TransportRecord]:
        pf = self._pfile(topic, part)
        if not pf.exists() or pf.stat().st_size == 0 or limit <= 0:
            return []
        out: list[TransportRecord] = []
        with pf.open("rb") as fh:
            while len(out) < limit:
                hdr = fh.read(_FRAME.size)
                if len(hdr) < _FRAME.size:
                    break
                off, ts, kl, hl, vl = _FRAME.unpack(hdr)
                if off < start_off:
                    fh.seek(kl + hl + vl, 1)
                    continue
                kb, hb, vb = fh.read(kl), fh.read(hl), fh.read(vl)
                out.append(
                    TransportRecord(
                        topic,
                        part,
                        off,
                        kb.decode("utf-8"),
                        vb,
                        ts,
                        json.loads(hb.decode("utf-8")) if hl else {},
                    )
                )
        return out

    def consume_batch(
        self,
        topic: str,
        consumer_group: str,
        *,
        max_records: int = 10_000,
        auto_commit: bool = False,
    ) -> list[TransportRecord]:
        with self._lock:
            if not self._online:
                raise BrokerUnavailableError("Local WAL broker offline (fault injection)")
            committed = self._load_offsets(consumer_group, topic)
            new_off = dict(committed)
            collected: list[TransportRecord] = []
            rem = max_records
            for p in range(self._parts[topic]):
                if rem <= 0:
                    break
                recs = self._read_part(topic, p, committed.get(p, 0), rem)
                if recs:
                    collected.extend(recs)
                    new_off[p] = recs[-1].offset + 1
                    rem -= len(recs)
            if auto_commit and collected:
                self._save_offsets(consumer_group, topic, new_off)
            return collected

    def commit_offsets(
        self, topic: str, consumer_group: str, partition_offsets: dict[int, int]
    ) -> None:
        with self._lock:
            cur = self._load_offsets(consumer_group, topic)
            for p, off in partition_offsets.items():
                cur[int(p)] = max(cur.get(int(p), 0), int(off))
            self._save_offsets(consumer_group, topic, cur)

    def get_consumer_lag(self, topic: str, consumer_group: str) -> dict[str, Any]:
        with self._lock:
            committed = self._load_offsets(consumer_group, topic)
            total = 0
            per_part: dict[int, dict[str, int]] = {}
            for p in range(self._parts[topic]):
                hwm = self._next.get((topic, p), 0)
                cur = committed.get(p, 0)
                lag = max(0, hwm - cur)
                per_part[p] = {"committed": cur, "high_watermark": hwm, "lag": lag}
                total += lag
            return {"topic": topic, "consumer_group": consumer_group, "total_lag": total, "partitions": per_part}

    def read_range(
        self,
        topic: str,
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        max_records: int = 500_000,
    ) -> list[TransportRecord]:
        with self._lock:
            if not self._online:
                raise BrokerUnavailableError("Local WAL broker offline")
            out: list[TransportRecord] = []
            for p in range(self._parts[topic]):
                for r in self._read_part(topic, p, 0, max_records - len(out)):
                    if start_ts_ms is not None and r.timestamp_ms < start_ts_ms:
                        continue
                    if end_ts_ms is not None and r.timestamp_ms > end_ts_ms:
                        continue
                    out.append(r)
                    if len(out) >= max_records:
                        return out
            return out

    def topic_stats(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            res: dict[str, dict[str, Any]] = {}
            for name, nparts in self._parts.items():
                recs = sum(self._next.get((name, p), 0) for p in range(nparts))
                sz = sum(self._pfile(name, p).stat().st_size for p in range(nparts) if self._pfile(name, p).exists())
                per_p = [self._next.get((name, p), 0) for p in range(nparts)]
                res[name] = {
                    "partitions": nparts,
                    "records": recs,
                    "bytes": sz,
                    "max_partition_records": max(per_p) if per_p else 0,
                    "min_partition_records": min(per_p) if per_p else 0,
                    "backend": self.backend_name,
                }
            return res


class KafkaEventBus(EventBus):
    """Production Apache Kafka 4.3.2 transport using confluent-kafka (librdkafka)."""

    backend_name = "kafka"

    def __init__(self, bootstrap_servers: str) -> None:
        from confluent_kafka import Producer

        self.bootstrap_servers = bootstrap_servers
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "zstd",
                "linger.ms": 10,
                "batch.size": 65536,
                "retries": 5,
            }
        )

    def ensure_topics(self) -> None:
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        existing = set(admin.list_topics(timeout=10).topics.keys())
        to_create = [
            NewTopic(
                s.name,
                num_partitions=s.partitions,
                replication_factor=1,
                config={"retention.ms": str(s.retention_ms), "compression.type": s.compression, "cleanup.policy": s.cleanup_policy},
            )
            for s in TOPIC_SPECS.values()
            if s.name not in existing
        ]
        if to_create:
            for fut in admin.create_topics(to_create).values():
                try:
                    fut.result(timeout=15)
                except Exception as exc:
                    if "TOPIC_ALREADY_EXISTS" not in str(exc):
                        raise

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        *,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> TransportRecord:
        part = murmur2_partition(key, TOPIC_SPECS[topic].partitions)
        ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        hdrs: list[tuple[str, str | bytes | None]] = [(k, v.encode()) for k, v in (headers or {}).items()]
        self._producer.produce(topic=topic, key=key.encode(), value=value, partition=part, timestamp=ts, headers=hdrs)
        self._producer.flush(10)
        return TransportRecord(topic, part, -1, key, value, ts, headers or {})

    def produce_batch(
        self,
        topic: str,
        items: Sequence[tuple[str, bytes, int | None, dict[str, str] | None]],
    ) -> int:
        nparts = TOPIC_SPECS[topic].partitions
        now_ms = int(time.time() * 1000)
        for key, val, ts_opt, hdr_opt in items:
            hdrs: list[tuple[str, str | bytes | None]] = [(k, v.encode()) for k, v in (hdr_opt or {}).items()]
            self._producer.produce(
                topic=topic,
                key=key.encode(),
                value=val,
                partition=murmur2_partition(key, nparts),
                timestamp=ts_opt if ts_opt is not None else now_ms,
                headers=hdrs,
            )
            self._producer.poll(0)
        self._producer.flush(30)
        return len(items)

    def consume_batch(
        self, topic: str, consumer_group: str, *, max_records: int = 10_000, auto_commit: bool = False
    ) -> list[TransportRecord]:
        from confluent_kafka import Consumer

        c = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )
        c.subscribe([topic])
        out: list[TransportRecord] = []
        empty = 0
        try:
            while len(out) < max_records and empty < 3:
                msg = c.poll(1.0)
                if msg is None:
                    empty += 1
                    continue
                if msg.error():
                    continue
                out.append(
                    TransportRecord(
                        str(msg.topic() or topic),
                        int(msg.partition() or 0),
                        int(msg.offset() or 0),
                        (msg.key() or b"").decode("utf-8"),
                        msg.value() or b"",
                        int(msg.timestamp()[1]),
                    )
                )
            if auto_commit and out:
                c.commit(asynchronous=False)
        finally:
            c.close()
        return out

    def commit_offsets(
        self, topic: str, consumer_group: str, partition_offsets: dict[int, int]
    ) -> None:
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer({"bootstrap.servers": self.bootstrap_servers, "group.id": consumer_group})
        try:
            c.commit(
                offsets=[TopicPartition(topic, int(p), int(o)) for p, o in partition_offsets.items()],
                asynchronous=False,
            )
        finally:
            c.close()

    def get_consumer_lag(self, topic: str, consumer_group: str) -> dict[str, Any]:
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer({"bootstrap.servers": self.bootstrap_servers, "group.id": consumer_group})
        try:
            parts = list(c.list_topics(topic, timeout=10).topics[topic].partitions.keys())
            committed = c.committed([TopicPartition(topic, p) for p in parts], timeout=10)
            total = 0
            per_part: dict[int, dict[str, int]] = {}
            for tp in committed:
                _, hwm = c.get_watermark_offsets(tp, timeout=10)
                cur = max(0, tp.offset)
                lag = max(0, hwm - cur)
                per_part[tp.partition] = {"committed": cur, "high_watermark": hwm, "lag": lag}
                total += lag
            return {"topic": topic, "consumer_group": consumer_group, "total_lag": total, "partitions": per_part}
        finally:
            c.close()

    def read_range(
        self,
        topic: str,
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        max_records: int = 500_000,
    ) -> list[TransportRecord]:
        return self.consume_batch(topic, f"replay-{int(time.time() * 1000)}", max_records=max_records)

    def topic_stats(self) -> dict[str, dict[str, Any]]:
        return {name: {"partitions": s.partitions, "records": -1, "bytes": -1, "backend": self.backend_name} for name, s in TOPIC_SPECS.items()}


def create_event_bus(config: MarketplaceConfig) -> EventBus:
    if config.transport_mode == "kafka":
        return KafkaEventBus(config.kafka_bootstrap_servers)
    return LocalWalEventBus(config.wal_dir)
