"""First-class observability: structured JSON logs with marketplace entity IDs & secret redaction,
Prometheus metrics, OpenTelemetry tracing, and OpenLineage v2 run-event emission.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_SENSITIVE_KEY_RE = re.compile(
    r"(token|secret|password|authorization|api_key|credential|cookie|private_key)", re.IGNORECASE
)
_OTEL_INIT = False
_OL_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
_OL_PRODUCER = "https://github.com/realtime-mobility-marketplace-platform/openlineage/0.1.0"


def _scrub(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: ("[REDACTED]" if _SENSITIVE_KEY_RE.search(str(k)) else _scrub(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_scrub(x) for x in obj]
    if isinstance(obj, str) and len(obj) > 512:
        return obj[:512] + "...[TRUNCATED]"
    return obj


scrub_secrets = _scrub


class MarketplaceLogger:
    """Structured JSON logger emitting mandatory Section 21 fields."""

    def __init__(self, service: str, component: str, environment: str | None = None) -> None:
        self.service = service
        self.component = component
        self.environment = environment or os.environ.get("MARKETPLACE_ENV", "dev")
        lvl = os.environ.get("LOG_LEVEL", "INFO").upper()
        self._level = getattr(logging, lvl, logging.INFO)

    def emit(
        self,
        severity: str,
        message: str,
        *,
        event_type: str = "log",
        trace_id: str | None = None,
        correlation_id: str | None = None,
        request_id: str | None = None,
        driver_id: str | None = None,
        trip_id: str | None = None,
        pipeline_run_id: str | None = None,
        error_code: str = "NONE",
        retry_count: int = 0,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sev = severity.upper()
        rec: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": self.service,
            "environment": self.environment,
            "severity": sev,
            "trace_id": trace_id or uuid.uuid4().hex,
            "correlation_id": correlation_id or uuid.uuid4().hex[:16],
            "request_id": request_id,
            "driver_id": driver_id,
            "trip_id": trip_id,
            "pipeline_run_id": pipeline_run_id or "none",
            "component": self.component,
            "event_type": event_type,
            "error_code": error_code,
            "retry_count": retry_count,
            "message": message,
            "context": _scrub(context or {}),
        }
        if getattr(logging, sev, logging.INFO) >= self._level:
            stream = sys.stderr if sev in ("WARNING", "ERROR", "CRITICAL") else sys.stdout
            print(json.dumps(rec, separators=(",", ":"), default=str), file=stream, flush=True)
        return rec

    def info(self, event_type: str, message: str, **kw: Any) -> dict[str, Any]:
        return self.emit("INFO", message, event_type=event_type, **kw)

    def warning(self, event_type: str, message: str, **kw: Any) -> dict[str, Any]:
        return self.emit("WARNING", message, event_type=event_type, **kw)

    def error(self, event_type: str, message: str, **kw: Any) -> dict[str, Any]:
        return self.emit("ERROR", message, event_type=event_type, **kw)

    def audit(self, event_type: str, message: str, **kw: Any) -> dict[str, Any]:
        ctx = dict(kw.pop("context", None) or {})
        ctx["audit"] = True
        return self.emit("INFO", message, event_type=event_type, context=ctx, **kw)


class MarketplaceMetrics:
    """Prometheus instrumentation for simulator, stream state, pricing, serving store, lakehouse, and API."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.events_produced_total = Counter(
            "marketplace_events_produced_total",
            "Total synthetic marketplace events produced",
            ["topic", "event_type"],
            registry=self.registry,
        )
        self.dlq_messages_total = Counter(
            "marketplace_dlq_messages_total",
            "Total invalid/poison messages quarantined to marketplace.dead-letter",
            ["source_topic", "error_code"],
            registry=self.registry,
        )
        self.stream_events_processed_total = Counter(
            "marketplace_stream_events_processed_total",
            "Events processed by stateful marketplace processor",
            ["domain", "status"],
            registry=self.registry,
        )
        self.pricing_decisions_total = Counter(
            "marketplace_pricing_decisions_total",
            "Pricing decisions emitted by status and policy version",
            ["policy_version", "decision_status"],
            registry=self.registry,
        )
        self.pricing_decision_latency_seconds = Histogram(
            "marketplace_pricing_decision_latency_seconds",
            "Dynamic pricing calculation latency per H3 cell",
            buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1),
            registry=self.registry,
        )
        self.serving_store_latency_seconds = Histogram(
            "marketplace_serving_store_latency_seconds",
            "Low-latency state store read/write latency",
            ["operation"],
            buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1),
            registry=self.registry,
        )
        self.serving_store_errors_total = Counter(
            "marketplace_serving_store_errors_total",
            "Serving state store errors or stale version rejections",
            ["reason"],
            registry=self.registry,
        )
        self.active_drivers_gauge = Gauge(
            "marketplace_active_drivers_total",
            "Current available drivers tracked by state engine",
            ["city_id"],
            registry=self.registry,
        )
        self.active_demand_gauge = Gauge(
            "marketplace_active_requests_total",
            "Current active rider requests tracked by state engine",
            ["city_id"],
            registry=self.registry,
        )
        self.checkpoint_duration_seconds = Histogram(
            "marketplace_checkpoint_duration_seconds",
            "Streaming state checkpoint save latency",
            registry=self.registry,
        )
        self.reconciliation_drift_total = Counter(
            "marketplace_reconciliation_drift_corrected_total",
            "State inconsistencies detected and corrected by reconciliation",
            ["entity_type"],
            registry=self.registry,
        )
        self.dq_checks_total = Counter(
            "marketplace_dq_checks_total",
            "Data quality check outcomes",
            ["check_name", "status"],
            registry=self.registry,
        )
        self.api_requests_total = Counter(
            "marketplace_api_requests_total",
            "HTTP requests served by Marketplace FastAPI",
            ["endpoint", "method", "status_code", "role"],
            registry=self.registry,
        )
        self.api_latency_seconds = Histogram(
            "marketplace_api_request_duration_seconds",
            "API request latency in seconds",
            ["endpoint"],
            registry=self.registry,
        )

    def render_prometheus(self) -> bytes:
        return generate_latest(self.registry)


class OpenLineageEmitter:
    def __init__(
        self,
        namespace: str = "realtime-mobility-marketplace",
        marquez_url: str = "",
        local_store_dir: Path | None = None,
    ) -> None:
        self.namespace = namespace
        self.marquez_url = marquez_url.rstrip("/") if marquez_url else ""
        self.local_store_dir = local_store_dir
        if local_store_dir is not None:
            local_store_dir.mkdir(parents=True, exist_ok=True)

    def emit(
        self,
        *,
        event_type: str,
        job_name: str,
        run_id: str | None = None,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "eventType": event_type.upper(),
            "eventTime": datetime.now(UTC).isoformat(),
            "run": {"runId": run_id or str(uuid.uuid4()), "facets": {}},
            "job": {"namespace": self.namespace, "name": job_name, "facets": {}},
            "inputs": [{"namespace": self.namespace, "name": i["name"], "facets": {}} for i in (inputs or [])],
            "outputs": [
                {
                    "namespace": self.namespace,
                    "name": o["name"],
                    "facets": (
                        {"outputStatistics": {"rowCount": int(o["row_count"])}}
                        if "row_count" in o
                        else {}
                    ),
                }
                for o in (outputs or [])
            ],
            "producer": _OL_PRODUCER,
            "schemaURL": _OL_SCHEMA,
        }
        if self.local_store_dir is not None:
            p = self.local_store_dir / "openlineage_events.jsonl"
            with p.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, default=str) + "\n")
        if self.marquez_url:
            try:
                requests.post(f"{self.marquez_url}/api/v1/lineage", json=payload, timeout=1.5)
            except Exception:
                pass
        return payload

    def read_local_events(self, limit: int = 50) -> list[dict[str, Any]]:
        if self.local_store_dir is None:
            return []
        p = self.local_store_dir / "openlineage_events.jsonl"
        if not p.exists():
            return []
        return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines()[-limit:] if line.strip()]


_DEFAULT_METRICS: MarketplaceMetrics | None = None


def get_logger(service: str, component: str, environment: str | None = None) -> MarketplaceLogger:
    return MarketplaceLogger(service=service, component=component, environment=environment)


def get_metrics() -> MarketplaceMetrics:
    global _DEFAULT_METRICS
    if _DEFAULT_METRICS is None:
        _DEFAULT_METRICS = MarketplaceMetrics()
    return _DEFAULT_METRICS


def get_tracer(service_name: str = "realtime-mobility-marketplace") -> trace.Tracer:
    global _OTEL_INIT
    if not _OTEL_INIT:
        trace.set_tracer_provider(
            TracerProvider(resource=Resource.create({"service.name": service_name}))
        )
        _OTEL_INIT = True
    return trace.get_tracer(service_name)


def new_trace_context() -> tuple[str, str]:
    return uuid.uuid4().hex, uuid.uuid4().hex[:16]
