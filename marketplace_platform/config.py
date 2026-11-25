"""Environment-driven typed configuration for the Mobility Marketplace Platform."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw is not None and raw.strip() else default


@dataclass(frozen=True)
class MarketplaceConfig:
    env: str = field(default_factory=lambda: _env("MARKETPLACE_ENV", "dev"))
    execution_profile: str = field(
        default_factory=lambda: _env("MARKETPLACE_EXECUTION_PROFILE", "local_native")
    )
    transport_mode: str = field(
        default_factory=lambda: _env("MARKETPLACE_TRANSPORT_MODE", "local_wal")
    )
    serving_store_mode: str = field(
        default_factory=lambda: _env("MARKETPLACE_SERVING_STORE", "local_kv")
    )
    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    local_data_dir: Path = field(
        default_factory=lambda: Path(_env("MARKETPLACE_LOCAL_DATA_DIR", ".local_data")).resolve()
    )
    warehouse_dir: Path = field(
        default_factory=lambda: Path(
            _env("MARKETPLACE_WAREHOUSE_DIR", ".local_data/warehouse")
        ).resolve()
    )
    wal_dir: Path = field(
        default_factory=lambda: Path(_env("MARKETPLACE_WAL_DIR", ".local_data/wal")).resolve()
    )
    checkpoint_dir: Path = field(
        default_factory=lambda: Path(
            _env("MARKETPLACE_CHECKPOINT_DIR", ".local_data/checkpoints")
        ).resolve()
    )
    kv_store_path: Path = field(
        default_factory=lambda: Path(
            _env("MARKETPLACE_KV_STORE_PATH", ".local_data/serving_kv.sqlite")
        ).resolve()
    )
    lineage_dir: Path = field(
        default_factory=lambda: Path(
            _env("MARKETPLACE_LINEAGE_DIR", ".local_data/lineage")
        ).resolve()
    )
    sqlite_catalog_path: Path = field(
        default_factory=lambda: Path(
            _env("MARKETPLACE_SQLITE_CATALOG_PATH", ".local_data/iceberg_catalog.db")
        ).resolve()
    )
    # Kafka, Apicurio, Valkey, Iceberg REST (Docker mode)
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    schema_registry_url: str = field(
        default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/ccompat/v7")
    )
    valkey_host: str = field(default_factory=lambda: _env("VALKEY_HOST", "localhost"))
    valkey_port: int = field(default_factory=lambda: _env_int("VALKEY_PORT", 6379))
    valkey_password: str = field(default_factory=lambda: _env("VALKEY_PASSWORD", ""))
    iceberg_rest_uri: str = field(
        default_factory=lambda: _env("ICEBERG_REST_URI", "http://localhost:8181/catalog")
    )
    iceberg_warehouse: str = field(default_factory=lambda: _env("ICEBERG_WAREHOUSE", "marketplace"))
    s3_endpoint_url: str = field(
        default_factory=lambda: _env("S3_ENDPOINT_URL", "http://localhost:9000")
    )
    s3_access_key_id: str = field(default_factory=lambda: _env("S3_ACCESS_KEY_ID", ""))
    s3_secret_access_key: str = field(default_factory=lambda: _env("S3_SECRET_ACCESS_KEY", ""))
    # Serving API & RBAC
    api_host: str = field(default_factory=lambda: _env("MARKETPLACE_API_HOST", "127.0.0.1"))
    api_port: int = field(default_factory=lambda: _env_int("MARKETPLACE_API_PORT", 8010))
    api_admin_token: str = field(
        default_factory=lambda: _env("MARKETPLACE_API_ADMIN_TOKEN", "dev-mkt-admin-token")
    )
    api_operator_token: str = field(
        default_factory=lambda: _env("MARKETPLACE_API_OPERATOR_TOKEN", "dev-mkt-operator-token")
    )
    api_analyst_token: str = field(
        default_factory=lambda: _env("MARKETPLACE_API_ANALYST_TOKEN", "dev-mkt-analyst-token")
    )
    # Multi-resolution H3 policy
    h3_fine_res: int = 9
    h3_medium_res: int = 7
    h3_coarse_res: int = 5
    # Observability
    openlineage_url: str = field(default_factory=lambda: _env("OPENLINEAGE_URL", ""))
    openlineage_namespace: str = field(
        default_factory=lambda: _env("OPENLINEAGE_NAMESPACE", "realtime-mobility-marketplace")
    )
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))

    def ensure_local_dirs(self) -> None:
        for d in (
            self.local_data_dir,
            self.warehouse_dir,
            self.wal_dir,
            self.checkpoint_dir,
            self.lineage_dir,
            self.sqlite_catalog_path.parent,
            self.kv_store_path.parent,
        ):
            d.mkdir(parents=True, exist_ok=True)


def load_config() -> MarketplaceConfig:
    cfg = MarketplaceConfig()
    cfg.ensure_local_dirs()
    return cfg
