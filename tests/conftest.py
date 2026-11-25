"""Shared pytest fixtures providing isolated per-test storage directories."""

from __future__ import annotations

from pathlib import Path

import pytest

from marketplace_platform.config import MarketplaceConfig


@pytest.fixture
def isolated_config(tmp_path: Path) -> MarketplaceConfig:
    data_dir = tmp_path / "data"
    cfg = MarketplaceConfig(
        env="test",
        execution_profile="local_native",
        transport_mode="local_wal",
        serving_store_mode="local_kv",
        local_data_dir=data_dir,
        warehouse_dir=data_dir / "warehouse",
        wal_dir=data_dir / "wal",
        checkpoint_dir=data_dir / "checkpoints",
        kv_store_path=data_dir / "serving_kv.sqlite",
        lineage_dir=data_dir / "lineage",
        sqlite_catalog_path=data_dir / "iceberg_catalog.db",
    )
    cfg.ensure_local_dirs()
    return cfg
