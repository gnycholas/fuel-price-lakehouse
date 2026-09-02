"""Configuration is environment driven, with no machine specific defaults."""

from __future__ import annotations

import pytest

from fuel_lakehouse.config import StorageConfig


def storage(**overrides: str) -> StorageConfig:
    defaults = {
        "endpoint": "http://localhost:9000",
        "access_key": "key",
        "secret_key": "secret",
        "bucket_bronze": "bronze",
        "bucket_silver": "silver",
        "bucket_gold": "gold",
    }
    return StorageConfig(**{**defaults, **overrides})  # type: ignore[arg-type]


def test_table_path_uses_the_bucket_of_its_layer() -> None:
    cfg = storage()
    assert cfg.table("silver", "price_observation") == "s3a://silver/price_observation"


def test_unknown_layer_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown layer"):
        storage().table("platinum", "whatever")


def test_raw_prefix_lives_under_bronze() -> None:
    assert storage(bucket_bronze="b").bronze_raw == "s3a://b/_raw"
