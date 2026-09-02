"""Configuration from the environment, with defaults for local development."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class StorageConfig:
    """Object store connection and the lakehouse buckets."""

    endpoint: str
    access_key: str
    secret_key: str
    bucket_bronze: str
    bucket_silver: str
    bucket_gold: str

    @property
    def bronze_raw(self) -> str:
        """Where downloaded files land, byte for byte as published."""
        return f"s3a://{self.bucket_bronze}/_raw"

    def table(self, layer: str, name: str) -> str:
        buckets = {
            "bronze": self.bucket_bronze,
            "silver": self.bucket_silver,
            "gold": self.bucket_gold,
        }
        if layer not in buckets:
            raise ValueError(f"unknown layer: {layer!r}")
        return f"s3a://{buckets[layer]}/{name}"


@dataclass(frozen=True)
class Config:
    storage: StorageConfig
    java_home: str | None


@lru_cache(maxsize=1)
def load_config() -> Config:
    return Config(
        storage=StorageConfig(
            endpoint=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
            access_key=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
            bucket_bronze=os.environ.get("BUCKET_BRONZE", "bronze"),
            bucket_silver=os.environ.get("BUCKET_SILVER", "silver"),
            bucket_gold=os.environ.get("BUCKET_GOLD", "gold"),
        ),
        java_home=os.environ.get("JAVA_HOME"),
    )
