"""Shared test fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from fuel_lakehouse.spark import build_spark

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session scoped: a JVM per test makes the suite unusable.

    Local storage, so no MinIO needed.
    """
    session = build_spark("tests", local_storage=True)
    yield session
    session.stop()
