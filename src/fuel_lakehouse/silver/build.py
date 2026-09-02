"""Writing the accepted rows into silver.

Merge on the natural key rather than overwrite, so rerunning a window lands on
the same content instead of duplicating it.
"""

from __future__ import annotations

from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Measured unique across the series: 281,531 rows gave 281,531 keys in the 2004
# file, 51,685 gave 51,685 in a 2025 one.
NATURAL_KEY = ("reseller_cnpj", "product", "collection_date")

PARTITION_BY = ("collection_year",)


def prepare(accepted: DataFrame, *, updated_at: datetime | None = None) -> DataFrame:
    stamped = updated_at or datetime.now(UTC)
    return accepted.select(
        "*",
        F.year("collection_date").alias("collection_year"),
        F.lit(stamped).cast("timestamp").alias("_updated_at"),
    )


def merge(spark: SparkSession, prepared: DataFrame, table_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, table_path):
        prepared.write.format("delta").partitionBy(*PARTITION_BY).save(table_path)
        return

    condition = " AND ".join(f"t.{column} <=> s.{column}" for column in NATURAL_KEY)
    (
        DeltaTable.forPath(spark, table_path)
        .alias("t")
        .merge(prepared.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
