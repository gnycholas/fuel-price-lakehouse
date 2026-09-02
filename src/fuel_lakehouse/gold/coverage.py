"""How much of the purchase price actually exists, and what that does to margin.

The column is declared for the whole series but stops being filled: 63% of rows
carry it in 2004, 22% by 2020, none at all from 2021 on. A margin built on that
without saying so is a wrong number that looks perfectly normal.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Below this share of rows carrying a purchase price, a margin says more about
# who bothered to report than about the market. Measured over the 112,125 cells
# that have any margin at all: p25 0.25, median 0.43, p75 0.61. Half the cell
# reporting is the line, and 41% of cells clear it.
RELIABLE_ABOVE = 0.5


def coverage(silver: DataFrame) -> DataFrame:
    return (
        silver.withColumn("collection_year", F.year("collection_date"))
        .groupBy("collection_year", "product", "state")
        .agg(
            F.count("*").alias("total_rows"),
            F.count("purchase_price").alias("rows_with_purchase"),
        )
        .withColumn(
            "coverage_ratio",
            F.round(F.col("rows_with_purchase") / F.col("total_rows"), 6),
        )
    )


def margin(silver: DataFrame) -> DataFrame:
    """Weekly margin, always carrying the coverage that backs it.

    Weeks with no purchase price produce no row at all. A zero margin would
    drag every average down while looking like real data.
    """
    priced = silver.filter(F.col("purchase_price").isNotNull())

    per_week = silver.groupBy("state", "product", "unit", "collection_week").agg(
        F.count("*").alias("total_rows"),
        F.count("purchase_price").alias("rows_with_purchase"),
    )

    margins = priced.groupBy("state", "product", "unit", "collection_week").agg(
        F.round(F.avg(F.col("sale_price") - F.col("purchase_price")), 4).alias("avg_margin"),
        F.round(
            F.avg((F.col("sale_price") - F.col("purchase_price")) / F.col("sale_price")), 6
        ).alias("avg_margin_ratio"),
    )

    return (
        margins.join(per_week, on=["state", "product", "unit", "collection_week"])
        .withColumn(
            "coverage_ratio",
            F.round(F.col("rows_with_purchase") / F.col("total_rows"), 6),
        )
        .withColumn("is_reliable", F.col("coverage_ratio") >= RELIABLE_ABOVE)
        .withColumn("collection_year", F.year("collection_week"))
    )
