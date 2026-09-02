"""Weekly price per state and product.

The unit is part of the grain, not a descriptive column. Left out of the key, a
GROUP BY state, product would average litres of petrol with 13 kg cylinders of
LPG and return a number that means nothing, with nothing failing.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GRAIN = ("state", "product", "unit", "collection_week")

# Measured over 136,878 weekly cells across the series: median 89 observations,
# p10 at 14, p1 at 1. Ten is the floor, catching the bottom 7%. Below that a
# weekly average is a handful of forecourts rather than a market.
LOW_SAMPLE_BELOW = 10

# Exact percentiles do not pay for themselves at this volume.
PERCENTILE_ACCURACY = 1000


def build(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy(*GRAIN)
        .agg(
            F.round(F.avg("sale_price"), 4).alias("avg_price"),
            F.expr(f"percentile_approx(sale_price, 0.5, {PERCENTILE_ACCURACY})").alias(
                "median_price"
            ),
            F.min("sale_price").alias("min_price"),
            F.max("sale_price").alias("max_price"),
            F.round(F.stddev("sale_price"), 4).alias("stddev_price"),
            F.count("*").alias("observation_count"),
            F.countDistinct("reseller_cnpj").alias("reseller_count"),
        )
        .withColumn("is_low_sample", F.col("observation_count") < LOW_SAMPLE_BELOW)
        .withColumn("collection_year", F.year("collection_week"))
    )
