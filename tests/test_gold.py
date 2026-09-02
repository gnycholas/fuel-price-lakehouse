"""The gold tables: weekly prices, and what the purchase price column is worth."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from fuel_lakehouse.gold.coverage import coverage, margin
from fuel_lakehouse.gold.price import build

PRICE = DecimalType(9, 4)

SILVER_SCHEMA = StructType(
    [
        StructField("state", StringType()),
        StructField("product", StringType()),
        StructField("unit", StringType()),
        StructField("collection_week", DateType()),
        StructField("collection_date", DateType()),
        StructField("reseller_cnpj", StringType()),
        StructField("sale_price", PRICE),
        StructField("purchase_price", PRICE),
    ]
)


def silver(spark: SparkSession, *rows: dict):
    defaults = {
        "state": "SP",
        "product": "GASOLINA",
        "unit": "R$ / litro",
        "collection_week": date(2025, 3, 3),
        "collection_date": date(2025, 3, 3),
        "reseller_cnpj": "00003188000121",
        "sale_price": Decimal("5.6500"),
        "purchase_price": None,
    }
    built = []
    for row in rows:
        merged = {**defaults, **row}
        for key in ("collection_week", "collection_date"):
            if isinstance(merged[key], str):
                merged[key] = date.fromisoformat(merged[key])
        built.append(tuple(merged[f.name] for f in SILVER_SCHEMA.fields))
    return spark.createDataFrame(built, schema=SILVER_SCHEMA)


def test_units_are_never_averaged_together(spark: SparkSession) -> None:
    frame = silver(
        spark,
        {"product": "GASOLINA", "unit": "R$ / litro", "sale_price": Decimal("5.65")},
        {"product": "GLP", "unit": "R$ / 13 kg", "sale_price": Decimal("110.00")},
    )
    gold = build(frame)

    assert gold.count() == 2
    assert {r["unit"] for r in gold.collect()} == {"R$ / litro", "R$ / 13 kg"}


def test_unit_is_part_of_the_grain(spark: SparkSession) -> None:
    assert "unit" in build(silver(spark, {})).columns


def test_dispersion_travels_with_the_average(spark: SparkSession) -> None:
    frame = silver(
        spark,
        {"sale_price": Decimal("5.00"), "reseller_cnpj": "1"},
        {"sale_price": Decimal("7.00"), "reseller_cnpj": "2"},
    )
    row = build(frame).first()

    assert row["min_price"] == Decimal("5.0000")
    assert row["max_price"] == Decimal("7.0000")
    assert row["stddev_price"] is not None
    assert row["observation_count"] == 2
    assert row["reseller_count"] == 2


def test_thin_weeks_are_flagged(spark: SparkSession) -> None:
    assert build(silver(spark, {}, {})).first()["is_low_sample"]


def test_a_well_sampled_week_is_not_flagged(spark: SparkSession) -> None:
    rows = [{"reseller_cnpj": str(i), "sale_price": Decimal("5.00")} for i in range(12)]
    assert not build(silver(spark, *rows)).first()["is_low_sample"]


def test_rebuilding_gives_the_same_answer(spark: SparkSession) -> None:
    frame = silver(spark, {"sale_price": Decimal("5.00")}, {"sale_price": Decimal("7.00")})
    first = build(frame).collect()
    assert build(frame).collect() == first


def test_coverage_reports_an_empty_column_as_zero(spark: SparkSession) -> None:
    row = coverage(silver(spark, {}, {})).first()

    assert row["total_rows"] == 2
    assert row["rows_with_purchase"] == 0
    assert row["coverage_ratio"] == 0.0


def test_coverage_counts_what_is_there(spark: SparkSession) -> None:
    frame = silver(spark, {"purchase_price": Decimal("4.00")}, {})
    assert coverage(frame).first()["coverage_ratio"] == 0.5


def test_margin_carries_its_own_coverage(spark: SparkSession) -> None:
    frame = silver(
        spark,
        {"purchase_price": Decimal("4.0000"), "sale_price": Decimal("5.0000")},
        *[{"reseller_cnpj": str(i)} for i in range(9)],
    )
    row = margin(frame).first()

    assert row["avg_margin"] == Decimal("1.0000")
    assert row["coverage_ratio"] == 0.1
    assert row["is_reliable"] is False


def test_a_week_with_no_purchase_price_produces_no_row(spark: SparkSession) -> None:
    assert margin(silver(spark, {}, {})).count() == 0


def test_absent_margin_is_absence_not_zero(spark: SparkSession) -> None:
    frame = silver(
        spark,
        {"collection_week": "2025-03-03", "purchase_price": Decimal("4.00")},
        {"collection_week": "2025-03-10"},
    )
    weeks = {str(r["collection_week"]) for r in margin(frame).collect()}
    assert weeks == {"2025-03-03"}


def test_reliability_columns_are_always_present(spark: SparkSession) -> None:
    frame = silver(spark, {"purchase_price": Decimal("4.00")})
    assert {"coverage_ratio", "is_reliable"} <= set(margin(frame).columns)


def test_good_coverage_is_marked_reliable(spark: SparkSession) -> None:
    frame = silver(
        spark,
        {"purchase_price": Decimal("4.00"), "reseller_cnpj": "1"},
        {"purchase_price": Decimal("4.10"), "reseller_cnpj": "2"},
    )
    assert margin(frame).first()["is_reliable"] is True
