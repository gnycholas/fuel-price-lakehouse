"""Merging into silver, and the property that makes reruns safe."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Row, SparkSession
from samples import GOOD

from fuel_lakehouse.bronze.ingest import EXPECTED_COLUMNS, add_lineage
from fuel_lakehouse.silver.build import merge, prepare
from fuel_lakehouse.silver.contract import evaluate
from fuel_lakehouse.silver.quarantine import split
from fuel_lakehouse.sources.anp import SourceFile

SOURCE = SourceFile("dsan", None, 2025, "03", "glp", "csv", "https://x/precos-glp-03.csv")


def accepted(spark: SparkSession, *overrides: dict[str, str]):
    rows = [Row(**{c: {**GOOD, **o}[c] for c in EXPECTED_COLUMNS}) for o in overrides]
    bronze = add_lineage(spark.createDataFrame(rows), SOURCE, run_id="r1")
    return prepare(split(evaluate(bronze)).accepted)


def content(spark: SparkSession, path: str) -> set[tuple]:
    frame = spark.read.format("delta").load(path)
    return {
        (r["reseller_cnpj"], r["product"], str(r["collection_date"]), r["sale_price"])
        for r in frame.collect()
    }


def test_first_write_creates_the_table(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "silver")
    merge(spark, accepted(spark, {}), path)

    assert spark.read.format("delta").load(path).count() == 1


def test_rerun_produces_identical_content(spark: SparkSession, tmp_path: Path) -> None:
    """Counting alone would pass with the same number of wrong rows."""
    path = str(tmp_path / "silver")
    merge(spark, accepted(spark, {}, {"produto": "ETANOL"}), path)
    before = content(spark, path)

    merge(spark, accepted(spark, {}, {"produto": "ETANOL"}), path)

    assert content(spark, path) == before
    assert spark.read.format("delta").load(path).count() == 2


def test_a_new_window_leaves_the_old_rows_alone(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "silver")
    merge(spark, accepted(spark, {}), path)
    merge(spark, accepted(spark, {"data_da_coleta": "07/04/2025"}), path)

    dates = {str(r["collection_date"]) for r in spark.read.format("delta").load(path).collect()}
    assert dates == {"2025-03-03", "2025-04-07"}


def test_a_republished_row_updates_in_place(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "silver")
    merge(spark, accepted(spark, {}), path)
    merge(spark, accepted(spark, {"valor_de_venda": "5,68"}), path)

    frame = spark.read.format("delta").load(path)
    assert frame.count() == 1
    assert str(frame.first()["sale_price"]) == "5.6800"


def test_partitioned_by_collection_year(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "silver")
    merge(spark, accepted(spark, {}), path)

    assert spark.read.format("delta").load(path).first()["collection_year"] == 2025


def test_updated_at_is_stamped(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "silver")
    merge(spark, accepted(spark, {}), path)

    assert spark.read.format("delta").load(path).first()["_updated_at"] is not None
