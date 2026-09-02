"""Quarantine and the count that keeps it honest."""

from __future__ import annotations

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from samples import GOOD

from fuel_lakehouse.bronze.ingest import EXPECTED_COLUMNS, LINEAGE_COLUMNS, add_lineage
from fuel_lakehouse.silver.contract import evaluate
from fuel_lakehouse.silver.quarantine import (
    ReconciliationError,
    reconcile,
    split,
)
from fuel_lakehouse.sources.anp import SourceFile

SOURCE = SourceFile("dsan", None, 2025, "03", "glp", "csv", "https://x/precos-glp-03.csv")


def bronze(spark: SparkSession, *overrides: dict[str, str]):
    rows = [Row(**{c: {**GOOD, **o}[c] for c in EXPECTED_COLUMNS}) for o in overrides]
    return add_lineage(spark.createDataFrame(rows), SOURCE, run_id="run-1")


def test_good_rows_go_to_accepted(spark: SparkSession) -> None:
    result = split(evaluate(bronze(spark, {}, {})))
    assert result.accepted.count() == 2
    assert result.rejected.count() == 0


def test_bad_row_keeps_its_original_value(spark: SparkSession) -> None:
    result = split(evaluate(bronze(spark, {"valor_de_venda": "ABC"})))
    row = result.rejected.first()

    assert row["valor_de_venda"] == "ABC"
    assert "sale_price_not_numeric" in row["_rejection_reasons"]
    assert row["_rejected_at"] is not None


def test_rejected_row_keeps_its_lineage(spark: SparkSession) -> None:
    result = split(evaluate(bronze(spark, {"produto": "XPTO"})))
    row = result.rejected.first()

    assert row["_source_file"] == "precos-glp-03.csv"
    assert row["_ingestion_run_id"] == "run-1"


def test_reasons_accumulate(spark: SparkSession) -> None:
    result = split(evaluate(bronze(spark, {"estado_sigla": "XX", "valor_de_venda": "-1,00"})))
    reasons = set(result.rejected.first()["_rejection_reasons"])

    assert {"unknown_state", "sale_price_not_positive"} <= reasons


def test_accepted_side_carries_the_typed_columns(spark: SparkSession) -> None:
    row = split(evaluate(bronze(spark, {}))).accepted.first()
    assert row["reseller_cnpj"] == "00003188000121"
    assert str(row["collection_date"]) == "2025-03-03"


def test_reconciliation_balances(spark: SparkSession) -> None:
    source = bronze(spark, {}, {"valor_de_venda": "ABC"}, {"produto": "XPTO"})
    result = reconcile(source.count(), split(evaluate(source)))

    assert result.balances
    assert (result.accepted, result.rejected) == (1, 2)


def test_reconciliation_catches_a_silent_drop(spark: SparkSession) -> None:
    """The test that makes the safety net worth having: break the split on
    purpose and check the count notices."""
    source = bronze(spark, {}, {}, {"valor_de_venda": "ABC"})
    broken = split(evaluate(source))
    sabotaged = broken.__class__(
        accepted=broken.accepted.limit(1),
        rejected=broken.rejected,
    )

    with pytest.raises(ReconciliationError, match="1 row"):
        reconcile(source.count(), sabotaged)


def test_reconciliation_message_names_the_numbers(spark: SparkSession) -> None:
    source = bronze(spark, {}, {})
    empty = split(evaluate(source))
    sabotaged = empty.__class__(
        accepted=empty.accepted.filter(F.lit(False)), rejected=empty.rejected
    )

    with pytest.raises(ReconciliationError) as err:
        reconcile(source.count(), sabotaged)
    assert "2 in bronze" in str(err.value)


def test_no_column_is_lost_on_the_rejected_side(spark: SparkSession) -> None:
    rejected = split(evaluate(bronze(spark, {"produto": "XPTO"}))).rejected
    assert set(EXPECTED_COLUMNS) <= set(rejected.columns)
    assert set(LINEAGE_COLUMNS) <= set(rejected.columns)
