"""Bronze ingestion, over a fixture cut from the real files.

The fixture keeps the awkward parts: BOM, CRLF, a semicolon inside quotes, an
empty purchase price, a CNPJ with a leading space.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fuel_lakehouse.bronze.ingest import (
    EXPECTED_COLUMNS,
    UnexpectedHeaderError,
    add_lineage,
    header_signature,
    normalize_column,
    read_raw,
    write_bronze,
)
from fuel_lakehouse.sources.anp import SourceFile

SAMPLE = str(Path(__file__).parent / "fixtures" / "anp_sample.csv")

SOURCE = SourceFile(
    "dsan", None, 2025, "03", "glp", "csv", "https://x/arquivos/shpc/dsan/2025/precos-glp-03.csv"
)


@pytest.fixture(scope="module")
def raw(spark: SparkSession):
    return read_raw(spark, SAMPLE)


def test_bom_is_stripped_from_column_name() -> None:
    assert normalize_column("﻿Regiao - Sigla") == "regiao_sigla"


def test_accents_and_punctuation_are_normalized() -> None:
    assert normalize_column("Unidade de Medida") == "unidade_de_medida"
    assert normalize_column("Município") == "municipio"


def test_all_contracted_columns_are_present(raw) -> None:
    assert list(raw.columns) == list(EXPECTED_COLUMNS)


def test_every_column_is_a_string(raw) -> None:
    assert {f.dataType.simpleString() for f in raw.schema.fields} == {"string"}


def test_quoted_semicolon_does_not_shift_columns(raw) -> None:
    products = {row["produto"] for row in raw.collect()}
    assert products <= {"GASOLINA", "ETANOL", "DIESEL", "GNV", "GLP", "GASOLINA ADITIVADA"}


def test_carriage_return_does_not_stick_to_last_column(raw) -> None:
    """The published files are CRLF. A stray \\r would ride along on the brand."""
    assert all(not row["bandeira"].endswith("\r") for row in raw.collect())


def test_empty_purchase_price_is_kept(raw) -> None:
    blanks = [r for r in raw.collect() if r["valor_de_compra"] in (None, "")]
    assert blanks, "the fixture contains rows with no purchase price"


def test_values_are_not_converted(raw) -> None:
    prices = {r["valor_de_venda"] for r in raw.collect()}
    assert "1,88" in prices, "the decimal comma belongs to Silver, not here"
    assert any(r["cnpj_da_revenda"].startswith(" ") for r in raw.collect())


def test_unit_of_measure_is_kept_per_row(raw) -> None:
    assert {r["unidade_de_medida"] for r in raw.collect()} == {"R$ / litro", "R$ / 13 kg"}


def test_missing_contracted_column_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    wrong = tmp_path / "wrong.csv"
    wrong.write_text("Regiao - Sigla;Produto\nSE;GASOLINA\n", encoding="utf-8")

    with pytest.raises(UnexpectedHeaderError, match="missing contracted column"):
        read_raw(spark, str(wrong))


def test_lineage_columns_are_stamped(raw) -> None:
    stamped = add_lineage(raw, SOURCE, run_id="run-1", ingested_at=datetime(2026, 1, 1, tzinfo=UTC))
    row = stamped.first()

    assert row["_source_file"] == "precos-glp-03.csv"
    assert row["_source_series"] == "dsan"
    assert row["_source_year"] == 2025
    assert row["_ingestion_run_id"] == "run-1"
    assert row["_header_signature"]


def test_header_signature_changes_when_the_shape_does() -> None:
    baseline = header_signature(list(EXPECTED_COLUMNS))
    assert header_signature([*EXPECTED_COLUMNS, "novidade"]) != baseline


def test_reloading_the_same_file_does_not_duplicate(raw, tmp_path: Path) -> None:
    table = str(tmp_path / "bronze")
    stamped = add_lineage(raw, SOURCE, run_id="run-1")

    write_bronze(stamped, table, SOURCE.filename)
    first = stamped.sparkSession.read.format("delta").load(table).count()
    write_bronze(add_lineage(raw, SOURCE, run_id="run-2"), table, SOURCE.filename)
    second = stamped.sparkSession.read.format("delta").load(table)

    assert second.count() == first
    assert {r["_ingestion_run_id"] for r in second.collect()} == {"run-2"}


def test_other_file_is_left_alone(raw, tmp_path: Path) -> None:
    table = str(tmp_path / "bronze")
    other = SourceFile("dsan", None, 2025, "04", "glp", "csv", "https://x/precos-glp-04.csv")

    write_bronze(add_lineage(raw, SOURCE, run_id="r1"), table, SOURCE.filename)
    write_bronze(add_lineage(raw, other, run_id="r2"), table, other.filename)

    stored = raw.sparkSession.read.format("delta").load(table)
    assert {r["_source_file"] for r in stored.collect()} == {
        "precos-glp-03.csv",
        "precos-glp-04.csv",
    }


LATIN1_SAMPLE = str(Path(__file__).parent / "fixtures" / "anp_sample_latin1.csv")


def test_latin1_file_read_as_utf8_mangles_the_text(spark: SparkSession) -> None:
    """What the wrong charset costs: it is not an error, just wrong data."""
    wrong = read_raw(spark, LATIN1_SAMPLE, encoding="UTF-8")
    names = " ".join(r["revenda"] or "" for r in wrong.collect())

    assert "�" in names or "Ó" not in names


def test_latin1_file_read_with_its_own_charset_is_intact(spark: SparkSession) -> None:
    right = read_raw(spark, LATIN1_SAMPLE, encoding="ISO-8859-1")
    names = " ".join(r["revenda"] or "" for r in right.collect())

    assert "�" not in names
    assert any(c in names for c in "ÓÃÇÁÉ")


def test_encoding_is_recorded_as_lineage(raw) -> None:
    stamped = add_lineage(raw, SOURCE, run_id="r1", encoding="ISO-8859-1")
    assert stamped.first()["_source_encoding"] == "ISO-8859-1"


def test_a_new_lineage_column_does_not_break_an_existing_table(raw, tmp_path: Path) -> None:
    """Bronze has to accept its own schema growing, or the day a lineage
    column is added the whole table has to be rebuilt."""
    table = str(tmp_path / "bronze")
    write_bronze(add_lineage(raw, SOURCE, run_id="r1"), table, SOURCE.filename)

    widened = add_lineage(raw, SOURCE, run_id="r2").withColumn(
        "_source_note", F.lit("something new")
    )
    write_bronze(widened, table, SOURCE.filename)

    stored = raw.sparkSession.read.format("delta").load(table)
    assert "_source_note" in stored.columns
    assert stored.count() == raw.count()
