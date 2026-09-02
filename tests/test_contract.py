"""The Silver contract: what gets typed, and what gets objected to."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession
from samples import GOOD

from fuel_lakehouse.bronze.ingest import EXPECTED_COLUMNS
from fuel_lakehouse.silver.contract import evaluate


def frame(spark: SparkSession, **overrides: str):
    row = {**GOOD, **overrides}
    return spark.createDataFrame([Row(**{c: row[c] for c in EXPECTED_COLUMNS})])


def one(spark: SparkSession, **overrides: str) -> Row:
    return evaluate(frame(spark, **overrides)).first()


def reasons(spark: SparkSession, **overrides: str) -> list[str]:
    return list(one(spark, **overrides)["_rejection_reasons"])


def test_a_good_row_has_nothing_against_it(spark: SparkSession) -> None:
    assert reasons(spark) == []


def test_comma_decimal_becomes_a_number(spark: SparkSession) -> None:
    assert one(spark)["sale_price"] == Decimal("5.6500")


def test_four_decimal_places_are_not_truncated(spark: SparkSession) -> None:
    row = one(spark, valor_de_compra="1,6623")
    assert row["purchase_price"] == Decimal("1.6623")
    assert row["_rejection_reasons"] == []


def test_lpg_in_the_hundreds_fits(spark: SparkSession) -> None:
    assert one(spark, produto="GLP", valor_de_venda="110,00", unidade_de_medida="R$ / 13 kg")[
        "sale_price"
    ] == Decimal("110.0000")


def test_cnpj_loses_its_mask_and_leading_space(spark: SparkSession) -> None:
    assert one(spark)["reseller_cnpj"] == "00003188000121"


def test_date_is_parsed_from_the_brazilian_format(spark: SparkSession) -> None:
    assert str(one(spark)["collection_date"]) == "2025-03-03"


def test_collection_week_falls_on_the_monday(spark: SparkSession) -> None:
    # 2025-03-03 is itself a Monday; 2025-03-07 is the Friday of that week.
    assert str(one(spark, data_da_coleta="07/03/2025")["collection_week"]) == "2025-03-03"


def test_absent_purchase_price_is_accepted(spark: SparkSession) -> None:
    row = one(spark, valor_de_compra="")
    assert row["purchase_price"] is None
    assert row["_rejection_reasons"] == []


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"valor_de_venda": ""}, "sale_price_missing"),
        ({"valor_de_venda": "ABC"}, "sale_price_not_numeric"),
        ({"valor_de_venda": "-1,00"}, "sale_price_not_positive"),
        ({"valor_de_compra": "nope"}, "purchase_price_not_numeric"),
        ({"produto": "GASOLINA PREMIUM XPTO"}, "unknown_product"),
        ({"unidade_de_medida": "R$ / barril"}, "unknown_unit"),
        ({"estado_sigla": "XX"}, "unknown_state"),
        ({"regiao_sigla": "ZZ"}, "unknown_region"),
        ({"municipio": "  "}, "municipality_missing"),
        ({"bandeira": ""}, "brand_missing"),
        ({"cnpj_da_revenda": "123"}, "cnpj_invalid"),
        ({"data_da_coleta": ""}, "collection_date_missing"),
        ({"data_da_coleta": "31/02/2025"}, "collection_date_not_parseable"),
        ({"data_da_coleta": "01/01/1999"}, "collection_date_out_of_range"),
    ],
)
def test_contract_violations(spark: SparkSession, overrides: dict[str, str], expected: str) -> None:
    assert expected in reasons(spark, **overrides)


def test_a_row_can_fail_more_than_once(spark: SparkSession) -> None:
    got = reasons(spark, estado_sigla="XX", valor_de_venda="-3,00")
    assert "unknown_state" in got
    assert "sale_price_not_positive" in got


def test_nothing_is_dropped(spark: SparkSession) -> None:
    evaluated = evaluate(
        frame(spark).union(frame(spark, valor_de_venda="ABC")).union(frame(spark, produto="X"))
    )
    assert evaluated.count() == 3


def test_original_values_survive_alongside_the_typed_ones(spark: SparkSession) -> None:
    row = one(spark, valor_de_venda="ABC")
    assert row["valor_de_venda"] == "ABC"
    assert row["sale_price"] is None


def test_blank_price_is_not_reported_as_unparseable(spark: SparkSession) -> None:
    assert "purchase_price_not_numeric" not in reasons(spark, valor_de_compra="")


def test_branca_is_a_brand_not_a_missing_value(spark: SparkSession) -> None:
    assert reasons(spark, bandeira="BRANCA") == []


def many(spark: SparkSession, *overrides: dict[str, str]):
    rows = [Row(**{c: {**GOOD, **o}[c] for c in EXPECTED_COLUMNS}) for o in overrides]
    return evaluate(spark.createDataFrame(rows))


def reasons_of(frame) -> list[list[str]]:
    return [list(r["_rejection_reasons"]) for r in frame.collect()]


def test_an_exact_repeat_keeps_one_and_quarantines_the_rest(spark: SparkSession) -> None:
    got = reasons_of(many(spark, {}, {}, {}))

    assert got.count([]) == 1
    assert got.count(["exact_duplicate"]) == 2


def test_a_key_whose_rows_disagree_is_quarantined_whole(spark: SparkSession) -> None:
    """Picking a winner would be the silent choice the pipeline exists to avoid."""
    got = reasons_of(many(spark, {}, {"valor_de_venda": "5,70"}))

    assert got == [["duplicate_key_conflict"], ["duplicate_key_conflict"]]


def test_distinct_keys_are_left_alone(spark: SparkSession) -> None:
    got = reasons_of(many(spark, {}, {"produto": "ETANOL"}))
    assert got == [[], []]


def test_a_repeat_that_differs_only_in_address_is_a_conflict(spark: SparkSession) -> None:
    got = reasons_of(many(spark, {}, {"municipio": "CAMPINAS"}))
    assert all(r == ["duplicate_key_conflict"] for r in got)


def test_an_already_rejected_row_does_not_create_a_false_conflict(
    spark: SparkSession,
) -> None:
    got = reasons_of(many(spark, {}, {"valor_de_venda": "ABC"}))

    assert [] in got
    assert any("sale_price_not_numeric" in r for r in got)
    assert not any("duplicate_key_conflict" in r for r in got)


def test_nothing_is_dropped_by_the_duplicate_check(spark: SparkSession) -> None:
    assert many(spark, {}, {}, {}, {}).count() == 4
