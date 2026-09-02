"""The rule engine and the gate."""

from __future__ import annotations

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from fuel_lakehouse.dq.engine import (
    CRITICAL,
    EVALUATORS,
    WARNING,
    Context,
    DataQualityError,
    RuleResult,
    check_one_unit_per_product,
    gate,
    load_rules,
    run,
)


def accepted(spark: SparkSession, *rows: tuple[str, str, str, float]):
    return spark.createDataFrame(
        [
            Row(
                reseller_cnpj=c,
                product=p,
                unit=u,
                collection_date="2025-03-03",
                state="SP",
                sale_price=v,
            )
            for c, p, u, v in rows
        ]
    )


ONE_GOOD_ROW = ("00003188000121", "GASOLINA", "R$ / litro", 5.65)


def ctx(frame, **metrics: float) -> Context:
    return Context(frames={"accepted": frame}, metrics=metrics)


def only(rule_name: str) -> list[dict]:
    return [r for r in load_rules() if r["name"] == rule_name]


def result(name: str, frame, **metrics: float) -> RuleResult:
    return run(ctx(frame, **metrics), only(name))[0]


def test_shipped_rules_all_have_a_known_type() -> None:
    assert all(rule["type"] in EVALUATORS for rule in load_rules())


def test_shipped_rules_have_a_severity() -> None:
    assert all(rule["severity"] in (CRITICAL, WARNING) for rule in load_rules())


def test_a_clean_frame_passes_everything(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW)
    results = run(ctx(frame, source=1, accepted=1, rejected=0, with_purchase_price=0))

    assert [r.name for r in results if not r.passed] == []


def test_negative_price_fails_the_range_rule(spark: SparkSession) -> None:
    frame = accepted(spark, ("00003188000121", "GASOLINA", "R$ / litro", -1.0))
    assert not result("sale_price_positive", frame).passed


def test_state_outside_the_domain_fails(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW).withColumn("state", F.lit("XX"))
    assert not result("state_in_domain", frame).passed


def test_duplicated_natural_key_fails(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW, ONE_GOOD_ROW)
    assert not result("natural_key_unique", frame).passed


def test_distinct_keys_pass(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW, ("00003188000121", "ETANOL", "R$ / litro", 3.78))
    assert result("natural_key_unique", frame).passed


def test_a_product_with_two_units_is_caught(spark: SparkSession) -> None:
    frame = accepted(
        spark,
        ONE_GOOD_ROW,
        ("00003188000122", "GASOLINA", "R$ / 13 kg", 110.0),
    )
    passed, detail = check_one_unit_per_product(frame)

    assert not passed
    assert "GASOLINA" in detail


def test_units_that_differ_per_product_are_fine(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW, ("00003188000122", "GLP", "R$ / 13 kg", 110.0))
    assert check_one_unit_per_product(frame)[0]


def test_rejection_rate_over_the_limit_fails(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW)
    over = result("rejection_rate", frame, source=100, rejected=4)

    assert not over.passed
    assert "0.04" in over.observed


def test_rejection_rate_under_the_limit_passes(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW)
    assert result("rejection_rate", frame, source=1_000_000, rejected=500).passed


def test_the_rate_that_first_broke_the_gate_still_breaks_it(spark: SparkSession) -> None:
    """The first full run rejected 53,038 of 5,036,072 rows, all of them from
    gaps in the contract rather than bad data."""
    frame = accepted(spark, ONE_GOOD_ROW)
    assert not result("rejection_rate", frame, source=5_036_072, rejected=53_038).passed


def test_reconciliation_that_does_not_add_up_fails(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW)
    assert not result("layer_reconciliation", frame, source=10, accepted=7, rejected=2).passed


def test_reconciliation_that_adds_up_passes(spark: SparkSession) -> None:
    frame = accepted(spark, ONE_GOOD_ROW)
    assert result("layer_reconciliation", frame, source=10, accepted=8, rejected=2).passed


def test_gate_raises_on_a_critical_breach() -> None:
    results = [RuleResult("bad", CRITICAL, False, "4", "0")]
    with pytest.raises(DataQualityError, match="bad"):
        gate(results)


def test_gate_message_carries_observed_and_expected() -> None:
    with pytest.raises(DataQualityError) as err:
        gate([RuleResult("rejection_rate", CRITICAL, False, "0.040000", "[0, 0.01]")])

    assert "observed 0.040000" in str(err.value)
    assert "expected [0, 0.01]" in str(err.value)


def test_a_warning_does_not_stop_the_run() -> None:
    gate([RuleResult("coverage", WARNING, False, "0.0", "[0.0, 1]")])


def test_empty_purchase_price_coverage_is_only_a_warning(spark: SparkSession) -> None:
    """100% empty is the normal state of this column from 2025 on."""
    frame = accepted(spark, ONE_GOOD_ROW)
    coverage = result("purchase_price_coverage", frame, accepted=1000, with_purchase_price=0)

    assert coverage.severity == WARNING
    gate([coverage])
