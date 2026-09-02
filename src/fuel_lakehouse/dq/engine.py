"""Rules from YAML, evaluated against the layers, with a gate at the end.

Not Great Expectations on purpose. The rules needed here are few and known, and
the point of the exercise is the framework itself, not the wiring around
somebody else's.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RULES_FILE = Path(__file__).parent / "rules.yaml"

CRITICAL = "critical"
WARNING = "warning"


class DataQualityError(RuntimeError):
    """A critical rule did not hold."""


@dataclass(frozen=True)
class RuleResult:
    name: str
    severity: str
    passed: bool
    observed: str
    expected: str

    def __str__(self) -> str:
        verdict = "ok" if self.passed else "FAILED"
        return f"[{verdict}] {self.name}: observed {self.observed}, expected {self.expected}"


@dataclass(frozen=True)
class Context:
    """What the rules get to look at."""

    frames: dict[str, DataFrame]
    metrics: dict[str, float]


def load_rules(path: Path = RULES_FILE) -> list[dict[str, Any]]:
    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
    rules: list[dict[str, Any]] = parsed["rules"]
    return rules


def _not_null(rule: dict[str, Any], ctx: Context) -> RuleResult:
    frame = ctx.frames[rule["frame"]]
    nulls = frame.filter(F.col(rule["column"]).isNull()).count()
    return RuleResult(rule["name"], rule["severity"], nulls == 0, f"{nulls} null", "0 null")


def _domain(rule: dict[str, Any], ctx: Context) -> RuleResult:
    frame = ctx.frames[rule["frame"]]
    outside = frame.filter(~F.col(rule["column"]).isin(*rule["values"])).count()
    return RuleResult(
        rule["name"], rule["severity"], outside == 0, f"{outside} outside", "0 outside"
    )


def _range(rule: dict[str, Any], ctx: Context) -> RuleResult:
    frame = ctx.frames[rule["frame"]]
    column = F.col(rule["column"])
    condition = F.lit(True)
    if "min" in rule:
        condition &= column >= rule["min"]
    if "max" in rule:
        condition &= column <= rule["max"]

    offending = frame.filter(column.isNotNull() & ~condition).count()
    bounds = f"[{rule.get('min', '-inf')}, {rule.get('max', 'inf')}]"
    return RuleResult(
        rule["name"], rule["severity"], offending == 0, f"{offending} outside", bounds
    )


def _unique_key(rule: dict[str, Any], ctx: Context) -> RuleResult:
    frame = ctx.frames[rule["frame"]]
    columns = rule["columns"]
    duplicates = (
        frame.groupBy(*columns).count().filter(F.col("count") > 1).count() if frame.count() else 0
    )
    return RuleResult(
        rule["name"], rule["severity"], duplicates == 0, f"{duplicates} duplicated", "0 duplicated"
    )


def _ratio(rule: dict[str, Any], ctx: Context) -> RuleResult:
    denominator = ctx.metrics.get(rule["denominator"], 0)
    numerator = ctx.metrics.get(rule["numerator"], 0)
    value = numerator / denominator if denominator else 0.0

    passed = True
    if "max" in rule:
        passed &= value <= rule["max"]
    if "min" in rule:
        passed &= value >= rule["min"]

    bounds = f"[{rule.get('min', 0)}, {rule.get('max', 1)}]"
    return RuleResult(rule["name"], rule["severity"], passed, f"{value:.6f}", bounds)


def _reconciliation(rule: dict[str, Any], ctx: Context) -> RuleResult:
    source = ctx.metrics.get("source", 0)
    accounted = ctx.metrics.get("accepted", 0) + ctx.metrics.get("rejected", 0)
    return RuleResult(
        rule["name"],
        rule["severity"],
        source == accounted,
        f"{accounted:.0f} accounted",
        f"{source:.0f} in source",
    )


def check_one_unit_per_product(frame: DataFrame) -> tuple[bool, str]:
    offenders = (
        frame.select("product", "unit")
        .distinct()
        .groupBy("product")
        .count()
        .filter(F.col("count") > 1)
    )
    names = [row["product"] for row in offenders.collect()]
    return not names, ", ".join(names) if names else "none"


CUSTOM_CHECKS: dict[str, Callable[[DataFrame], tuple[bool, str]]] = {
    "one_unit_per_product": check_one_unit_per_product,
}


def _custom(rule: dict[str, Any], ctx: Context) -> RuleResult:
    passed, detail = CUSTOM_CHECKS[rule["check"]](ctx.frames[rule["frame"]])
    return RuleResult(rule["name"], rule["severity"], passed, detail, "none")


EVALUATORS: dict[str, Callable[[dict[str, Any], Context], RuleResult]] = {
    "not_null": _not_null,
    "domain": _domain,
    "range": _range,
    "unique_key": _unique_key,
    "ratio": _ratio,
    "reconciliation": _reconciliation,
    "custom": _custom,
}


def run(ctx: Context, rules: list[dict[str, Any]] | None = None) -> list[RuleResult]:
    return [EVALUATORS[rule["type"]](rule, ctx) for rule in (rules or load_rules())]


def gate(results: list[RuleResult]) -> None:
    """Stop the run if a critical rule failed. Warnings only get recorded."""
    breaches = [r for r in results if not r.passed and r.severity == CRITICAL]
    if breaches:
        raise DataQualityError(
            "data quality gate failed:\n" + "\n".join(f"  {b}" for b in breaches)
        )
