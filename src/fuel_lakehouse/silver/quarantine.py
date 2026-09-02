"""Splitting evaluated rows into what passed and what did not.

A row the contract rejects is kept whole, with the reasons attached. Dropping
it would leave nobody able to answer where the missing rows went, which is how
trust in a table gets lost.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fuel_lakehouse.bronze.ingest import EXPECTED_COLUMNS, LINEAGE_COLUMNS

TYPED_COLUMNS = (
    "region",
    "state",
    "municipality",
    "reseller_name",
    "reseller_cnpj",
    "product",
    "collection_date",
    "collection_week",
    "sale_price",
    "purchase_price",
    "unit",
    "brand",
)


class ReconciliationError(RuntimeError):
    """Rows went missing between two layers."""


@dataclass(frozen=True)
class Split:
    accepted: DataFrame
    rejected: DataFrame


@dataclass(frozen=True)
class Reconciliation:
    source: int
    accepted: int
    rejected: int

    @property
    def missing(self) -> int:
        return self.source - self.accepted - self.rejected

    @property
    def balances(self) -> bool:
        return self.missing == 0


def split(evaluated: DataFrame, *, rejected_at: datetime | None = None) -> Split:
    has_reasons = F.size("_rejection_reasons") > 0
    stamped = rejected_at or datetime.now(UTC)

    accepted = evaluated.filter(~has_reasons).select(*TYPED_COLUMNS, *LINEAGE_COLUMNS)
    rejected = (
        evaluated.filter(has_reasons)
        .select(*EXPECTED_COLUMNS, *LINEAGE_COLUMNS, "_rejection_reasons")
        .withColumn("_rejected_at", F.lit(stamped).cast("timestamp"))
    )
    return Split(accepted=accepted, rejected=rejected)


def write_rejected(rejected: DataFrame, table_path: str, scope: str | None) -> None:
    """Replace this window's rejected rows rather than appending them.

    Appending looks harmless until a window is reprocessed and every rejected
    row is written a second time, which quietly turns the quarantine into a
    table that overstates the damage.
    """
    writer = rejected.write.format("delta").mode("overwrite").option("mergeSchema", "true")
    if scope:
        writer = writer.option("replaceWhere", scope)
    writer.partitionBy("_source_series", "_source_year").save(table_path)


def reconcile(source: int, split_result: Split) -> Reconciliation:
    """Every source row is either in one side or the other.

    Without this the quarantine is just another table that might be incomplete.
    """
    result = Reconciliation(
        source=source,
        accepted=split_result.accepted.count(),
        rejected=split_result.rejected.count(),
    )
    if not result.balances:
        raise ReconciliationError(
            f"{result.missing} row(s) unaccounted for: {result.source} in bronze, "
            f"{result.accepted} accepted, {result.rejected} rejected"
        )
    return result
