"""The contract every row has to meet, and the typing that goes with it.

Declared in one place on purpose. Spread across when/otherwise chains, nobody
can answer what a valid row is without reading the whole pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Measured across the series, not assumed. Sampling only 2004 and 2025 missed
# DIESEL S50 entirely, which cost 44,495 rows on the first full run.
PRODUCTS = (
    "DIESEL",
    "DIESEL S10",
    "DIESEL S50",
    "ETANOL",
    "GASOLINA",
    "GASOLINA ADITIVADA",
    "GLP",
    "GNV",
)

# Not interchangeable: a litre of petrol and a 13 kg cylinder of LPG do not
# average into anything.
UNITS = ("R$ / litro", "R$ / m³", "R$ / 13 kg")

REGIONS = ("N", "NE", "CO", "SE", "S")

STATES = (
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
)  # fmt: skip

# 9,4 covers both ends of the series: four decimals in the 2004 files, and LPG
# in the hundreds.
PRICE_TYPE = "decimal(9,4)"

DATE_FORMAT = "dd/MM/yyyy"
SERIES_START = "2004-01-01"


@dataclass(frozen=True)
class Check:
    reason: str
    valid: Column


def _blank(name: str) -> Column:
    return F.col(name).isNull() | (F.trim(F.col(name)) == "")


def _price(name: str) -> Column:
    """Comma decimal to a real number.

    try_cast, not cast: Spark 4 runs in ANSI mode, where a plain cast throws on
    the first bad value and takes the whole run with it. Bad values belong in
    quarantine, not in a stack trace.
    """
    return F.regexp_replace(F.trim(F.col(name)), ",", ".").try_cast(PRICE_TYPE)


def typed_columns() -> list[Column]:
    return [
        F.trim(F.col("regiao_sigla")).alias("region"),
        F.upper(F.trim(F.col("estado_sigla"))).alias("state"),
        F.trim(F.col("municipio")).alias("municipality"),
        F.trim(F.col("revenda")).alias("reseller_name"),
        F.regexp_replace(F.col("cnpj_da_revenda"), "[^0-9]", "").alias("reseller_cnpj"),
        F.trim(F.col("produto")).alias("product"),
        F.try_to_timestamp(F.trim(F.col("data_da_coleta")), F.lit(DATE_FORMAT))
        .cast("date")
        .alias("collection_date"),
        _price("valor_de_venda").alias("sale_price"),
        _price("valor_de_compra").alias("purchase_price"),
        F.trim(F.col("unidade_de_medida")).alias("unit"),
        F.trim(F.col("bandeira")).alias("brand"),
    ]


def checks() -> list[Check]:
    """Each entry names the rejection reason and the condition for a good row."""
    date = F.col("collection_date")
    sale = F.col("sale_price")
    purchase = F.col("purchase_price")

    return [
        Check("collection_date_missing", ~_blank("data_da_coleta")),
        Check(
            "collection_date_not_parseable",
            _blank("data_da_coleta") | date.isNotNull(),
        ),
        Check(
            "collection_date_out_of_range",
            date.isNull() | date.between(F.lit(SERIES_START).cast("date"), F.current_date()),
        ),
        Check("cnpj_invalid", F.length(F.col("reseller_cnpj")) == 14),
        Check("unknown_product", F.col("product").isin(*PRODUCTS)),
        Check("unknown_unit", F.col("unit").isin(*UNITS)),
        Check("unknown_state", F.col("state").isin(*STATES)),
        Check("unknown_region", F.col("region").isin(*REGIONS)),
        Check("municipality_missing", ~_blank("municipio")),
        Check("brand_missing", ~_blank("bandeira")),
        Check("sale_price_missing", ~_blank("valor_de_venda")),
        Check(
            "sale_price_not_numeric",
            _blank("valor_de_venda") | sale.isNotNull(),
        ),
        Check("sale_price_not_positive", sale.isNull() | (sale > 0)),
        # Absent purchase price is the normal state of this series: the column
        # is 100% empty from 2025 on. Only a present-but-broken value fails.
        Check(
            "purchase_price_not_numeric",
            _blank("valor_de_compra") | purchase.isNotNull(),
        ),
        Check("purchase_price_not_positive", purchase.isNull() | (purchase > 0)),
    ]


def rejection_reasons() -> Column:
    return F.array_compact(F.array(*[F.when(~c.valid, F.lit(c.reason)) for c in checks()])).alias(
        "_rejection_reasons"
    )


NATURAL_KEY = ("reseller_cnpj", "product", "collection_date")

TYPED_NAMES = (
    "region",
    "state",
    "municipality",
    "reseller_name",
    "reseller_cnpj",
    "product",
    "collection_date",
    "sale_price",
    "purchase_price",
    "unit",
    "brand",
)


def flag_duplicates(evaluated: DataFrame) -> DataFrame:
    """Mark rows that share a natural key.

    The key was measured unique on two files and is not unique across the
    series: one 2005 file repeats 36 keys, 34 of them byte for byte and two
    with different prices. Those are two different problems.

    An exact repeat is one observation published more than once, so the extra
    copies are dropped into quarantine and one is kept. A key whose rows
    disagree is a conflict in the source, and picking a winner would be the
    silent choice this pipeline exists to avoid, so every row of that key goes
    to quarantine instead.
    """
    valid = F.size("_rejection_reasons") == 0
    by_key = Window.partitionBy(*NATURAL_KEY)
    ordered = Window.partitionBy(*NATURAL_KEY).orderBy(F.col("_row_fingerprint"))

    marked = (
        evaluated.withColumn("_row_fingerprint", F.hash(*TYPED_NAMES))
        .withColumn("_key_rows", F.count(F.when(valid, 1)).over(by_key))
        .withColumn(
            "_key_variants",
            F.size(F.collect_set(F.when(valid, F.col("_row_fingerprint"))).over(by_key)),
        )
        .withColumn("_key_position", F.row_number().over(ordered))
    )

    repeated = valid & (F.col("_key_rows") > 1)
    conflict = repeated & (F.col("_key_variants") > 1)
    surplus_copy = repeated & (F.col("_key_variants") == 1) & (F.col("_key_position") > 1)

    return marked.withColumn(
        "_rejection_reasons",
        F.array_compact(
            F.concat(
                F.col("_rejection_reasons"),
                F.array(
                    F.when(conflict, F.lit("duplicate_key_conflict")),
                    F.when(surplus_copy, F.lit("exact_duplicate")),
                ),
            )
        ),
    ).drop("_row_fingerprint", "_key_rows", "_key_variants", "_key_position")


def evaluate(bronze: DataFrame) -> DataFrame:
    """Bronze rows with the typed columns and whatever the contract objects to.

    Nothing is dropped or split here. Both sides of the split need the original
    values, so that is FEAT-004's job.
    """
    typed = bronze.select("*", *typed_columns())
    contracted = typed.select(
        "*",
        F.date_trunc("week", F.col("collection_date")).cast("date").alias("collection_week"),
        rejection_reasons(),
    )
    return flag_duplicates(contracted)
