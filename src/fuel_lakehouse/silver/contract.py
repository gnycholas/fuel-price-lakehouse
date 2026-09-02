"""The contract every row has to meet, and the typing that goes with it.

Declared in one place on purpose. Spread across when/otherwise chains, nobody
can answer what a valid row is without reading the whole pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Measured across the series, not assumed. DIESEL S10 and GASOLINA ADITIVADA
# only show up in the recent files; the 2004 ones have neither.
PRODUCTS = (
    "DIESEL",
    "DIESEL S10",
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


def evaluate(bronze: DataFrame) -> DataFrame:
    """Bronze rows with the typed columns and whatever the contract objects to.

    Nothing is dropped or split here. Both sides of the split need the original
    values, so that is FEAT-004's job.
    """
    typed = bronze.select("*", *typed_columns())
    return typed.select(
        "*",
        F.date_trunc("week", F.col("collection_date")).cast("date").alias("collection_week"),
        rejection_reasons(),
    )
