"""Bronze: load published files exactly as they arrived.

Nothing is converted and no row is refused here. Every column stays a string,
because a type at this stage turns an unparseable value into a null and the
original is gone before anyone can decide what to do about it. The raw bytes
also stay in the raw prefix, so a bad transformation downstream is always
recoverable without going back to the publisher.

The one thing Bronze does check is shape. A file missing a contracted column is
not drift to absorb, it is a different file, and mapping it anyway would fill a
column with values that belong to another one.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
import uuid
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fuel_lakehouse.sources.anp import SourceFile

EXPECTED_COLUMNS = (
    "regiao_sigla",
    "estado_sigla",
    "municipio",
    "revenda",
    "cnpj_da_revenda",
    "nome_da_rua",
    "numero_rua",
    "complemento",
    "bairro",
    "cep",
    "produto",
    "data_da_coleta",
    "valor_de_venda",
    "valor_de_compra",
    "unidade_de_medida",
    "bandeira",
)

LINEAGE_COLUMNS = (
    "_source_file",
    "_source_series",
    "_source_year",
    "_header_signature",
    "_ingested_at",
    "_ingestion_run_id",
)

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


class UnexpectedHeaderError(RuntimeError):
    """A file whose columns are not the ones under contract."""


def normalize_column(name: str) -> str:
    """Published headers into snake_case.

    The files are UTF-8 with a byte order mark, which arrives attached to the
    first column name and silently makes it not match anything.
    """
    without_bom = name.replace("﻿", "")
    unaccented = unicodedata.normalize("NFKD", without_bom).encode("ascii", "ignore").decode()
    return _NON_ALNUM.sub("_", unaccented.strip().lower()).strip("_")


def header_signature(columns: list[str]) -> str:
    """Short digest of the header, so a change upstream is visible in the data."""
    return hashlib.sha256("|".join(columns).encode()).hexdigest()[:12]


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """One published file, every column a string, column names normalized."""
    frame = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("quote", '"')
        .option("encoding", "UTF-8")
        # 966 rows of the 2004 file carry a semicolon inside a quoted field.
        # Splitting on the delimiter without honouring quotes shifts every
        # column after it, which reads as a postcode in the product column.
        .option("multiLine", "false")
        .csv(path)
    )
    renamed = frame.toDF(*(normalize_column(c) for c in frame.columns))

    missing = [c for c in EXPECTED_COLUMNS if c not in renamed.columns]
    if missing:
        raise UnexpectedHeaderError(
            f"{path} is missing contracted column(s): {', '.join(missing)}. "
            f"Found: {', '.join(renamed.columns)}"
        )
    return renamed


def add_lineage(
    frame: DataFrame,
    source: SourceFile,
    *,
    run_id: str,
    ingested_at: datetime | None = None,
) -> DataFrame:
    """Stamp where each row came from, and select the contracted columns.

    Columns beyond the contract are dropped from the table but never lost: the
    published file is kept verbatim in the raw prefix, and the header signature
    below changes the moment the shape does.
    """
    signature = header_signature(list(frame.columns))
    stamped = ingested_at or datetime.now(UTC)

    return frame.select(*EXPECTED_COLUMNS).select(
        "*",
        F.lit(source.filename).alias("_source_file"),
        F.lit(source.series).alias("_source_series"),
        F.lit(source.year).cast("int").alias("_source_year"),
        F.lit(signature).alias("_header_signature"),
        F.lit(stamped).cast("timestamp").alias("_ingested_at"),
        F.lit(run_id).alias("_ingestion_run_id"),
    )


def write_bronze(frame: DataFrame, table_path: str, source_file: str) -> None:
    """Replace this file's rows, leaving every other file untouched.

    Appending would duplicate on reload and overwriting would destroy the rest
    of the table, so the write is scoped to the one file being ingested.
    """
    escaped = source_file.replace("'", "''")
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_source_file = '{escaped}'")
        .partitionBy("_source_series", "_source_year")
        .save(table_path)
    )


def ingest_file(
    spark: SparkSession,
    source: SourceFile,
    raw_path: str,
    table_path: str,
    *,
    run_id: str | None = None,
) -> int:
    frame = add_lineage(read_raw(spark, raw_path), source, run_id=run_id or str(uuid.uuid4()))
    frame.cache()
    count = frame.count()
    write_bronze(frame, table_path, source.filename)
    frame.unpersist()
    return count
