"""Bronze layer: the published files, loaded as they came.

Everything stays a string here. Typing at this point silently turns a bad value
into a null and the original is gone.
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
    """Columns are not the ones we contracted for."""


def normalize_column(name: str) -> str:
    """Header to snake_case. The BOM rides along on the first column name."""
    without_bom = name.replace("﻿", "")
    unaccented = unicodedata.normalize("NFKD", without_bom).encode("ascii", "ignore").decode()
    return _NON_ALNUM.sub("_", unaccented.strip().lower()).strip("_")


def header_signature(columns: list[str]) -> str:
    """Digest of the header, so upstream shape changes show up in the table."""
    return hashlib.sha256("|".join(columns).encode()).hexdigest()[:12]


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    frame = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("quote", '"')
        .option("encoding", "UTF-8")
        # 966 rows of the 2004 file have a semicolon inside a quoted field.
        # Ignore the quoting and every column after it shifts.
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
    """Select the contracted columns and stamp where the rows came from.

    Anything beyond the contract is dropped here but still sits in _raw.
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
    """Replace only this file's rows. Append duplicates, overwrite wipes the rest."""
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
