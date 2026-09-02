"""Command line entry points."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fuel_lakehouse.bronze.ingest import ingest_file
from fuel_lakehouse.config import Config, load_config
from fuel_lakehouse.dq.engine import Context, gate, run
from fuel_lakehouse.gold.coverage import coverage, margin
from fuel_lakehouse.gold.price import build as price_gold
from fuel_lakehouse.silver.build import merge, prepare
from fuel_lakehouse.silver.contract import evaluate
from fuel_lakehouse.silver.quarantine import reconcile, split
from fuel_lakehouse.sources.anp import (
    INDEX_URL,
    STATUS_MISSING_UPSTREAM,
    STATUS_UNKNOWN,
    SourceFile,
    discover,
    merge_manifest,
    read_manifest,
    write_manifest,
)
from fuel_lakehouse.sources.download import (
    MAX_CONCURRENCY,
    RAW_PREFIX,
    build_s3_client,
    download_files,
)
from fuel_lakehouse.spark import build_spark

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

MANIFEST = Path("manifest/anp_files.json")
INDEX_TIMEOUT = 90

log = logging.getLogger("fuel_lakehouse")


def cmd_discover(args: argparse.Namespace) -> int:
    response = requests.get(INDEX_URL, timeout=INDEX_TIMEOUT)
    response.raise_for_status()

    found = discover(response.text)
    merged = merge_manifest(read_manifest(args.manifest), found)
    write_manifest(merged, args.manifest)

    unknown = [f for f in merged if f.status == STATUS_UNKNOWN]
    withdrawn = [f for f in merged if f.status == STATUS_MISSING_UPSTREAM]

    log.info("%d files in manifest (%d found now)", len(merged), len(found))
    if withdrawn:
        log.warning("%d file(s) no longer published upstream", len(withdrawn))
    if unknown:
        # Not an error. Needs someone to look, not the run to stop.
        log.warning("%d file(s) with an unrecognized name:", len(unknown))
        for f in unknown:
            log.warning("  %s", f.filename)
    return 0


def _select(files: list[SourceFile], args: argparse.Namespace) -> list[SourceFile]:
    selected = [f for f in files if f.status != STATUS_MISSING_UPSTREAM]
    if args.series:
        selected = [f for f in selected if f.series == args.series]
    else:
        # The rolling four-week feed just duplicates the historical series.
        selected = [f for f in selected if f.series != "qus"]
    if args.year:
        selected = [f for f in selected if f.year in args.year]
    if args.group:
        selected = [f for f in selected if f.group == args.group]
    return selected


def cmd_download(args: argparse.Namespace) -> int:
    manifest = read_manifest(args.manifest)
    if not manifest:
        log.error("manifest is empty; run `discover` first")
        return 1

    selected = _select(manifest, args)
    if not selected:
        log.error("no file in the manifest matches the given filters")
        return 1

    cfg = load_config()
    report = download_files(
        selected,
        build_s3_client(cfg.storage),
        cfg.storage.bucket_bronze,
        force=args.force,
        concurrency=args.concurrency or MAX_CONCURRENCY,
    )
    log.info("%s", report.summary())
    for failure in report.failed:
        log.error("  %s: %s", failure.file.filename, failure.error)
    return 1 if report.failed else 0


def _encoding_of(s3: S3Client, bucket: str, source: SourceFile) -> str:
    """Charset recorded when the file was downloaded.

    Not every published file is UTF-8, and the difference is invisible until
    accented names come out wrong.
    """
    key = f"{RAW_PREFIX}/{source.raw_key}.meta.json"
    try:
        meta = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except (ClientError, json.JSONDecodeError):
        log.warning("no sidecar for %s, assuming UTF-8", source.filename)
        return "UTF-8"
    recorded = meta.get("encoding")
    return recorded if isinstance(recorded, str) else "UTF-8"


def cmd_bronze(args: argparse.Namespace) -> int:
    manifest = read_manifest(args.manifest)
    selected = _select(manifest, args)
    if not selected:
        log.error("no file in the manifest matches the given filters")
        return 1

    cfg = load_config()
    # Zips need unpacking first. Not handled here yet.
    readable = [f for f in selected if f.content_type == "csv"]
    if len(readable) < len(selected):
        log.warning("skipping %d archive(s) for now", len(selected) - len(readable))

    s3 = build_s3_client(cfg.storage)
    spark = build_spark("bronze")
    run_id = str(uuid.uuid4())
    table = cfg.storage.table("bronze", "price_observation_raw")
    total = 0
    try:
        for source in readable:
            raw_path = f"{cfg.storage.bronze_raw}/{source.raw_key}"
            encoding = _encoding_of(s3, cfg.storage.bucket_bronze, source)
            rows = ingest_file(spark, source, raw_path, table, run_id=run_id, encoding=encoding)
            total += rows
            log.info("%s: %d rows", source.filename, rows)
    finally:
        spark.stop()

    log.info("run %s ingested %d rows from %d file(s)", run_id, total, len(readable))
    return 0


def _bronze_window(spark: SparkSession, cfg: Config, args: argparse.Namespace) -> DataFrame:
    frame = spark.read.format("delta").load(cfg.storage.table("bronze", "price_observation_raw"))
    if args.year:
        frame = frame.filter(F.col("_source_year").isin(*args.year))
    if args.series:
        frame = frame.filter(F.col("_source_series") == args.series)
    return frame


def cmd_silver(args: argparse.Namespace) -> int:
    cfg = load_config()
    spark = build_spark("silver")
    try:
        bronze = _bronze_window(spark, cfg, args).cache()
        source_count = bronze.count()
        if not source_count:
            log.error("no bronze rows match the given filters")
            return 1

        result = split(evaluate(bronze))
        result.accepted.cache()
        result.rejected.cache()
        counts = reconcile(source_count, result)
        log.info(
            "%d in bronze, %d accepted, %d rejected",
            counts.source,
            counts.accepted,
            counts.rejected,
        )

        with_purchase = result.accepted.filter(F.col("purchase_price").isNotNull()).count()
        results = run(
            Context(
                frames={"accepted": result.accepted},
                metrics={
                    "source": counts.source,
                    "accepted": counts.accepted,
                    "rejected": counts.rejected,
                    "with_purchase_price": with_purchase,
                },
            )
        )
        for outcome in results:
            log.info("%s", outcome)

        if counts.rejected:
            (
                result.rejected.write.format("delta")
                .mode("append")
                .save(cfg.storage.table("silver", "price_observation_rejected"))
            )

        # Gate before the merge: a breach must not reach the table that gold
        # reads from.
        gate(results)
        merge(spark, prepare(result.accepted), cfg.storage.table("silver", "price_observation"))
        log.info("silver updated")
    finally:
        spark.stop()
    return 0


def cmd_gold(args: argparse.Namespace) -> int:
    cfg = load_config()
    spark = build_spark("gold")
    try:
        silver = spark.read.format("delta").load(cfg.storage.table("silver", "price_observation"))
        silver.cache()

        tables = {
            "price_by_state_product_week": price_gold(silver),
            "purchase_price_coverage": coverage(silver),
            "margin_by_state_week": margin(silver),
        }
        for name, frame in tables.items():
            frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
                cfg.storage.table("gold", name)
            )
            log.info("%s: %d rows", name, frame.count())
    finally:
        spark.stop()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fuel-lakehouse")
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("discover", help="refresh the manifest from the index page")

    download = sub.add_parser("download", help="fetch manifest files into the raw prefix")
    download.add_argument("--year", type=int, nargs="*", help="restrict to these years")
    download.add_argument("--group", help="gasolina-etanol, diesel-gnv or glp")
    download.add_argument("--series", help="dsan, dsas or qus")
    download.add_argument("--force", action="store_true", help="ignore matching digests")
    download.add_argument(
        "--concurrency", type=int, default=None, help="parallel downloads (default 6)"
    )

    bronze = sub.add_parser("bronze", help="load raw files into the bronze table")
    bronze.add_argument("--year", type=int, nargs="*", help="restrict to these years")
    bronze.add_argument("--group", help="gasolina-etanol, diesel-gnv or glp")
    bronze.add_argument("--series", help="dsan, dsas or qus")

    silver = sub.add_parser("silver", help="apply the contract and merge into silver")
    silver.add_argument("--year", type=int, nargs="*", help="restrict to these source years")
    silver.add_argument("--series", help="dsan or dsas")

    sub.add_parser("gold", help="rebuild the gold tables")

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    handlers = {
        "discover": cmd_discover,
        "download": cmd_download,
        "bronze": cmd_bronze,
        "silver": cmd_silver,
        "gold": cmd_gold,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
