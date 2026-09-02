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

from fuel_lakehouse.bronze.ingest import ingest_file
from fuel_lakehouse.config import load_config
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
from fuel_lakehouse.sources.download import RAW_PREFIX, build_s3_client, download_files
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

    bronze = sub.add_parser("bronze", help="load raw files into the bronze table")
    bronze.add_argument("--year", type=int, nargs="*", help="restrict to these years")
    bronze.add_argument("--group", help="gasolina-etanol, diesel-gnv or glp")
    bronze.add_argument("--series", help="dsan, dsas or qus")

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    handlers = {"discover": cmd_discover, "download": cmd_download, "bronze": cmd_bronze}
    return handlers[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
