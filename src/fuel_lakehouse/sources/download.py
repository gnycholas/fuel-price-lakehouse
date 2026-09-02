"""Fetching published files into the raw prefix, once.

Every stored object gets a sidecar recording where it came from and the digest
of what arrived. A rerun compares digests rather than checking whether the key
exists, so a truncated or altered object is replaced instead of trusted.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

import boto3
import requests
from botocore.exceptions import ClientError

from fuel_lakehouse.config import StorageConfig
from fuel_lakehouse.sources.anp import SourceFile

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

log = logging.getLogger(__name__)

RAW_PREFIX = "_raw"
REQUEST_TIMEOUT = 90  # measured: the ANP server is slow but answers well inside this
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = 2.0

Action = Literal["downloaded", "skipped", "failed"]


@dataclass(frozen=True)
class DownloadResult:
    file: SourceFile
    action: Action
    sha256: str | None = None
    size: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class DownloadReport:
    results: list[DownloadResult]

    def _of(self, action: Action) -> list[DownloadResult]:
        return [r for r in self.results if r.action == action]

    @property
    def downloaded(self) -> list[DownloadResult]:
        return self._of("downloaded")

    @property
    def skipped(self) -> list[DownloadResult]:
        return self._of("skipped")

    @property
    def failed(self) -> list[DownloadResult]:
        return self._of("failed")

    def summary(self) -> str:
        return (
            f"{len(self.downloaded)} downloaded, "
            f"{len(self.skipped)} already current, "
            f"{len(self.failed)} failed"
        )


def build_s3_client(cfg: StorageConfig) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name="us-east-1",
    )


def _meta_key(key: str) -> str:
    return f"{key}.meta.json"


def _read_meta(s3: S3Client, bucket: str, key: str) -> dict[str, object] | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=_meta_key(key))["Body"].read()
    except ClientError:
        return None
    try:
        parsed: dict[str, object] = json.loads(body)
        return parsed
    except json.JSONDecodeError:
        return None


def _stored_digest(s3: S3Client, bucket: str, key: str) -> str | None:
    """Digest of what is actually stored, not what a sidecar claims.

    This re-reads the object, which costs a local round trip. Against a remote
    store it would be worth trading for size plus ETag; here correctness is
    cheap enough to prefer.
    """
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError:
        return None
    return hashlib.sha256(body).hexdigest()


def _fetch(url: str) -> bytes:
    last: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            last = exc
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"giving up after {MAX_ATTEMPTS} attempts: {last}")


def download_file(
    source: SourceFile,
    s3: S3Client,
    bucket: str,
    *,
    force: bool = False,
) -> DownloadResult:
    key = f"{RAW_PREFIX}/{source.raw_key}"
    meta = _read_meta(s3, bucket, key)

    if meta and not force:
        recorded = meta.get("sha256")
        if isinstance(recorded, str) and _stored_digest(s3, bucket, key) == recorded:
            return DownloadResult(source, "skipped", sha256=recorded)

    try:
        payload = _fetch(source.url)
    except RuntimeError as exc:
        log.warning("failed to download %s: %s", source.url, exc)
        return DownloadResult(source, "failed", error=str(exc))

    digest = hashlib.sha256(payload).hexdigest()
    s3.put_object(Bucket=bucket, Key=key, Body=payload)
    s3.put_object(
        Bucket=bucket,
        Key=_meta_key(key),
        Body=json.dumps(
            {
                "url": source.url,
                "series": source.series,
                "subseries": source.subseries,
                "year": source.year,
                "period": source.period,
                "group": source.group,
                "content_type": source.content_type,
                "size": len(payload),
                "sha256": digest,
                "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            },
            indent=2,
        ).encode(),
    )
    return DownloadResult(source, "downloaded", sha256=digest, size=len(payload))


def download_files(
    sources: list[SourceFile],
    s3: S3Client,
    bucket: str,
    *,
    force: bool = False,
) -> DownloadReport:
    """Download what is missing or stale.

    One failure does not abandon the batch: everything that can be fetched is
    fetched, and the report says what was left behind.
    """
    return DownloadReport([download_file(s, s3, bucket, force=force) for s in sources])
