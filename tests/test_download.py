"""Downloader, against an in-memory stand-in for the object store.

A local fake instead of moto keeps the suite free of containers and extra deps.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests
from botocore.exceptions import ClientError

from fuel_lakehouse.sources import download as dl
from fuel_lakehouse.sources.anp import SourceFile

CSV = b"Regiao - Sigla;Estado - Sigla\nSE;SP\n"


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.reads = 0

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        self.reads += 1
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803
        self.objects[Key] = Body


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


def source(url: str = "https://x/arquivos/shpc/dsan/2025/precos-glp-03.csv") -> SourceFile:
    return SourceFile("dsan", None, 2025, "03", "glp", "csv", url)


@pytest.fixture
def s3() -> FakeS3:
    return FakeS3()


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {"body": CSV, "calls": 0, "fail": 0}

    def fake_get(url: str, timeout: int) -> Any:
        state["calls"] += 1
        if state["fail"] > 0:
            state["fail"] -= 1
            raise requests.ConnectionError("boom")

        class Response:
            content = state["body"]

            def raise_for_status(self) -> None:
                return None

        return Response()

    monkeypatch.setattr(dl.requests, "get", fake_get)
    monkeypatch.setattr(dl.time, "sleep", lambda _: None)
    return state


def test_first_run_stores_object_and_sidecar(s3: FakeS3, served: dict[str, Any]) -> None:
    result = dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]

    assert result.action == "downloaded"
    assert "_raw/dsan/all/2025/precos-glp-03.csv" in s3.objects
    assert "_raw/dsan/all/2025/precos-glp-03.csv.meta.json" in s3.objects


def test_sidecar_records_provenance(s3: FakeS3, served: dict[str, Any]) -> None:
    import json

    dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]
    meta = json.loads(s3.objects["_raw/dsan/all/2025/precos-glp-03.csv.meta.json"])

    assert meta["url"].endswith("precos-glp-03.csv")
    assert meta["size"] == len(CSV)
    assert len(meta["sha256"]) == 64
    assert meta["downloaded_at"]


def test_rerun_skips_when_digest_matches(s3: FakeS3, served: dict[str, Any]) -> None:
    dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]
    served["calls"] = 0

    result = dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]

    assert result.action == "skipped"
    assert served["calls"] == 0, "a skip must not touch the network"


def test_altered_object_is_replaced(s3: FakeS3, served: dict[str, Any]) -> None:
    dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]
    s3.objects["_raw/dsan/all/2025/precos-glp-03.csv"] = b"truncated"

    result = dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]

    assert result.action == "downloaded"
    assert s3.objects["_raw/dsan/all/2025/precos-glp-03.csv"] == CSV


def test_object_without_sidecar_is_fetched_again(s3: FakeS3, served: dict[str, Any]) -> None:
    s3.objects["_raw/dsan/all/2025/precos-glp-03.csv"] = CSV

    assert dl.download_file(source(), s3, "bronze").action == "downloaded"  # type: ignore[arg-type]


def test_force_ignores_a_matching_digest(s3: FakeS3, served: dict[str, Any]) -> None:
    dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]
    served["calls"] = 0

    result = dl.download_file(source(), s3, "bronze", force=True)  # type: ignore[arg-type]

    assert result.action == "downloaded"
    assert served["calls"] == 1


def test_transient_failure_is_retried(s3: FakeS3, served: dict[str, Any]) -> None:
    served["fail"] = 2

    result = dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]

    assert result.action == "downloaded"
    assert served["calls"] == 3


def test_persistent_failure_is_reported(s3: FakeS3, served: dict[str, Any]) -> None:
    served["fail"] = 99

    result = dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]

    assert result.action == "failed"
    assert result.error is not None


def test_one_failure_does_not_abandon_the_batch(
    s3: FakeS3, monkeypatch: pytest.MonkeyPatch, served: dict[str, Any]
) -> None:
    bad = source("https://x/arquivos/shpc/dsan/2025/precos-glp-99.csv")
    real = dl.download_file

    def flaky(src: SourceFile, *args: Any, **kwargs: Any) -> dl.DownloadResult:
        if src.url == bad.url:
            return dl.DownloadResult(src, "failed", error="boom")
        return real(src, *args, **kwargs)

    monkeypatch.setattr(dl, "download_file", flaky)
    report = dl.download_files([source(), bad], s3, "bronze")  # type: ignore[arg-type]

    assert len(report.downloaded) == 1
    assert len(report.failed) == 1
    assert "1 downloaded" in report.summary()


def test_zip_keeps_its_extension_in_the_key(s3: FakeS3, served: dict[str, Any]) -> None:
    archive = SourceFile(
        "dsas", "ca", 2022, "02", None, "zip", "https://x/arquivos/shpc/dsas/ca/ca-2022-02.zip"
    )
    served["body"] = b"PK\x03\x04rest of the archive"
    dl.download_file(archive, s3, "bronze")  # type: ignore[arg-type]

    assert "_raw/dsas/ca/2022/ca-2022-02.zip" in s3.objects


def test_html_error_page_is_never_stored(s3: FakeS3, served: dict[str, Any]) -> None:
    served["body"] = b"<!DOCTYPE html><html><body>Ops</body></html>"

    result = dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]

    assert result.action == "failed"
    assert "HTML" in (result.error or "")
    assert not s3.objects


def test_response_without_a_delimiter_is_refused(s3: FakeS3, served: dict[str, Any]) -> None:
    served["body"] = b"something went wrong\n"

    assert dl.download_file(source(), s3, "bronze").action == "failed"  # type: ignore[arg-type]


def test_bom_does_not_hide_an_error_page(s3: FakeS3, served: dict[str, Any]) -> None:
    served["body"] = b"\xef\xbb\xbf<html>nope</html>"

    assert dl.download_file(source(), s3, "bronze").action == "failed"  # type: ignore[arg-type]


def test_zip_must_actually_be_a_zip(s3: FakeS3, served: dict[str, Any]) -> None:
    archive = SourceFile(
        "dsas", "ca", 2022, "02", None, "zip", "https://x/arquivos/shpc/dsas/ca/ca-2022-02.zip"
    )
    served["body"] = b"Regiao;Estado\nSE;SP\n"

    assert dl.download_file(archive, s3, "bronze").action == "failed"  # type: ignore[arg-type]


def test_a_real_csv_still_passes(s3: FakeS3, served: dict[str, Any]) -> None:
    assert dl.download_file(source(), s3, "bronze").action == "downloaded"  # type: ignore[arg-type]


def test_encoding_is_detected_and_recorded(s3: FakeS3, served: dict[str, Any]) -> None:
    import json

    served["body"] = "Regiao;Revenda\nSE;PETRÓLEO LTDA\n".encode("latin-1")
    dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]
    meta = json.loads(s3.objects["_raw/dsan/all/2025/precos-glp-03.csv.meta.json"])

    assert meta["encoding"] == "ISO-8859-1"


def test_utf8_is_detected_as_utf8(s3: FakeS3, served: dict[str, Any]) -> None:
    import json

    served["body"] = "Regiao;Revenda\nSE;PETRÓLEO LTDA\n".encode()
    dl.download_file(source(), s3, "bronze")  # type: ignore[arg-type]
    meta = json.loads(s3.objects["_raw/dsan/all/2025/precos-glp-03.csv.meta.json"])

    assert meta["encoding"] == "UTF-8"
