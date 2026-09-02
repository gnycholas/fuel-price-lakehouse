"""Finding the files ANP publishes.

There is no single filename pattern. 2026 alone has four shapes, one with a typo
from the publisher (02-cados-abertos-preco-...), April is missing, and one 2022
semester is a zip. So attributes are pulled out one by one instead of matching
the whole name, and anything unrecognized is kept as unknown rather than
dropped.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

INDEX_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
    "/serie-historica-de-precos-de-combustiveis"
)

# Longest first, so "gasolina-etanol" is never shadowed by a shorter token.
PRODUCT_GROUPS = ("gasolina-etanol", "diesel-gnv", "glp")

_FILE_LINK = re.compile(r'href="([^"]*?/arquivos/shpc/[^"]+\.(?:csv|zip))"', re.IGNORECASE)
_DSAN_YEAR = re.compile(r"/dsan/(\d{4})/")
_DSAS_SUBSERIES = re.compile(r"/dsas/([a-z]+)/")
_LEADING_PERIOD = re.compile(r"^(\d{2})-")
_TRAILING_PERIOD = re.compile(r"-(\d{2})\.(?:csv|zip)$", re.IGNORECASE)
_YEAR_PERIOD = re.compile(r"(\d{4})-(\d{2})")

STATUS_OK = "ok"
STATUS_UNKNOWN = "unknown"
STATUS_MISSING_UPSTREAM = "missing_upstream"


@dataclass(frozen=True)
class SourceFile:
    """One file published by ANP, as discovered on the index page."""

    series: str
    subseries: str | None
    year: int | None
    period: str | None
    group: str | None
    content_type: str
    url: str
    status: str = STATUS_OK

    @property
    def sort_key(self) -> tuple[str, str, int, str, str, str]:
        """Ordering that survives a file with no year. Absent values sort first."""
        return (
            self.series,
            self.subseries or "",
            self.year or 0,
            self.period or "",
            self.group or "",
            self.url,
        )

    @property
    def filename(self) -> str:
        return self.url.rsplit("/", 1)[-1]

    @property
    def raw_key(self) -> str:
        """Key under the raw prefix. Keeps the published filename as is."""
        folder = self.subseries or "all"
        year = self.year if self.year is not None else "undated"
        return f"{self.series}/{folder}/{year}/{self.filename}"


def _group_of(filename: str) -> str | None:
    lowered = filename.lower()
    return next((g for g in PRODUCT_GROUPS if g in lowered), None)


def _classify_dsan(url: str, filename: str) -> tuple[int | None, str | None, str | None]:
    """Monthly series. Year lives in the path, month in a leading or trailing number."""
    year_match = _DSAN_YEAR.search(url)
    year = int(year_match.group(1)) if year_match else None

    period_match = _LEADING_PERIOD.search(filename) or _TRAILING_PERIOD.search(filename)
    period = period_match.group(1) if period_match else None

    return year, period, _group_of(filename)


def _classify_dsas(filename: str) -> tuple[int | None, str | None]:
    """Semiannual series. Both year and semester sit inside the filename."""
    match = _YEAR_PERIOD.search(filename)
    if match:
        return int(match.group(1)), match.group(2)
    return None, None


def classify(url: str) -> SourceFile:
    filename = url.rsplit("/", 1)[-1]
    content_type = "zip" if filename.lower().endswith(".zip") else "csv"

    if "/dsan/" in url:
        year, period, group = _classify_dsan(url, filename)
        status = STATUS_OK if year and period and group else STATUS_UNKNOWN
        return SourceFile("dsan", None, year, period, group, content_type, url, status)

    if "/dsas/" in url:
        sub_match = _DSAS_SUBSERIES.search(url)
        subseries = sub_match.group(1) if sub_match else None
        year, period = _classify_dsas(filename)
        status = STATUS_OK if subseries and year and period else STATUS_UNKNOWN
        return SourceFile("dsas", subseries, year, period, None, content_type, url, status)

    if "/qus/" in url:
        # Rolling "last four weeks" feed, not part of the historical series.
        return SourceFile("qus", None, None, None, _group_of(filename), content_type, url)

    return SourceFile("unknown", None, None, None, None, content_type, url, STATUS_UNKNOWN)


def sort_files(files: list[SourceFile]) -> list[SourceFile]:
    return sorted(files, key=lambda f: f.sort_key)


def discover(index_html: str) -> list[SourceFile]:
    """Every data file linked from the index page."""
    urls = {match.group(1) for match in _FILE_LINK.finditer(index_html)}
    return sort_files([classify(url) for url in urls])


def write_manifest(files: list[SourceFile], path: Path) -> None:
    """Stable ordering, so a change shows up as a readable diff."""
    payload = {
        "source": INDEX_URL,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "files": [asdict(f) for f in sort_files(files)],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_manifest(path: Path) -> list[SourceFile]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [SourceFile(**record) for record in payload["files"]]


def merge_manifest(known: list[SourceFile], found: list[SourceFile]) -> list[SourceFile]:
    """Previous manifest plus a fresh discovery.

    A file that vanished upstream keeps its entry, flagged. Dropping it would
    hide that ANP withdrew something.
    """
    found_by_url = {f.url: f for f in found}
    merged = list(found)
    merged.extend(
        SourceFile(**{**asdict(old), "status": STATUS_MISSING_UPSTREAM})
        for old in known
        if old.url not in found_by_url
    )
    return sort_files(merged)
