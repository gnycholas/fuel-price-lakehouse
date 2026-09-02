"""Discovery over a trimmed copy of the real index page.

The fixture keeps the anomalies verbatim, because those are the whole reason
discovery exists instead of a URL template.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from fuel_lakehouse.sources.anp import (
    STATUS_MISSING_UPSTREAM,
    STATUS_OK,
    STATUS_UNKNOWN,
    SourceFile,
    classify,
    discover,
    merge_manifest,
    read_manifest,
    sort_files,
    write_manifest,
)

INDEX = (Path(__file__).parent / "fixtures" / "anp_index.html").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def files() -> list[SourceFile]:
    return discover(INDEX)


def find(files: list[SourceFile], fragment: str) -> SourceFile:
    return next(f for f in files if fragment in f.url)


def test_all_four_2026_filename_shapes_are_discovered(files: list[SourceFile]) -> None:
    gasolina_2026 = sorted(
        f.period
        for f in files
        if f.series == "dsan" and f.year == 2026 and f.group == "gasolina-etanol"
    )
    # April is genuinely absent upstream; every other month must be found,
    # across all four naming shapes.
    assert gasolina_2026 == ["01", "02", "03", "05", "06", "07"]


def test_publisher_typo_is_classified_not_skipped(files: list[SourceFile]) -> None:
    typo = find(files, "02-cados-abertos-preco-gasolina-etanol")
    assert (typo.year, typo.period, typo.group) == (2026, "02", "gasolina-etanol")
    assert typo.status == STATUS_OK


def test_publisher_typo_is_not_normalized(files: list[SourceFile]) -> None:
    """Correcting it here would break the day ANP fixes it upstream."""
    assert find(files, "cados").filename == "02-cados-abertos-preco-gasolina-etanol.csv"


def test_month_embedded_in_the_middle_is_read(files: list[SourceFile]) -> None:
    june = find(files, "06-dados-abertos-precos-2026-06-gasolina-etanol")
    assert (june.year, june.period) == (2026, "06")


def test_missing_month_is_simply_absent(files: list[SourceFile]) -> None:
    assert not [f for f in files if f.year == 2026 and f.period == "04"]


def test_regular_monthly_shape_still_works(files: list[SourceFile]) -> None:
    march = find(files, "dsan/2025/precos-gasolina-etanol-03")
    assert (march.year, march.period, march.group) == (2025, "03", "gasolina-etanol")


def test_zip_among_the_csv_files_is_recorded_as_zip(files: list[SourceFile]) -> None:
    archive = find(files, "ca-2022-02")
    assert archive.content_type == "zip"
    assert (archive.subseries, archive.year, archive.period) == ("ca", 2022, "02")


def test_three_lpg_naming_schemes_all_resolve(files: list[SourceFile]) -> None:
    assert find(files, "dsas/glp/glp-2010-01").year == 2010
    assert find(files, "precos-semestrais-glp-2022-01").year == 2022
    # glp2021-NN has no separator between the word and the year.
    assert find(files, "precos-semestrais-glp2021").year == 2021


def test_rolling_feed_is_kept_apart_from_the_historical_series(
    files: list[SourceFile],
) -> None:
    rolling = find(files, "ultimas-4-semanas-glp")
    assert rolling.series == "qus"
    assert rolling.year is None


def test_unrecognized_shape_is_flagged_rather_than_dropped() -> None:
    surprise = classify("https://x/arquivos/shpc/dsan/2027/brand-new-shape.csv")
    assert surprise.status == STATUS_UNKNOWN
    assert surprise.url.endswith("brand-new-shape.csv")


def test_raw_key_keeps_the_published_filename(files: list[SourceFile]) -> None:
    assert find(files, "cados").raw_key == (
        "dsan/all/2026/02-cados-abertos-preco-gasolina-etanol.csv"
    )


def test_manifest_round_trips(tmp_path: Path, files: list[SourceFile]) -> None:
    path = tmp_path / "anp_files.json"
    write_manifest(files, path)
    assert read_manifest(path) == sort_files(files)


def test_manifest_ordering_is_stable(tmp_path: Path, files: list[SourceFile]) -> None:
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    write_manifest(files, first)
    write_manifest(list(reversed(files)), second)
    strip = lambda text: "\n".join(  # noqa: E731
        line for line in text.splitlines() if "generated_at" not in line
    )
    assert strip(first.read_text()) == strip(second.read_text())


def test_upstream_removal_is_flagged_not_deleted(files: list[SourceFile]) -> None:
    withdrawn = files[0]
    merged = merge_manifest(known=files, found=files[1:])

    assert len(merged) == len(files)
    assert find(merged, withdrawn.url).status == STATUS_MISSING_UPSTREAM


def test_reappearing_file_loses_the_missing_flag(files: list[SourceFile]) -> None:
    stale = [SourceFile(**{**vars(f), "status": STATUS_MISSING_UPSTREAM}) for f in files]
    merged = merge_manifest(known=stale, found=files)

    assert all(f.status != STATUS_MISSING_UPSTREAM for f in merged)


def test_sorting_tolerates_a_file_with_no_year() -> None:
    """A shape that yields no year must not break ordering for the rest."""
    dated = classify("https://x/arquivos/shpc/dsan/2025/precos-glp-03.csv")
    undated = classify("https://x/arquivos/shpc/dsan/brand-new-shape.csv")

    assert sort_files([dated, undated])[0] is undated
