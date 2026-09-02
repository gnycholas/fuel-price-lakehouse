"""Draw purchase price coverage over time, for the README.

Hand-rolled SVG rather than a plotting library: one chart does not justify
matplotlib in the dependency list, and this way the light and dark versions come
from the same code.

Years with no data break the line instead of being interpolated across. The
source has a real gap and drawing through it would invent a number.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import functions as F

from fuel_lakehouse.config import load_config
from fuel_lakehouse.spark import build_spark

TITLE = "Share of rows carrying a purchase price"
SUBTITLE = "ANP fuel price survey, by year of collection"

WIDTH, HEIGHT = 760, 340
LEFT, RIGHT, TOP, BOTTOM = 62, 30, 62, 46


@dataclass(frozen=True)
class Theme:
    name: str
    surface: str
    series: str
    text: str
    muted: str
    grid: str


LIGHT = Theme("light", "#fcfcfb", "#2a78d6", "#0b0b0b", "#52514e", "#dedcd6")
DARK = Theme("dark", "#1a1a19", "#3987e5", "#ffffff", "#c3c2b7", "#3a3a37")


def runs(years: list[int]) -> list[list[int]]:
    """Consecutive stretches. A missing year splits the line rather than being
    drawn through."""
    out: list[list[int]] = []
    for year in years:
        if out and year == out[-1][-1] + 1:
            out[-1].append(year)
        else:
            out.append([year])
    return out


def render(data: dict[int, float], theme: Theme) -> str:
    years = sorted(data)
    first, last = years[0], years[-1]
    plot_w = WIDTH - LEFT - RIGHT
    plot_h = HEIGHT - TOP - BOTTOM

    def x(year: int) -> float:
        return LEFT + (year - first) / max(last - first, 1) * plot_w

    def y(ratio: float) -> float:
        return TOP + plot_h - ratio * plot_h

    parts: list[str] = [
        f'<rect width="{WIDTH}" height="{HEIGHT}" rx="6" fill="{theme.surface}"/>',
        f'<text x="{LEFT}" y="26" fill="{theme.text}" font-size="15" '
        f'font-weight="600">{TITLE}</text>',
        f'<text x="{LEFT}" y="45" fill="{theme.muted}" font-size="12.5">{SUBTITLE}</text>',
    ]

    for fraction in (0, 0.25, 0.5, 0.75, 1.0):
        gy = y(fraction)
        parts.append(
            f'<line x1="{LEFT}" y1="{gy:.1f}" x2="{WIDTH - RIGHT}" y2="{gy:.1f}" '
            f'stroke="{theme.grid}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{LEFT - 12}" y="{gy + 4:.1f}" fill="{theme.muted}" font-size="11.5" '
            f'text-anchor="end">{fraction * 100:.0f}%</text>'
        )

    for run in runs(years):
        line = " ".join(
            f"{'M' if i == 0 else 'L'}{x(yr):.1f},{y(data[yr]):.1f}" for i, yr in enumerate(run)
        )
        if len(run) > 1:
            area = f"{line} L{x(run[-1]):.1f},{y(0):.1f} L{x(run[0]):.1f},{y(0):.1f} Z"
            parts.append(f'<path d="{area}" fill="{theme.series}" fill-opacity="0.10"/>')
        parts.append(
            f'<path d="{line}" fill="none" stroke="{theme.series}" stroke-width="2" '
            f'stroke-linejoin="round" stroke-linecap="round"/>'
        )

    # Every four years plus the ends. A regular tick too close to an end label
    # is dropped rather than left to collide with it.
    ticks = {first, last} | {
        yr for yr in years if yr % 4 == 0 and abs(yr - first) > 2 and abs(yr - last) > 2
    }
    for year in sorted(ticks):
        parts.append(
            f'<text x="{x(year):.1f}" y="{HEIGHT - BOTTOM + 22:.1f}" fill="{theme.muted}" '
            f'font-size="11.5" text-anchor="middle">{year}</text>'
        )

    zero_from = next((yr for yr in years if data[yr] == 0), None)
    marked = {first, last} | ({zero_from} if zero_from else set())

    for year in sorted(marked):
        cx, cy = x(year), y(data[year])
        parts.append(
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="5.5" fill="{theme.series}" '
            f'stroke="{theme.surface}" stroke-width="2"/>'
        )

    # Label the start and the point it hits zero. Every point labelled would be
    # noise, and those two carry the story.
    label_first = f"{data[first] * 100:.0f}%"
    parts.append(
        f'<text x="{x(first) + 12:.1f}" y="{y(data[first]) - 10:.1f}" fill="{theme.text}" '
        f'font-size="13" font-weight="600">{label_first}</text>'
    )
    if zero_from:
        parts.append(
            f'<text x="{x(zero_from):.1f}" y="{y(0) - 14:.1f}" fill="{theme.text}" '
            f'font-size="13" font-weight="600" text-anchor="middle">'
            f"none from {zero_from}</text>"
        )

    body = "\n  ".join(parts)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, '
        f'sans-serif">\n  {body}\n</svg>\n'
    )


def main() -> int:
    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/img")
    spark = build_spark("chart")
    try:
        coverage = spark.read.format("delta").load(
            load_config().storage.table("gold", "purchase_price_coverage")
        )
        rows = (
            coverage.groupBy("collection_year")
            .agg((F.sum("rows_with_purchase") / F.sum("total_rows")).alias("ratio"))
            .orderBy("collection_year")
            .collect()
        )
    finally:
        spark.stop()

    data = {int(r["collection_year"]): float(r["ratio"]) for r in rows}
    out_dir.mkdir(parents=True, exist_ok=True)
    for theme in (LIGHT, DARK):
        path = out_dir / f"purchase-price-coverage-{theme.name}.svg"
        path.write_text(render(data, theme), encoding="utf-8")
        print(f"{path}")
    for year in sorted(data):
        print(f"  {year}: {data[year] * 100:.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
