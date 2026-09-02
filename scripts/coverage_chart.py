"""Draw the purchase price coverage per year as an SVG for the README.

Hand-rolled SVG rather than a plotting library: one chart does not justify
matplotlib in the dependency list, and the output has to look right in both
GitHub themes.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import functions as F

from fuel_lakehouse.config import load_config
from fuel_lakehouse.spark import build_spark

WIDTH, HEIGHT = 720, 300
PAD_LEFT, PAD_BOTTOM, PAD_TOP = 56, 44, 24
BAR_GAP = 18

# Readable on white and on dark, without a theme switch.
BAR = "#3d7ea6"
TEXT = "#8a8f98"
AXIS = "#8a8f98"


def bars(data: list[tuple[int, float]]) -> str:
    plot_w = WIDTH - PAD_LEFT - 20
    plot_h = HEIGHT - PAD_BOTTOM - PAD_TOP
    slot = plot_w / len(data)
    width = slot - BAR_GAP

    out = []
    for index, (year, ratio) in enumerate(data):
        height = max(ratio * plot_h, 1.5)
        x = PAD_LEFT + index * slot + BAR_GAP / 2
        y = PAD_TOP + plot_h - height
        label = f"{ratio * 100:.1f}%"
        out.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" '
            f'height="{height:.1f}" fill="{BAR}" rx="2"/>'
        )
        out.append(
            f'<text x="{x + width / 2:.1f}" y="{y - 7:.1f}" fill="{TEXT}" '
            f'font-size="13" text-anchor="middle">{label}</text>'
        )
        out.append(
            f'<text x="{x + width / 2:.1f}" y="{HEIGHT - PAD_BOTTOM + 20:.1f}" '
            f'fill="{TEXT}" font-size="13" text-anchor="middle">{year}</text>'
        )
    return "\n  ".join(out)


def render(data: list[tuple[int, float]]) -> str:
    plot_h = HEIGHT - PAD_BOTTOM - PAD_TOP
    grid = []
    for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = PAD_TOP + plot_h - fraction * plot_h
        grid.append(
            f'<line x1="{PAD_LEFT}" y1="{y:.1f}" x2="{WIDTH - 20}" y2="{y:.1f}" '
            f'stroke="{AXIS}" stroke-opacity="0.18"/>'
        )
        grid.append(
            f'<text x="{PAD_LEFT - 10}" y="{y + 4:.1f}" fill="{TEXT}" font-size="12" '
            f'text-anchor="end">{fraction * 100:.0f}%</text>'
        )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}"
     viewBox="0 0 {WIDTH} {HEIGHT}" font-family="system-ui, sans-serif">
  <text x="{PAD_LEFT}" y="16" fill="{TEXT}" font-size="13">
    Rows carrying a purchase price, by year
  </text>
  {chr(10).join("  " + line for line in grid).strip()}
  {bars(data)}
</svg>
"""


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/img/purchase-price-coverage.svg")
    spark = build_spark("chart")
    try:
        coverage = spark.read.format("delta").load(
            load_config().storage.table("gold", "purchase_price_coverage")
        )
        rows = (
            coverage.groupBy("collection_year")
            .agg(
                (F.sum("rows_with_purchase") / F.sum("total_rows")).alias("ratio"),
            )
            .orderBy("collection_year")
            .collect()
        )
    finally:
        spark.stop()

    data = [(r["collection_year"], float(r["ratio"])) for r in rows]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(data), encoding="utf-8")
    print(f"{out}: {len(data)} year(s)")
    for year, ratio in data:
        print(f"  {year}: {ratio * 100:.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
