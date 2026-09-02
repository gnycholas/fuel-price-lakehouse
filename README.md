# fuel-price-lakehouse

[![CI](https://github.com/gnycholas/fuel-price-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/gnycholas/fuel-price-lakehouse/actions/workflows/ci.yml)

A bronze/silver/gold pipeline over Brazil's national fuel price survey, built
with PySpark and Delta Lake. Around 20 years of weekly price collection from
petrol stations, roughly 5 million rows in the window loaded here.

What it is really about: the quality checks stop the run instead of logging a
warning, and rows that fail the contract go to a quarantine table rather than
disappearing.

## Running it

```bash
docker compose up -d                                   # MinIO, buckets included
make install
make pipeline                                          # discover, download, bronze, silver, gold
```

No cloud account needed. Storage is MinIO behind `s3a://` paths, so the code has
no local mode branch. A smaller slice, if you would rather not pull the lot:

```bash
.venv/bin/python -m fuel_lakehouse.cli download --year 2025 --group glp
.venv/bin/python -m fuel_lakehouse.cli bronze --year 2025
.venv/bin/python -m fuel_lakehouse.cli silver --year 2025
.venv/bin/python -m fuel_lakehouse.cli gold
```

## Shape

```mermaid
flowchart TD
    A["ANP index page"] -->|discover| M["manifest/anp_files.json"]
    M -->|download, sha256 checked| R["bronze/_raw<br/>files as published"]
    R --> B["bronze.price_observation_raw<br/>every column a string"]
    B -->|contract| Q["silver.price_observation_rejected<br/>whole row plus reasons"]
    B -->|contract| S["silver.price_observation<br/>merged on the natural key"]
    S --> G1["gold.price_by_state_product_week"]
    S --> G2["gold.purchase_price_coverage"]
    S --> G3["gold.margin_by_state_week"]

    GATE{{"quality gate<br/>critical breach stops here"}}
    S --- GATE
    GATE --- G1
```

Four things hold on every run, and each has a test that breaks them on purpose:

- `count(bronze) == count(accepted) + count(rejected)`
- `(reseller_cnpj, product, collection_date)` is unique in silver
- rerunning a window changes nothing
- a critical rule breach stops the run before gold is touched

## What is awkward about this data

The interesting part of this dataset is not its size. The schema has been
byte-identical since 2004, so there is no drift to absorb. What it has instead
is a set of traps that produce wrong numbers without producing errors.

### The purchase price column stops being filled

![Purchase price coverage by year](docs/img/purchase-price-coverage.svg)

The column is declared for the whole series and stops being filled partway
through: 63% of rows carry a purchase price in 2004, 41% in 2012, and none at
all in 2021 or 2025. Any margin metric built on top of it quietly stops meaning
anything, and nothing in the pipeline notices unless somebody makes it notice.

So `gold.margin_by_state_week` always carries `coverage_ratio` and `is_reliable`
next to the number, and a week with no purchase price produces no row rather
than a zero margin. A zero would drag every average down while looking like
data.

### Prices come in units that cannot be averaged together

| Product | Unit |
|---|---|
| GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, DIESEL S50 | `R$ / litro` |
| GNV | `R$ / m³` |
| GLP | `R$ / 13 kg` |

A `GROUP BY state, product` mixes litres of petrol with 13 kg cylinders of LPG.
The result is arithmetically meaningless and looks perfectly normal. The unit is
therefore part of the gold grain, not a descriptive column, which makes that
query impossible to write by accident.

### The published filenames follow no pattern

2026 alone has four shapes. One of them carries a typo from the publisher:

```
01-dados-abertos-precos-gasolina-etanol.csv
02-cados-abertos-preco-gasolina-etanol.csv          <- cados, and preco singular
03-dados-abertos-precos-gasolina-etanol.csv
05-dados-abertos-precos-gasolina-etanol.csv         <- April is not published
06-dados-abertos-precos-2026-06-gasolina-etanol.csv <- year and month in the middle
```

Add a ZIP among the CSVs for one 2022 semester, a whole year missing between the
old semiannual series and the current monthly one, and three naming schemes in
the LPG files. Building URLs from a template loses February 2026 to a typo,
silently. Files are discovered from the index page instead, into a versioned
manifest, and anything unrecognized is recorded rather than skipped.

### One file in the series is latin-1

`ca-2021-02.csv` is latin-1. `ca-2021-01.csv`, the other half of the same year,
is UTF-8 with a byte order mark. Read the first one as UTF-8 and nothing fails:
every accented character in it comes out mangled, municipality and reseller
names included, and the damage surfaces much later as groupings that do not
match. The charset is detected at download time and travels with the rows as
`_source_encoding`.

### Quoting is not optional

966 rows of the 2004 file contain a semicolon inside a quoted field. Split on
the delimiter without honouring quotes and every column after it shifts, which
reads as a postcode sitting in the product column. That is exactly the count of
rows a naive split gets wrong, and none of them raise anything.

## What the numbers looked like

Over the window loaded here, 5,036,072 rows from 2004, 2012, 2021 and 2025:

```
5036072 in bronze, 5036072 accepted, 0 rejected
[ok] natural_key_unique:     0 duplicated
[ok] one_unit_per_product:   none
[ok] layer_reconciliation:   5036072 accounted, 5036072 in source
[ok] rejection_rate:         0.000000, expected [0, 0.001]
[--] purchase_price_coverage: 0.316287
```

It did not start there. The first run against the full window rejected 53,038
rows, 1.05% against a threshold guessed at 1%, so the gate failed. None of those
rows were bad data. Two things were wrong with the contract:

- `DIESEL S50` was missing from the product list. Sampling only 2004 and 2025
  skipped the era when it existed, which cost 44,495 rows.
- `R$ / m³` was arriving as `R$ / m?`, because one file in the series is latin-1
  and was being read as UTF-8.

Thresholds are set from what was measured rather than picked in advance, and the
numbers behind each one sit next to it in the code. Rejection rate allows 0.1%
against an observed 0. A weekly cell is flagged as thin below 10 observations,
which is the bottom decile of 30,822 cells whose median is 60. A margin counts
as reliable above 50% coverage, near the middle of the observed spread.

Picking those in advance is how a gate ends up rejecting good data for a year
and the conclusion becomes "the publisher ships rubbish" instead of "the
contract is incomplete".

## Layout

```
src/fuel_lakehouse/
  sources/      discovery, manifest, download with digest checks
  bronze/       load as published, all strings, lineage stamped
  silver/       contract, quarantine, reconciliation, merge
  gold/         weekly prices, coverage, margin
  dq/           rules.yaml and the engine that runs them
tests/          fixtures cut from the real published files
scripts/        chart generation for this page
```

## Tests

Transformations are functions from DataFrame to DataFrame, so most of the suite
runs without touching object storage. Fixtures are cut from the real files
rather than invented, keeping the byte order mark, CRLF endings, the quoted
semicolon and a latin-1 sample: made-up fixtures do not reproduce what real data
does.

```bash
make check      # format, lint, types, tests
```

## Decisions

Written up in [docs/decisions.md](docs/decisions.md), with what each one costs.

## Source

[Série Histórica de Preços de Combustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis),
published as open data by ANP, Brazil's petroleum and fuel regulator. It surveys
petrol stations across the country and publishes what each one charged, by
product and week.

Code is MIT licensed. The data belongs to ANP and keeps its own terms.
