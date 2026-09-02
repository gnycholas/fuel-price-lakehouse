# Decisions

Short notes on the choices that shaped this pipeline, and what each one costs.

## Bronze keeps every column as a string

Typing at ingestion turns an unparseable value into a null, and the original is
gone before anyone can decide what to do with it. Bronze stores what arrived;
the contract in Silver is where values get judged.

Cost: an extra layer, and a Silver step that has to parse everything.

## Rejected rows go to quarantine, not to the bin

A row that fails the contract is written whole to
`silver.price_observation_rejected` with the reasons attached. Dropping it with
a log line means that when someone asks where 4,312 rows went, the answer is a
shrug.

Every run also checks `bronze == accepted + rejected`. Without that count the
quarantine is just another table that might be incomplete, and a bug that
silently drops rows looks exactly like a clean run. There is a test that breaks
the split on purpose and checks the count notices.

## Silver merges, it does not overwrite

The write is a `MERGE` on `(reseller_cnpj, product, collection_date)`. Rerunning
a window lands on the same content instead of duplicating it, and a republished
file updates in place.

The key was measured before it was trusted: 281,531 rows gave 281,531 distinct
keys in the 2004 file, and 51,685 gave 51,685 in a 2025 one. The uniqueness rule
still runs on every load anyway, because a key that holds in the files you
sampled may not hold in the ones you have not.

That is not hypothetical. Loading the full series, 28 million rows instead of 5,
the rule failed: 36 keys repeat, all of them inside a single 2005 file. Without
the check the merge would have picked among them arbitrarily and silver would
have been quietly wrong.

The idempotency test compares content, not counts. The same number of wrong
rows would pass a count.

## Repeated keys are two different problems

34 of those 36 keys repeat byte for byte. Two have the same key and different
prices. Treating them the same way would be wrong either way round.

An exact repeat is one observation published more than once, so the surplus
copies go to quarantine as `exact_duplicate` and one is kept. A key whose rows
disagree is a conflict in the source, and choosing a winner is exactly the
silent decision this pipeline exists to avoid, so every row of that key goes to
quarantine as `duplicate_key_conflict` and a person can decide.

Both land in quarantine rather than being dropped, which is also what keeps
`bronze == accepted + rejected` true. A deduplication that quietly removed rows
would break the one count that proves nothing went missing.

The quarantine is written scoped to the window being processed, not appended.
Appending looks harmless until a window is reprocessed and every rejected row
lands a second time, leaving a table that overstates the damage.

## The unit of measure is part of the gold grain

Prices come in three units: `R$ / litro`, `R$ / m³` for CNG, `R$ / 13 kg` for
LPG. With the unit as a plain descriptive column, a `GROUP BY state, product`
averages litres of petrol with 13 kg cylinders and returns a number with no
physical meaning, and nothing fails.

Putting the unit in the key makes that impossible to write by accident.
Structure that prevents the mistake beats documentation asking for care.

## Quality checks stop the run

Rules live in `src/fuel_lakehouse/dq/rules.yaml`. A critical breach raises and
the gate sits before the merge, so bad data never reaches the table gold reads
from. Warnings are recorded and the run continues.

Coverage of the purchase price is deliberately a warning. That column is 100%
empty from 2025 on, which is the normal state of this series rather than a
defect. Making it critical would turn the gate into an alarm everyone learns to
ignore, and then it protects nothing.

## The quality framework is written here rather than imported

Great Expectations, Soda and Pandera all do more than this. What is needed here
is a handful of known rules: not null, domain, range, unique key, ratio,
cross-layer reconciliation. The libraries bring stores, docs sites and
checkpoints that this project does not use, and the glue costs more than the
rules.

Cost: no ecosystem, no ready-made reporting. Worth it at this size, and not a
recommendation for every project.

## Files are discovered, not constructed from a pattern

The obvious approach is building URLs from a template. It does not survive
contact with what ANP publishes: four different filename shapes in 2026 alone,
one of them carrying a typo from the publisher
(`02-cados-abertos-preco-gasolina-etanol.csv`), April missing, one 2022 semester
as a ZIP among the CSVs, three naming schemes in the LPG series.

Discovery reads the index page and writes a manifest, which is versioned so a
change upstream shows up as a diff. Filenames nobody anticipated are recorded
with `unknown` status instead of vanishing. That paid off on the first run: it
caught a fifth shape, `precos-semestrais-ca.zip`, which has no year or period in
its name at all.

## Charset is detected per file

One file out of the 48 sampled is latin-1 while the other semester of the same
year is UTF-8 with a BOM. Reading with the wrong charset raises nothing. It
mangles every accented character in the file, municipality and reseller names
included, and the damage surfaces much later as groupings that do not match.

The charset is detected at download time, recorded in the sidecar, and carried
into the table as `_source_encoding`.

## MinIO instead of a cloud account

Paths are `s3a://`, so the code has no local mode branch and moving to S3 or
ADLS is a change of endpoint and credentials. It also means anyone can run the
whole thing with `docker compose up` and no cloud account, which matters more
than it sounds: a pipeline nobody can execute proves very little.
