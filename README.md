# Market Basket

Web frontend that displays the same interactive quote/bridging tables as the
Excel version, but fed straight from **PRD-SQL-02 → parquet → static site**.
End users open a link and never touch SQL or the VPN.

**Live site:** https://resolute-global-partners.github.io/MarketBasket/

**Pipeline spec:** [TRANSFORMATIONS.md](TRANSFORMATIONS.md) — the current
transformation logic, step by step. (The untracked
`MarketBasket_Transformations.pdf` is the April 2026 version and describes the
retired 14-plan / 10-dimension pipeline; it is superseded.)

**Current data:** IL (20 mo, 202501–202608), AZ (13 mo, through 202608), IN (12 mo,
202509–202608), TN (12 mo, through 202608), TX (10 mo, 202509–202606). TX is
behind the others — see [Known data gaps](#known-data-gaps).

## Architecture

```
+- monthly, on YOUR machine (needs VPN) ---------------------------------+
|                                                                        |
|   uv run refresh --missing                                             |
|     |   queries MarketUnified.fact_Rate + _Car/_Driver/_Violation      |
|     |   collapses rate->policy, aggregates to a 16-dim group-by        |
|     v                                                                  |
|   docs/data/<STATE>.parquet  +  docs/data/index.json                  |
|                                                                        |
+-------------------------------------+---------------------------------+
                                      |  git add + commit + push
                                      v
+- GitHub Pages (public repo) ----------------------------------------+
|   https://resolute-global-partners.github.io/MarketBasket/           |
|   Served from /docs branch: main                                      |
+-------------------------------------+--------------------------------+
                                      |  URL
                                      v
+- User (any device, no VPN) -----------------------------------------+
|   loads page -> picks state -> parquet cached in browser             |
|   all filters run through DuckDB-WASM in-browser (ms-level)          |
+---------------------------------------------------------------------+
```

## Monthly refresh — the one workflow you'll actually use

Connect to the VPN, then:

```bash
cd C:/Users/dchemaly/Documents/MarketBasket
uv run refresh --missing        # pulls only months not already on disk
git add docs/data && git commit -m "data $(date +%Y-%m)" && git push
```

`--missing` is the magic flag: it calls `TABLESAMPLE` on `fact_Rate` to
discover every (state, month) combo in SQL, compares that list to what's
already in `docs/data/*.parquet`, and pulls only the diff. If MarketUnified
got a new month of data, it pulls that one. If a new state showed up, it
pulls all of it.

**Timing:** first month of a table = ~7 min cold scan, subsequent months
~3-5 min each (buffer pool warm). Budget 15-20 min for a single-state / single-
month refresh, 3-5 hours for a first-time 50-state initial load.

## All commands

```bash
# The daily driver — pull any (state, month) combo not already on disk
uv run refresh --missing

# Restrict "missing" to just one state
uv run refresh --missing --state IL

# Force-pull specific months for one state (even if already on disk)
uv run refresh --state IL --months 202605 202606

# Rebuild a state completely — pulls every month available in SQL
uv run refresh --state IL --all-months

# Full refresh everything from SQL (long, overwrites existing)
uv run refresh --all

# Just print what's available (no pulling)
uv run refresh --discover-only

# Check the PUBLISHED parquets for truncated months (local, no SQL, seconds).
# Flags any month under 55% of its state's median — the signature of a pull
# that died partway. Exits non-zero and prints the re-pull commands.
uv run refresh --verify
uv run refresh --verify --state TX

# --dry-run works with any of the above (no parquet writes)
uv run refresh --missing --dry-run
```

### Full rebuild (crash-safe)

`--all-months` does a whole state in one process, which OOMs on TX and loses
all progress if the machine dies. Use the driver instead — one subprocess per
unit of work, a JSON ledger of completed units, and resume-on-restart:

```bash
uv run python scripts/rebuild.py --plan     # show the plan, pull nothing
uv run python scripts/rebuild.py            # all states (overnight)
uv run python scripts/rebuild.py --states TX
uv run python scripts/rebuild.py --reset    # forget the ledger and start over
```

Kill it at any point and re-run the same command; completed units are skipped.
Per-unit logs land in `scripts/_rebuild_logs/`.

### Preview the site locally

```bash
cd C:/Users/dchemaly/Documents/MarketBasket/docs && python -m http.server 8000
# open http://localhost:8000
```

DuckDB-WASM won't read `file://` URLs, so a static server is required.

## Project structure

```
MarketBasket/
+-- pyproject.toml
+-- src/marketbasket/
|   +-- config.py             # Company maps + pay-plan labels + bins
|   +-- preprocess.py         # Row-level prep (ported from WSL data.py)
|   +-- aggregate.py          # Merge + groupby per (state, month)
|   +-- sql.py                # MarketUnified queries + TABLESAMPLE discovery
|   +-- refresh.py            # Main CLI -- `uv run refresh`
|   +-- refresh_local.py      # Alternative CLI that reads local parquet dumps
|                             # (used when SQL is unavailable; `uv run refresh-local`)
+-- docs/                     # GitHub Pages serves this folder (branch: main, /docs)
    +-- index.html            # Filters + grid shell
    +-- app.js                # DuckDB-WASM + AG-Grid logic
    +-- style.css
    +-- data/                 # Generated by refresh (committed to repo)
        +-- index.json        # State registry + metadata
        +-- IL.parquet        # single-file layout
        +-- TX/               # SHARDED layout: one parquet per month, used
        |   +-- 202509.parquet    # when a single file would exceed 95 MiB
        |   +-- ...               # (GitHub rejects pushes over 100 MiB).
        +-- ...                   # index.json carries a "shards" list and the
                                  # frontend unions them via read_parquet([..]).
```

Plus `scripts/rebuild.py` (crash-safe full-rebuild driver) and its
`_rebuild_ledger.json` / `_rebuild_logs/`.

## Curation rules (matching the Excel behaviour)

All five active states now have an **exhaustive** curated map (they are all in
`EXHAUSTIVE_MAP_STATES`), so every unmapped `CompanyId` collapses into a single
`Other` row — no top-N-by-ID survivors.

| State | Curated map | Reference columns |
|---|---|---|
| **IL** | 19 IDs -> 13 named | vs UIC + vs SIC |
| **AZ** | 16 IDs -> 14 named | vs SunCoast + vs SIC |
| **TX** | 32 IDs -> 24 named | vs Lamar Platinum |
| **TN** | 19 IDs -> 14 named | vs UIC |
| **IN** | 10 IDs -> 10 named | vs UIC |

### Adding a new curated state

1. Edit `src/marketbasket/config.py`:
   - Add entry to `COMPANY_MAP_BY_STATE` (CompanyId -> display name)
   - Add entry to `COMPARISON_COMPANY_BY_STATE` (state -> reference company name)
   - Optionally add to `EXHAUSTIVE_MAP_STATES` if ALL unmapped IDs should collapse to one Other row (like IL)
2. Run `uv run refresh --state <X> --all-months`
3. `git add docs/data && git commit -m "add <X>" && git push`

## Rate Source filter

`MarketUnified.fact_Rate.Rate_Source` is now pulled, grouped on, and exposed as
the **Rate Source** dropdown. It holds three codes:

| Code | Rows (all states) | Bind rate |
|---|---|---|
| `API` | 150,087,509 | 0.44% |
| `TR`  | 129,430,050 | 3.91% |
| `TFW` |   2,589,390 | 0.09% |

**The codes are shown raw, on purpose.** Fingerprinting them (ThirdPartyId
shape, carrier counts, bind rates) did *not* conclusively establish which is
ITC and which is EZ Lynx. The plausible reading is that `TR`/`TFW` are the
TurboRater family (ITC) and `API` is a programmatic feed, but that is a guess
from the names — exactly the mistake that the Diamond-naming rule exists to
prevent. **Confirm the mapping with the data owner before relabelling them**,
then change `RATE_SOURCE_CODES` in `config.py` and the dropdown labels.

Note the pipeline was *already ingesting all three sources* while the UI
claimed everything was ITC — so historical "ITC" figures were really all
platforms combined.

`RateSource` sits in `POLICY_COLS` and allows "Any": only 0.67% of
`PolicyLinkID`s span more than one source (TX 1.05%, others 0.10–0.20%), so an
"Any" total can exceed distinct policies by at most ~0.7%.

## Known data gaps

**Run this first, before any refresh — it takes seconds:**

```bash
uv run python scripts/etl_health.py
```

It reads `MarketUnified.dbo.DataLineage` (~4k rows, no table scan), which
records `RowsLoaded` per (state, source, month), and reports any month that is
missing or partial — naming the exact `state/source` slices that failed. A
healthy month is **15 `fact_Rate` loads** (5 states × 3 sources), ~23–27M rows.
Unlike the fact tables it distinguishes *never ran* from *ran and failed*,
which is what tells you whether the fix is yours or the data platform's.

Current output (2026-10-02):

```
202606   15   22,997,497   ok
202607   15   23,199,169   ok  (but see TX below - child tables incomplete)
202608   14    9,249,189   PARTIAL - missing TX/TR
```

**`etl_health.py` only counts `fact_Rate` loads.** It will not catch a slice
whose child tables (`fact_Rate_Car/_Driver/_Violation`) are missing — check
`DataLineage` for those too before trusting a month.

- **TX 202607:** reloaded upstream 2026-10-01, but only `fact_Rate` for the TR
  source (10.4M rows). TR `fact_Rate_Car`, `_Driver` and `_Violation` were never
  loaded (API/TFW have them; TR has them in 202606). Withheld via
  `EXCLUDED_MONTHS_BY_STATE`; TX is published through 202606.
- **TX 202608:** TR slice absent entirely (API 4.9M + TFW 0.3M only). Withheld.
- **AZ/IL/IN/TN 202608:** loaded and published. Volumes are below their
  medians (TN 73%, AZ 79%, IN 84%, IL 96%) — months are assigned by `RatedDate`
  (not snapshot partition), so this comes from the source data; cause not yet
  explained.
- **IL 202405–202412** only ever loaded ~10–12k rows/month (vs ~1.4M from
  202501). Withheld; IL history starts 202501.
- Latest `RatedDate` in `fact_Rate` is 2026-09-18 (in the 202608 partition).

The TX gaps are upstream; re-pulling cannot fix them. Once the TR slices
(including child tables) are loaded, run `uv run refresh --missing` and delete
the TX entries from `EXCLUDED_MONTHS_BY_STATE`.

## Troubleshooting

**`VPN/PRD-SQL-02 unreachable`** — `refresh` needs VPN. End users never do.

**First query is agonizingly slow, subsequent queries are fast** — expected.
`fact_Rate` has no indexes, so the first scan per table is 5-10 min while
SQL Server reads ~100 GB from disk into its buffer pool. Once warm, queries
on the same table complete in 2-5 min (mostly VPN transfer). Don't run two
refreshes concurrently — they'll fight for the same cache.

**Site loads but the grid is empty** — open DevTools -> Console. The usual
culprit is a path mismatch: the server has to be rooted at the `docs/`
directory so `fetch("data/index.json")` resolves.

**Browser seems to show stale data after refresh** — hard-refresh
(Ctrl+Shift+R). The app already uses `cache: "no-store"` on fetches, but some
browsers still aggressively cache. A single hard-refresh clears it.

**`out of range DateTime` from ConnectorX/pandas** — a new row has
`RatedDate = 9999-12-31` (a sentinel value). Patch the query in `sql.py` with
a defensive `AND RatedDate < '2100-01-01'`.
