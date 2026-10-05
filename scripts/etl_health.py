"""Is the upstream MarketUnified load healthy? Run this BEFORE any refresh.

`dbo.DataLineage` records every ETL load as
(State_Name, Rate_Source, Year_Month, Table_Name, RowsLoaded, LoadedDate).
It is ~4k rows, so this answers in seconds what a GROUP BY on fact_Rate takes
4-6 minutes to answer (282M rows, zero indexes) — and unlike the fact tables it
distinguishes "the load never ran" from "the load ran and failed", which is the
distinction that actually tells you whose problem it is.

A healthy month = 15 fact_Rate loads (5 states x 3 sources), ~23-27M rows.

    uv run python scripts/etl_health.py
    uv run python scripts/etl_health.py --months 6
"""
from __future__ import annotations

import argparse

import pandas as pd
from dataloader import load

EXPECTED_FACT_RATE_LOADS = 15          # 5 states x 3 Rate_Sources
HEALTHY_MIN_ROWS = 15_000_000          # a real month is ~23-27M


def main() -> int:
    p = argparse.ArgumentParser(prog="etl_health")
    p.add_argument("--months", type=int, default=8,
                   help="how many recent Year_Months to report (default 8)")
    args = p.parse_args()

    df = load("SELECT * FROM dbo.DataLineage WITH (NOLOCK)", "MarketUnified")
    for c in ("State_Name", "Rate_Source", "Year_Month", "Table_Name"):
        df[c] = df[c].astype(str).str.strip()

    months = sorted(df["Year_Month"].unique())[-args.months:]
    fr = df[df["Table_Name"] == "fact_Rate"]

    print(f"Latest load recorded: {df['LoadedDate'].max()}")
    print(f"Year_Months in lineage: {sorted(df['Year_Month'].unique())[-1]} "
          f"(showing last {len(months)})\n")

    print(f"{'month':<8} {'fact_Rate loads':>16} {'rows':>14}  {'loaded':<20} status")
    problems: list[str] = []
    for m in months:
        g = fr[fr["Year_Month"] == m]
        loads = len(g)
        rows = int(g["RowsLoaded"].sum()) if loads else 0
        when = str(g["LoadedDate"].max())[:19] if loads else "-"
        if loads == 0:
            status = "FAILED - no fact_Rate loaded at all"
        elif loads < EXPECTED_FACT_RATE_LOADS:
            missing = _missing_slices(g)
            status = f"PARTIAL - missing {missing}"
        elif rows < HEALTHY_MIN_ROWS:
            status = "SUSPECT - load count ok but row count low"
        else:
            status = "ok"
        if status != "ok":
            problems.append(f"{m}: {status}")
        print(f"{m:<8} {loads:>16} {rows:>14,}  {when:<20} {status}")

    print()
    if problems:
        print(f"!! {len(problems)} month(s) need an upstream reload:")
        for pr in problems:
            print(f"   {pr}")
        print("\nThis is a data-platform problem - re-pulling cannot fix it.")
        print("Once reloaded: `uv run refresh --missing`, then drop the matching")
        print("entries from EXCLUDED_MONTHS_BY_STATE in config.py.")
        return 1

    print("All reported months loaded cleanly.")
    return 0


def _missing_slices(g: pd.DataFrame) -> str:
    """Which (state, source) pairs did not load for this month."""
    got = set(zip(g["State_Name"], g["Rate_Source"]))
    want = {(s, src) for s in ("AZ", "IL", "IN", "TN", "TX")
            for src in ("API", "TR", "TFW")}
    gaps = sorted(f"{s}/{src}" for s, src in want - got)
    return ", ".join(gaps) if gaps else "(unknown)"


if __name__ == "__main__":
    raise SystemExit(main())
