"""Refresh site/data/ from LOCAL parquet dumps at C:/Temp/PNCDataDump/MarketUnified/.

Used for the demo / during development while we sort out a tractable SQL
refresh path. The production refresh eventually goes through refresh.py
(MarketUnified SQL queries) — but that requires either an index on
fact_Rate.(State_Name, Year_Month) or a per-state-per-month query pattern
with narrow result sets.

Usage:
    uv run refresh-local                # IL and AZ, all months present
    uv run refresh-local --states IL    # just IL
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

from .aggregate import _aggregate_one_state
from .config import COMPANY_MAP_BY_STATE, COMPARISON_COMPANY_BY_STATE, GROUP_COLS
from .preprocess import apply_top_n_on_aggregated

ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "docs" / "data"

DUMP_ROOT = Path("C:/Temp/PNCDataDump/MarketUnified")
RATE_DIR = DUMP_ROOT / "fact_Rate"
CAR_DIR = DUMP_ROOT / "fact_Rate_Car"
DRV_DIR = DUMP_ROOT / "fact_Rate_Driver"
VIOL_DIR = DUMP_ROOT / "fact_Rate_Violation"

_FNAME_RE = re.compile(r"fact_Rate_([A-Z]{2})_(\d{6})\.parquet$")


def scan_state_months() -> dict[str, list[str]]:
    """Return {state: sorted_months} based on which fact_Rate_*.parquet files exist."""
    by_state: dict[str, list[str]] = defaultdict(list)
    for p in sorted(glob.glob(str(RATE_DIR / "fact_Rate_*.parquet"))):
        m = _FNAME_RE.search(os.path.basename(p))
        if m:
            by_state[m.group(1)].append(m.group(2))
    for s in by_state:
        by_state[s] = sorted(by_state[s])
    return dict(sorted(by_state.items()))


def process_one(state: str, yyyymm: str) -> pd.DataFrame:
    rate_path = RATE_DIR / f"fact_Rate_{state}_{yyyymm}.parquet"
    car_path  = CAR_DIR  / f"fact_Rate_Car_{state}_{yyyymm}.parquet"
    drv_path  = DRV_DIR  / f"fact_Rate_Driver_{state}_{yyyymm}.parquet"
    viol_path = VIOL_DIR / f"fact_Rate_Violation_{state}_{yyyymm}.parquet"

    if not rate_path.exists():
        return pd.DataFrame()

    rate = pd.read_parquet(rate_path)
    car  = pd.read_parquet(car_path)  if car_path.exists()  else pd.DataFrame()
    drv  = pd.read_parquet(drv_path)  if drv_path.exists()  else pd.DataFrame()
    viol = pd.read_parquet(viol_path) if viol_path.exists() else pd.DataFrame()

    return _aggregate_one_state(state, yyyymm, rate, car, drv, viol)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="refresh-local")
    p.add_argument("--states", nargs="*", help="restrict to these state codes")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    available = scan_state_months()
    if not available:
        print(f"No parquet files found under {DUMP_ROOT}", file=sys.stderr)
        return 1

    targets = {s: ms for s, ms in available.items()
               if not args.states or s in args.states}
    if not targets:
        print(f"No matching states. Available: {list(available)}", file=sys.stderr)
        return 1

    print(f"Processing {len(targets)} state(s):")
    for s, ms in targets.items():
        print(f"  {s}: {len(ms):>3} months ({ms[0]} .. {ms[-1]})")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    entries = []

    for state, months in targets.items():
        print(f"\n-- {state} --")
        chunks = []
        for m in months:
            t0 = time.perf_counter()
            agg = process_one(state, m)
            print(f"  {m}: {len(agg):>6,} agg rows   {time.perf_counter() - t0:5.1f}s")
            if not agg.empty:
                chunks.append(agg)

        if not chunks:
            continue
        fresh = pd.concat(chunks, ignore_index=True)

        # Merge with existing parquet: keep months we're NOT refreshing here,
        # drop stale rows for months we're overwriting, re-aggregate Top-N
        # across the full merged set so the Other bucket is consistent.
        out = DATA_DIR / f"{state}.parquet"
        refreshed_months = {int(m) for m in months}
        if out.exists():
            existing = pd.read_parquet(out)
            keep = existing[~existing["YYYYMM"].isin(refreshed_months)]
            full = pd.concat([keep, fresh], ignore_index=True)
        else:
            full = fresh

        full = apply_top_n_on_aggregated(full, state, GROUP_COLS)

        if not args.dry_run:
            full.to_parquet(out, index=False, compression="snappy")
            size_kb = out.stat().st_size / 1024
            print(f"  -> wrote {out.name}: {len(full):,} rows, {size_kb:.0f} KB  "
                  f"({full['YYYYMM'].nunique()} months)")

        entries.append({
            "state": state,
            "rows": int(len(full)),
            "months": sorted(int(m) for m in full["YYYYMM"].unique()),
            "companies": sorted(full["CompanyName"].unique().tolist()),
            "curated": state in COMPANY_MAP_BY_STATE,
            "comparison_company": COMPARISON_COMPANY_BY_STATE.get(state),
        })

    if not args.dry_run and entries:
        # Merge with existing index so untouched states keep their metadata.
        index_path = DATA_DIR / "index.json"
        existing_states: dict = {}
        if index_path.exists():
            try:
                existing_states = json.loads(index_path.read_text()).get("states", {})
            except Exception:
                pass
        for e in entries:
            existing_states[e["state"]] = e
        payload = {
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "source": "mixed (local+sql)",
            "states": existing_states,
        }
        index_path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"\nIndex: {index_path} ({len(existing_states)} states total)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
