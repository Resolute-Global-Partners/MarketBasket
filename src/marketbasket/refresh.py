"""Entry point — refresh per-state parquets in site/data/ from SQL.

Typical commands:
    uv run refresh --state IL --months 202405 202406      # add specific months
    uv run refresh --state IL --all-months                # rebuild IL fully
    uv run refresh --all                                  # rebuild everything
    uv run refresh --discover-only                        # list what's available in SQL

Design:
- Per-(state, month) queries (matches the WSL historical workflow that worked).
- Existing site/data/<STATE>.parquet is MERGED with newly-pulled months. If a
  requested month already exists, its rows are replaced.
- Top-N + Other bucketing runs across the merged multi-month DataFrame so the
  Other label stays stable across months.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

from . import sql
from .aggregate import fetch_and_aggregate
from .config import COMPANY_MAP_BY_STATE, COMPARISON_COMPANY_BY_STATE, GROUP_COLS
from .preprocess import apply_top_n_on_aggregated

ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "site" / "data"


def discover() -> dict[str, list[str]]:
    print("Discovering (state, month) combos via TABLESAMPLE(1%)...", flush=True)
    t0 = time.perf_counter()
    df = sql.discover_state_months(sample_percent=1.0)
    print(f"  {len(df)} pairs found in {time.perf_counter() - t0:.1f}s", flush=True)
    by_state: dict[str, list[str]] = defaultdict(list)
    for _, row in df.iterrows():
        st = str(row["State_Name"]).strip()
        ym = str(row["Year_Month"]).strip()
        if st and ym:
            by_state[st].append(ym)
    for s in by_state:
        by_state[s] = sorted(set(by_state[s]))
    return dict(sorted(by_state.items()))


def compute_missing(found: dict[str, list[str]]) -> dict[str, list[str]]:
    """Return {state: [yyyymm, ...]} with only months NOT already in
    site/data/<STATE>.parquet."""
    missing: dict[str, list[str]] = {}
    for state, ms in found.items():
        path = DATA_DIR / f"{state}.parquet"
        if path.exists():
            have = {str(m) for m in pd.read_parquet(path)["YYYYMM"].unique()}
            gap = [m for m in ms if m not in have]
        else:
            gap = list(ms)
        if gap:
            missing[state] = gap
    return missing


def order_for_cache_warmth(targets: dict[str, list[str]]) -> list[tuple[str, list[str]]]:
    """Do the state with the MOST months first — its early months pay the cold
    scan, later months and later states ride the warm cache."""
    return sorted(targets.items(), key=lambda kv: -len(kv[1]))


def load_existing_parquet(state: str) -> pd.DataFrame:
    """Load the current site/data/<STATE>.parquet if it exists, else empty frame."""
    path = DATA_DIR / f"{state}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def merge_and_write(
    state: str,
    existing: pd.DataFrame,
    new_chunks: list[pd.DataFrame],
    replaced_months: set[int],
    *,
    dry_run: bool,
) -> dict | None:
    """Combine existing + new monthly chunks, bucket, write, return index entry."""
    keep = existing[~existing["YYYYMM"].isin(replaced_months)] if not existing.empty else existing
    combined = pd.concat([keep] + new_chunks, ignore_index=True)
    if combined.empty:
        return None

    combined = apply_top_n_on_aggregated(combined, state, GROUP_COLS)

    out = DATA_DIR / f"{state}.parquet"
    if not dry_run:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        combined.to_parquet(out, index=False, compression="snappy")
    size_kb = out.stat().st_size / 1024 if out.exists() else 0
    print(f"  {state}: {len(combined):>7,} rows, {size_kb:>6.0f} KB  "
          f"({combined['YYYYMM'].nunique()} months)")

    return {
        "state": state,
        "rows": int(len(combined)),
        "months": sorted(int(m) for m in combined["YYYYMM"].unique()),
        "companies": sorted(combined["CompanyName"].unique().tolist()),
        "curated": state in COMPANY_MAP_BY_STATE,
        "comparison_company": COMPARISON_COMPANY_BY_STATE.get(state),
    }


def write_index(entries: list[dict]) -> None:
    """Rewrite index.json, preserving entries for states not touched by this run."""
    index_path = DATA_DIR / "index.json"
    existing_states: dict[str, dict] = {}
    if index_path.exists():
        try:
            existing_states = json.loads(index_path.read_text()).get("states", {})
        except Exception:
            pass
    for e in entries:
        existing_states[e["state"]] = e

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source": "sql",
        "states": existing_states,
    }
    index_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"\nIndex: {index_path} ({len(existing_states)} states total)")


def refresh_state(state: str, months: list[str], *, dry_run: bool) -> dict | None:
    print(f"\n== {state} -- pulling {len(months)} month(s): {', '.join(months)} ==", flush=True)
    existing = load_existing_parquet(state)
    if not existing.empty:
        print(f"  existing: {len(existing):,} rows, "
              f"{existing['YYYYMM'].nunique()} months")

    chunks: list[pd.DataFrame] = []
    for m in months:
        try:
            chunk = fetch_and_aggregate(state, m)
        except Exception as e:
            print(f"!! {state} {m}: {type(e).__name__}: {e}", file=sys.stderr)
            continue
        if not chunk.empty:
            chunks.append(chunk)

    if not chunks and existing.empty:
        print(f"  no data for {state}")
        return None

    return merge_and_write(
        state, existing, chunks,
        replaced_months={int(m) for m in months},
        dry_run=dry_run,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="refresh")
    p.add_argument("--state", help="a single state code, e.g. IL")
    p.add_argument("--months", nargs="*", help="yyyymm values (default: all)")
    p.add_argument("--all-months", action="store_true",
                   help="pull every month available for --state (via discovery)")
    p.add_argument("--all", action="store_true",
                   help="pull every state + every month (overwrites existing)")
    p.add_argument("--missing", action="store_true",
                   help="pull only months not already in site/data/*.parquet")
    p.add_argument("--discover-only", action="store_true",
                   help="print what's available in SQL and exit")
    p.add_argument("--dry-run", action="store_true",
                   help="run without writing parquet files")
    args = p.parse_args(argv)

    if args.discover_only:
        found = discover()
        print(f"\n{len(found)} states:")
        for st, ms in found.items():
            print(f"  {st}: {len(ms):>3}  {ms[0]} .. {ms[-1]}")
        return 0

    if args.missing:
        found = discover()
        targets = compute_missing(found)
        if args.state:
            targets = {s: ms for s, ms in targets.items() if s == args.state}
        if not targets:
            print("\nNothing to refresh — all months already on disk.", flush=True)
            return 0
        total_months = sum(len(v) for v in targets.values())
        print(f"\n{total_months} months missing across {len(targets)} state(s):", flush=True)
        for s, ms in order_for_cache_warmth(targets):
            print(f"  {s}: {len(ms):>3}  ({ms[0]} .. {ms[-1]})", flush=True)
    elif args.all:
        found = discover()
        targets = dict(found)
    elif args.state:
        if not args.months and not args.all_months:
            p.error("--state requires --months or --all-months")
        if args.all_months:
            found = discover()
            ms = found.get(args.state)
            if not ms:
                print(f"{args.state}: not found in discovery", file=sys.stderr)
                return 1
            targets = {args.state: ms}
        else:
            targets = {args.state: args.months}
    else:
        p.error("specify --state, --all, or --missing")

    ordered = order_for_cache_warmth(targets)

    entries: list[dict] = []
    for st, ms in ordered:
        e = refresh_state(st, ms, dry_run=args.dry_run)
        if e:
            entries.append(e)

    if not args.dry_run and entries:
        write_index(entries)

    return 0


if __name__ == "__main__":
    sys.exit(main())
