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
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

from . import sql
from .aggregate import (
    checkpoint_key, clear_checkpoints, fetch_and_aggregate,
    fetch_and_aggregate_state,
)
from .config import (
    ACTIVE_STATES, COMPANY_MAP_BY_STATE, COMPARISON_COMPANY_BY_STATE,
    CREDIT_CODE_ORDER, CREDIT_FORMULA_BY_STATE, GROUP_COLS,
    OUR_COMPANIES_BY_STATE, VALID_LIAB_BY_STATE, excluded_months,
)
from .preprocess import (
    apply_county_top_n_on_aggregated, apply_top_n_on_aggregated,
)

ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "docs" / "data"

# GitHub rejects any pushed file > 100 MiB. When a state's single parquet would
# exceed this, we split it into one file per month under data/<STATE>/ and the
# frontend unions them. 95 MiB leaves headroom below the hard limit.
SHARD_THRESHOLD_BYTES = 95 * 1024 * 1024

# States whose single-month tables are big enough that one query per table
# (fact_Rate_Car alone runs 25-50 min for a TX month) is itself the VPN risk.
# Their pulls go one Rate_Source slice at a time (sql.SOURCE_SLICES): twice
# the scans, ~20 min more per TX month, but no query runs much past half its
# unsplit length, and with checkpoints a drop costs one slice, not a month.
SPLIT_BY_SOURCE_STATES = {"TX"}


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
    """Return {state: [yyyymm, ...]} with only months NOT already published.

    Uses load_existing_parquet so the SHARDED layout counts too — reading only
    <STATE>.parquet would find nothing for a sharded state like TX and re-pull
    all of its months on every --missing run.

    Months in EXCLUDED_MONTHS_BY_STATE are never "missing": they are withheld
    on purpose, and pulling them would burn a full-table scan per month to
    produce rows merge_and_write then discards.
    """
    missing: dict[str, list[str]] = {}
    for state, ms in found.items():
        existing = load_existing_parquet(state)
        have = (set() if existing.empty
                else {str(m) for m in existing["YYYYMM"].unique()})
        skip = {str(m) for m in excluded_months(state)}
        gap = [m for m in ms if m not in have and m not in skip]
        if gap:
            missing[state] = gap
    return missing


def order_for_cache_warmth(targets: dict[str, list[str]]) -> list[tuple[str, list[str]]]:
    """Do the state with the MOST months first — its early months pay the cold
    scan, later months and later states ride the warm cache."""
    return sorted(targets.items(), key=lambda kv: -len(kv[1]))


def load_existing_parquet(state: str) -> pd.DataFrame:
    """Load current data for a state. Prefers the sharded layout
    (data/<STATE>/*.parquet) when present, else the single
    data/<STATE>.parquet, else an empty frame."""
    shard_dir = DATA_DIR / state
    if shard_dir.is_dir():
        parts = [pd.read_parquet(p) for p in sorted(shard_dir.glob("*.parquet"))]
        if parts:
            return pd.concat(parts, ignore_index=True)
    path = DATA_DIR / f"{state}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def write_state_data(state: str, combined: pd.DataFrame) -> tuple[int, list[str] | None]:
    """Write a state's combined frame to disk, sharding by month when a single
    file would exceed SHARD_THRESHOLD_BYTES. Returns (total_bytes, shards) where
    shards is None for the single-file layout or the list of relative shard
    paths otherwise. Keeps the two layouts mutually exclusive on disk."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    single = DATA_DIR / f"{state}.parquet"
    shard_dir = DATA_DIR / state

    combined.to_parquet(single, index=False, compression="snappy")
    single_size = single.stat().st_size

    if single_size <= SHARD_THRESHOLD_BYTES:
        if shard_dir.exists():           # drop any stale shard layout
            shutil.rmtree(shard_dir)
        return single_size, None

    # Over the limit — split into one parquet per month. The Top-N / county
    # bucketing already ran on the full combined frame, so shards stay mutually
    # consistent. The frontend re-unions them via read_parquet([...]).
    shard_dir.mkdir(parents=True, exist_ok=True)
    months = sorted(int(m) for m in combined["YYYYMM"].unique())
    keep = {f"{m}.parquet" for m in months}
    shards, total = [], 0
    for m in months:
        fp = shard_dir / f"{m}.parquet"
        combined[combined["YYYYMM"] == m].to_parquet(fp, index=False, compression="snappy")
        total += fp.stat().st_size
        shards.append(f"{state}/{m}.parquet")
    for p in shard_dir.glob("*.parquet"):   # prune months no longer present
        if p.name not in keep:
            p.unlink()
    single.unlink()                          # remove the over-limit single file
    return total, shards


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
    # Drop columns from older parquet schemas that no longer exist (e.g. CreditBin
    # was replaced by CreditCode in 2026-05; PayPlan was replaced by PayPlanType +
    # coverage flags in 2026-06). Otherwise pd.concat would carry them through
    # as NaN into the new aggregate.
    legacy_cols = ["CreditBin", "PayPlan"]
    keep = keep.drop(columns=[c for c in legacy_cols if c in keep.columns], errors="ignore")
    # RateSource was added 2026-09; months written before that have no such
    # column. A month-by-month rebuild mixes both schemas until it finishes, so
    # label the old rows explicitly instead of letting concat produce NaN — a
    # NaN group key would silently split every bucket in two.
    if not keep.empty and "RateSource" not in keep.columns:
        keep = keep.assign(RateSource="Unknown")
    parts = [k for k in [keep] if not k.empty] + new_chunks
    combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if combined.empty:
        return None

    # Withhold months whose SOURCE load is known-incomplete. Applied here, on
    # the merged frame, so the rule holds no matter how the rows arrived — a
    # fresh pull, an older parquet, or a partial re-run — and so the Top-N and
    # county bucketing below are computed without the bad months skewing them.
    excluded = excluded_months(state)
    if excluded:
        drop = combined["YYYYMM"].isin(excluded)
        if drop.any():
            print(f"  {state}: withholding {int(drop.sum()):,} rows from "
                  f"incomplete month(s) {sorted(excluded)} "
                  f"(see EXCLUDED_MONTHS_BY_STATE)")
            combined = combined[~drop]
        if combined.empty:
            return None

    combined = apply_top_n_on_aggregated(combined, state, GROUP_COLS)
    combined = apply_county_top_n_on_aggregated(combined, GROUP_COLS)

    shards: list[str] | None = None
    if not dry_run:
        total_bytes, shards = write_state_data(state, combined)
        layout = (f"{len(shards)} month-shards" if shards
                  else f"{combined['YYYYMM'].nunique()} months")
        print(f"  {state}: {len(combined):>7,} rows, {total_bytes/1024:>6.0f} KB  ({layout})")
    else:
        print(f"  {state}: {len(combined):>7,} rows (dry-run)  "
              f"({combined['YYYYMM'].nunique()} months)")

    counties = sorted(combined["County"].dropna().unique().tolist()) if "County" in combined.columns else []

    # Letter codes ordered per CREDIT_CODE_ORDER, only those actually present.
    if "CreditCode" in combined.columns:
        seen_codes = set(combined["CreditCode"].dropna().unique().tolist())
        credit_codes = [c for c in CREDIT_CODE_ORDER if c in seen_codes]
    else:
        credit_codes = []

    # Liab limits actually accepted by this state (mirrors VALID_LIAB_BY_STATE
    # so the frontend dropdown shows the right floor — 25/50 vs 30/60).
    liab_map = VALID_LIAB_BY_STATE.get(state, {})
    liab_limits = list(liab_map.values())

    return {
        "state": state,
        "rows": int(len(combined)),
        "months": sorted(int(m) for m in combined["YYYYMM"].unique()),
        "companies": sorted(combined["CompanyName"].unique().tolist()),
        "counties": counties,
        "credit_codes": credit_codes,
        "liab_limits": liab_limits,
        "has_credit_formula": state in CREDIT_FORMULA_BY_STATE,
        "curated": state in COMPANY_MAP_BY_STATE,
        "comparison_company": COMPARISON_COMPANY_BY_STATE.get(state),
        "our_companies": OUR_COMPANIES_BY_STATE.get(state, []),
        "shards": shards,
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


def prune_inactive_states(*, dry_run: bool) -> None:
    """Delete parquet files and index entries for states not in ACTIVE_STATES."""
    index_path = DATA_DIR / "index.json"
    removed_files: list[str] = []
    for path in DATA_DIR.glob("*.parquet"):
        if path.stem not in ACTIVE_STATES:
            removed_files.append(path.name)
            if not dry_run:
                path.unlink()
    # Sharded states live in data/<STATE>/ — drop orphan shard dirs too.
    for path in DATA_DIR.iterdir():
        is_state_shard_dir = (
            path.is_dir() and path.name.isalpha() and len(path.name) == 2
            and any(path.glob("*.parquet"))
        )
        if is_state_shard_dir and path.name not in ACTIVE_STATES:
            removed_files.append(f"{path.name}/")
            if not dry_run:
                shutil.rmtree(path)

    if index_path.exists():
        try:
            payload = json.loads(index_path.read_text())
            states = payload.get("states", {})
            inactive = [s for s in states if s not in ACTIVE_STATES]
            for s in inactive:
                states.pop(s, None)
            if inactive and not dry_run:
                payload["states"] = states
                index_path.write_text(json.dumps(payload, indent=2, default=str))
        except Exception:
            inactive = []
    else:
        inactive = []

    if removed_files or inactive:
        verb = "Would remove" if dry_run else "Removed"
        print(f"\n{verb} inactive states: parquets={removed_files}, index entries={inactive}")


def refresh_state(
    state: str,
    months: list[str],
    *,
    dry_run: bool,
    failures: list[tuple[str, int]] | None = None,
    fallback: bool = True,
    checkpoint: bool = True,
) -> dict | None:
    """Pull `months` for `state` and merge them into the state's parquet.

    Months that fail to pull (dropped VPN, server timeout) are reported and
    appended to `failures` as (state, yyyymm). They are NOT treated as
    replaced, so an interrupted run leaves previously-good months untouched
    instead of deleting them.

    fallback=False skips the per-month retry of a failed batch. A caller that
    retries the whole unit itself (scripts/rebuild.py) wants that: the
    fallback costs 4 scans per month, and one run of it took 11 hours.
    """
    print(f"\n== {state} -- pulling {len(months)} month(s): {', '.join(months)} ==", flush=True)
    existing = load_existing_parquet(state)
    if not existing.empty:
        print(f"  existing: {len(existing):,} rows, "
              f"{existing['YYYYMM'].nunique()} months")

    # For multi-month refreshes, one full-table scan beats N. For a single
    # month the per-(state, month) path is identical cost.
    chunks: list[pd.DataFrame] = []
    batched = len(months) > 1
    batch_ok = False
    if batched:
        try:
            chunks = fetch_and_aggregate_state(state, months, checkpoint=checkpoint)
            batch_ok = True
        except Exception as e:
            print(f"!! {state} batched fetch failed: {type(e).__name__}: {e}", file=sys.stderr)
            if fallback:
                print(f"   falling back to per-month pulls", file=sys.stderr)
            chunks = []
    if not chunks and months and (fallback or not batched):
        for m in months:
            try:
                chunk = fetch_and_aggregate(
                    state, m, checkpoint=checkpoint,
                    split_sources=state in SPLIT_BY_SOURCE_STATES,
                )
            except Exception as e:
                print(f"!! {state} {m}: {type(e).__name__}: {e}", file=sys.stderr)
                continue
            if not chunk.empty:
                chunks.append(chunk)

    if not chunks and existing.empty:
        print(f"  no data for {state}")
        return None

    # Only months we actually pulled data for may replace what's on disk.
    # Using the full requested set here would drop an existing good month
    # whenever its re-pull failed — a silent data loss on every flaky run,
    # and the reason a half-finished refresh could shrink a state.
    pulled: set[int] = set()
    for c in chunks:
        if not c.empty:
            pulled.update(int(v) for v in c["YYYYMM"].unique())

    lost = [int(m) for m in months if int(m) not in pulled]
    if lost:
        for m in lost:
            print(f"!! {state} {m}: no rows returned — keeping existing data for "
                  f"that month (NOT replaced)", file=sys.stderr)
            if failures is not None:
                failures.append((state, m))

    entry = merge_and_write(
        state, existing, chunks,
        replaced_months=pulled,
        dry_run=dry_run,
    )

    # The pulled months are in the parquet now, so their raw checkpoints have
    # done their job. Failed months keep theirs for the re-run to resume from.
    if entry is not None and not dry_run:
        if batch_ok:
            clear_checkpoints(checkpoint_key(state, months))
        for m in pulled:
            clear_checkpoints(checkpoint_key(state, [str(m)]))
    return entry


#: A month whose Quotes fall below this fraction of the state's median month
#: is treated as truncated rather than as a genuine dip in shopping volume.
TRUNCATION_RATIO = 0.55


def verify(states: list[str] | None = None) -> list[tuple[str, int, int, float]]:
    """Flag months that look truncated — the signature of an interrupted pull.

    A refresh that dies partway (dropped VPN, machine crash) can leave a month
    holding only the rows that made it over the wire. The month is present, so
    `--missing` will never re-pull it; it just sits in the published data
    under-counted. Comparing each month against its state's median catches it.

    Local only — reads the published parquets, no SQL. Returns the offenders as
    (state, yyyymm, quotes, ratio_to_median).
    """
    index_path = DATA_DIR / "index.json"
    known = json.loads(index_path.read_text()).get("states", {}) if index_path.exists() else {}
    targets = states or sorted(known)

    bad: list[tuple[str, int, int, float]] = []
    for state in targets:
        df = load_existing_parquet(state)
        if df.empty:
            print(f"  {state}: NO DATA", flush=True)
            continue
        per_month = df.groupby("YYYYMM")["Quotes"].sum().sort_index()
        median = float(per_month.median())
        print(f"\n  {state}: {len(per_month)} months, median {median:,.0f} quotes/mo")
        for m, q in per_month.items():
            ratio = float(q) / median if median else 0.0
            mark = ""
            if ratio < TRUNCATION_RATIO:
                mark = f"   <<< TRUNCATED? {ratio:.0%} of median"
                bad.append((state, int(m), int(q), ratio))
            print(f"    {int(m)}  {int(q):>12,}  {ratio:>6.0%}{mark}")
    return bad


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
    p.add_argument("--verify", action="store_true",
                   help="check published parquets for truncated months (no SQL) and exit")
    p.add_argument("--dry-run", action="store_true",
                   help="run without writing parquet files")
    p.add_argument("--no-fallback", action="store_true",
                   help="if a multi-month batch fails, fail the run instead of "
                        "retrying month by month (for callers that retry themselves)")
    p.add_argument("--no-checkpoint", action="store_true",
                   help="don't save raw pulls to cache/pull/ for a re-run to resume from")
    args = p.parse_args(argv)

    if args.verify:
        print("Verifying published parquets for truncated months...")
        bad = verify([args.state] if args.state else None)
        if bad:
            print(f"\n!! {len(bad)} suspect month(s):", file=sys.stderr)
            by_state: dict[str, list[str]] = defaultdict(list)
            for st, m, q, ratio in bad:
                print(f"     {st} {m}: {q:,} quotes ({ratio:.0%} of median)", file=sys.stderr)
                by_state[st].append(str(m))
            print("   re-pull:", file=sys.stderr)
            for st, ms in by_state.items():
                print(f"     uv run refresh --state {st} --months {' '.join(ms)}", file=sys.stderr)
            return 1
        print("\nAll months look complete.")
        return 0

    if args.discover_only:
        found = discover()
        print(f"\n{len(found)} states:")
        for st, ms in found.items():
            print(f"  {st}: {len(ms):>3}  {ms[0]} .. {ms[-1]}")
        return 0

    if args.missing:
        found = discover()
        found = {s: ms for s, ms in found.items() if s in ACTIVE_STATES}
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
        targets = {s: ms for s, ms in found.items() if s in ACTIVE_STATES}
        print(f"\nActive states: {sorted(ACTIVE_STATES)}", flush=True)
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
    failures: list[tuple[str, int]] = []
    for st, ms in ordered:
        e = refresh_state(st, ms, dry_run=args.dry_run, failures=failures,
                          fallback=not args.no_fallback,
                          checkpoint=not args.no_checkpoint)
        if e:
            entries.append(e)

    if not args.dry_run and entries:
        write_index(entries)

    # When refreshing the full active set, drop any stale parquets/index entries
    # for states no longer in ACTIVE_STATES.
    if args.all:
        prune_inactive_states(dry_run=args.dry_run)

    # A partially-failed run must not look like a success — the published data
    # is now a mix of fresh and stale months, and the caller needs to re-run
    # the gaps before committing.
    if failures:
        print(f"\n!! {len(failures)} month(s) FAILED to pull — existing data kept "
              f"for these, nothing was overwritten:", file=sys.stderr)
        for st, m in sorted(failures):
            print(f"     {st} {m}", file=sys.stderr)
        by_state: dict[str, list[str]] = defaultdict(list)
        for st, m in sorted(failures):
            by_state[st].append(str(m))
        print("   re-run:", file=sys.stderr)
        for st, ms in by_state.items():
            print(f"     uv run refresh --state {st} --months {' '.join(ms)}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
