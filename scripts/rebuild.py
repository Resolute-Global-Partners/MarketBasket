"""Crash-safe driver for a full (or partial) re-pull.

Why this exists
---------------
`uv run refresh --state X --all-months` does the whole state in one process.
That is fine for the small states but has two failure modes we have actually
hit:

  * TX loads every month's four raw tables into RAM at once and OOMs.
  * If the machine dies partway, the run leaves no record of how far it got,
    so the restart redoes everything.

This driver runs ONE subprocess per unit of work (a whole state for the small
states, a single month for RAM-bound ones), records each completed unit in a
JSON ledger, and skips completed units on restart. Kill it at any point and
re-run the same command — it picks up where it stopped.

Usage
-----
    uv run python scripts/rebuild.py --plan                 # show what it would do
    uv run python scripts/rebuild.py                        # all states
    uv run python scripts/rebuild.py --states TX            # just TX
    uv run python scripts/rebuild.py --reset                # forget the ledger
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "scripts" / "_rebuild_ledger.json"
LOG_DIR = ROOT / "scripts" / "_rebuild_logs"

# TX's four raw tables for a single month are already several GB; the batched
# multi-month path OOMs on it. One process per month keeps peak RAM bounded
# because each process frees everything on exit. Within the month, refresh.py
# also splits each TX query by Rate_Source (SPLIT_BY_SOURCE_STATES).
MONTH_BY_MONTH = {"TX"}

# Cap how many months go into one subprocess. A 12-month AZ pull spent 69
# minutes on a single fact_Rate_Car query and then lost the whole batch to a
# transient VPN drop (TCP 10060) during fact_Rate_Driver. Shorter pulls are
# less exposed, and when one does fail it costs a chunk instead of the state.
# refresh.py still batches within a chunk, so this is 4 scans per chunk, not
# 4 per month -- far cheaper than the per-month fallback it replaces.
MAX_MONTHS_PER_UNIT = 6

# Transient network failures are the common case here, not bad data, so give
# a failed unit several more goes before moving on. Every raw pull is
# checkpointed (cache/pull/), so a retry resumes from the last finished query
# rather than starting the unit over — which makes retries cheap enough to
# wait out a real outage. The 9/28-29 TN run lost both its units to ~25 min of
# "cannot contact a domain controller"; a flat 2-min pause spent all three
# attempts inside it. Pauses escalate to span ~an hour in total.
RETRY_PAUSES_S = [120, 600, 1200, 1800]
UNIT_ATTEMPTS = len(RETRY_PAUSES_S) + 1


def load_ledger() -> dict:
    if LEDGER.exists():
        try:
            return json.loads(LEDGER.read_text())
        except Exception:
            pass
    return {"done": [], "started": None}


def save_ledger(led: dict) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text(json.dumps(led, indent=2))


def discover_months() -> dict[str, list[str]]:
    """Ask the pipeline itself what months exist, so the plan can't drift from
    what refresh.py would pull.

    Withheld months (EXCLUDED_MONTHS_BY_STATE) are dropped here: pulling one
    costs four full-table scans to produce rows merge_and_write then throws
    away. For TX that is a whole wasted unit of work.
    """
    sys.path.insert(0, str(ROOT / "src"))
    from marketbasket.config import ACTIVE_STATES, excluded_months  # noqa: E402
    from marketbasket.refresh import discover  # noqa: E402

    found = discover()
    out: dict[str, list[str]] = {}
    for s, ms in found.items():
        if s not in ACTIVE_STATES:
            continue
        skip = {str(m) for m in excluded_months(s)}
        kept = [m for m in ms if m not in skip]
        if skip:
            print(f"  {s}: skipping withheld month(s) {sorted(skip)}")
        if kept:
            out[s] = kept
    return out


def build_units(targets: dict[str, list[str]]) -> list[tuple[str, list[str]]]:
    """A unit is one subprocess: (state, months).

    RAM-bound states go one month at a time; everything else is split into
    chunks of at most MAX_MONTHS_PER_UNIT so a single long query cannot take
    the whole state down with it.
    """
    units: list[tuple[str, list[str]]] = []
    for state, months in sorted(targets.items(), key=lambda kv: -len(kv[1])):
        size = 1 if state in MONTH_BY_MONTH else MAX_MONTHS_PER_UNIT
        ms = list(months)
        for i in range(0, len(ms), size):
            units.append((state, ms[i:i + size]))
    return units


def unit_key(state: str, months: list[str]) -> str:
    return f"{state}:{months[0]}-{months[-1]}" if months else f"{state}:none"


def run_unit(state: str, months: list[str]) -> tuple[bool, float]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / f"{state}_{months[0]}_{months[-1]}.log"
    # --no-fallback: this driver does the retrying. refresh.py's own fallback
    # re-pulls a failed batch month by month (4 scans per month); one TN run
    # of it took 11 hours before failing anyway.
    cmd = ["uv", "run", "refresh", "--state", state, "--months", *months,
           "--no-fallback"]
    t0 = time.perf_counter()
    # Append, not truncate: a retry must not erase the previous attempt's
    # error, which is the only record of why the first try failed.
    with log.open("a", encoding="utf-8") as fh:
        fh.write(f"$ {' '.join(cmd)}\n{datetime.now(timezone.utc).isoformat()}\n\n")
        fh.flush()
        proc = subprocess.run(cmd, cwd=ROOT, stdout=fh, stderr=subprocess.STDOUT)
    return proc.returncode == 0, time.perf_counter() - t0


def main() -> int:
    p = argparse.ArgumentParser(prog="rebuild")
    p.add_argument("--states", nargs="*", help="limit to these states")
    p.add_argument("--plan", action="store_true", help="print the plan and exit")
    p.add_argument("--reset", action="store_true", help="clear the ledger first")
    args = p.parse_args()

    if args.reset and LEDGER.exists():
        LEDGER.unlink()
        print("ledger cleared")

    print("Discovering available months...", flush=True)
    targets = discover_months()
    if args.states:
        targets = {s: m for s, m in targets.items() if s in set(args.states)}
    units = build_units(targets)

    led = load_ledger()
    done = set(led["done"])
    todo = [u for u in units if unit_key(*u) not in done]

    print(f"\n{len(units)} unit(s) total, {len(done)} already done, {len(todo)} to run:")
    for st, ms in todo:
        print(f"  {st:<3} {len(ms):>2} month(s)  {ms[0]}..{ms[-1]}")
    if args.plan:
        return 0
    if not todo:
        print("\nNothing to do — ledger says everything is complete.")
        return 0

    led["started"] = led.get("started") or datetime.now(timezone.utc).isoformat()
    save_ledger(led)

    failed: list[str] = []
    for i, (st, ms) in enumerate(todo, 1):
        key = unit_key(st, ms)
        print(f"\n[{i}/{len(todo)}] {key} ...", flush=True)
        for attempt in range(1, UNIT_ATTEMPTS + 1):
            ok, secs = run_unit(st, ms)
            if ok:
                led["done"].append(key)
                save_ledger(led)
                print(f"    done in {secs / 60:.1f} min"
                      + (f" (attempt {attempt})" if attempt > 1 else ""), flush=True)
                break
            if attempt < UNIT_ATTEMPTS:
                # Nearly every failure here is a dropped VPN / server timeout.
                # Nothing was written (a failed month is never treated as
                # replaced), so retrying is safe and usually works.
                pause = RETRY_PAUSES_S[attempt - 1]
                print(f"    attempt {attempt}/{UNIT_ATTEMPTS} failed after "
                      f"{secs / 60:.1f} min — retrying in {pause // 60} min",
                      flush=True)
                time.sleep(pause)
        else:
            failed.append(key)
            print(f"    FAILED after {UNIT_ATTEMPTS} attempts "
                  f"(see scripts/_rebuild_logs/) — continuing", flush=True)

    if failed:
        print(f"\n!! {len(failed)} unit(s) failed: {', '.join(failed)}")
        print("   Re-run this same command to retry only those.")
        return 1
    print("\nAll units complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
