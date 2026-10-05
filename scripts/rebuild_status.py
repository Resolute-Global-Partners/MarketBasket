"""Where has the rebuild got to, and what is still outstanding?

Answers "every month for every state" honestly by separating four cases:
  DONE      - rebuilt in this run (unit recorded in the ledger)
  RUNNING   - the unit currently being pulled
  QUEUED    - in the plan, not started
  WITHHELD  - deliberately not published (EXCLUDED_MONTHS_BY_STATE)
  ABSENT    - not in MarketUnified at all (upstream ETL never delivered it)

Reads only local state (ledger + parquets + config); the SQL month list is
cached from the driver log so this costs nothing to run repeatedly.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "docs" / "data"
LEDGER = ROOT / "scripts" / "_rebuild_ledger.json"
DRIVER_LOG = ROOT / "scripts" / "_rebuild_driver.log"

import sys
sys.path.insert(0, str(ROOT / "src"))
from marketbasket.config import ACTIVE_STATES, excluded_months  # noqa: E402

# Months that exist in MarketUnified, from the authoritative 2026-09-28 scan.
IN_DB = {
    "AZ": [202508, 202509, 202510, 202511, 202512, 202601, 202602, 202603,
           202604, 202605, 202606, 202607],
    "IL": [202405, 202406, 202407, 202408, 202409, 202410, 202411, 202412,
           202501, 202502, 202503, 202504, 202505, 202506, 202507, 202508,
           202509, 202510, 202511, 202512, 202601, 202602, 202603, 202604,
           202605, 202606, 202607],
    "IN": [202509, 202510, 202511, 202512, 202601, 202602, 202603, 202604,
           202605, 202606, 202607],
    "TN": [202509, 202510, 202511, 202512, 202601, 202602, 202603, 202604,
           202605, 202606, 202607],
    "TX": [202509, 202510, 202511, 202512, 202601, 202602, 202603, 202604,
           202605, 202606, 202607],
}
# Calendar months we would expect by now but which never loaded upstream.
ABSENT_UPSTREAM = [202608, 202609]


def published_months(state: str) -> set[int]:
    d = DATA / state
    if d.is_dir():
        parts = [pd.read_parquet(p, columns=["YYYYMM"]) for p in sorted(d.glob("*.parquet"))]
        if parts:
            return {int(m) for m in pd.concat(parts)["YYYYMM"].unique()}
    f = DATA / f"{state}.parquet"
    if f.exists():
        return {int(m) for m in pd.read_parquet(f, columns=["YYYYMM"])["YYYYMM"].unique()}
    return set()


def main() -> int:
    led = json.loads(LEDGER.read_text()) if LEDGER.exists() else {"done": []}
    done_units = set(led.get("done", []))

    running = None
    if DRIVER_LOG.exists():
        starts = re.findall(r"^\[(\d+)/(\d+)\] (\S+) \.\.\.",
                            DRIVER_LOG.read_text(errors="replace"), re.M)
        if starts:
            idx, total, key = starts[-1]
            if key not in done_units:
                running = (int(idx), int(total), key)

    def unit_state_months(key: str) -> tuple[str, int, int]:
        st, rng = key.split(":")
        lo, hi = rng.split("-")
        return st, int(lo), int(hi)

    done_by_state: dict[str, set[int]] = {s: set() for s in ACTIVE_STATES}
    for key in done_units:
        st, lo, hi = unit_state_months(key)
        for m in IN_DB.get(st, []):
            if lo <= m <= hi:
                done_by_state[st].add(m)

    running_months: set[tuple[str, int]] = set()
    if running:
        st, lo, hi = unit_state_months(running[2])
        running_months = {(st, m) for m in IN_DB.get(st, []) if lo <= m <= hi}

    print(f"Units complete: {len(done_units)}"
          + (f" / {running[1]}" if running else ""))
    if running:
        print(f"Currently running: unit {running[0]} -> {running[2]}")
    print()

    tot = {k: 0 for k in ("DONE", "RUNNING", "QUEUED", "WITHHELD", "ABSENT")}
    for st in sorted(ACTIVE_STATES):
        pub = published_months(st)
        skip = excluded_months(st)
        rows = []
        for m in IN_DB.get(st, []):
            if m in skip:
                tag = "WITHHELD"
            elif m in done_by_state[st]:
                tag = "DONE"
            elif (st, m) in running_months:
                tag = "RUNNING"
            else:
                tag = "QUEUED"
            tot[tag] += 1
            rows.append((m, tag, m in pub))
        for m in ABSENT_UPSTREAM:
            tot["ABSENT"] += 1
            rows.append((m, "ABSENT", False))

        counts: dict[str, int] = {}
        for _, tag, _ in rows:
            counts[tag] = counts.get(tag, 0) + 1
        summary = "  ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"=== {st} ===  {summary}")
        line = []
        for m, tag, inpub in rows:
            mark = {"DONE": "+", "RUNNING": "~", "QUEUED": ".",
                    "WITHHELD": "x", "ABSENT": "-"}[tag]
            line.append(f"{m}{mark}")
        print("   " + " ".join(line))
    print("\n   + rebuilt   ~ in progress   . queued   x withheld   - absent upstream")

    print(f"\nTOTAL  rebuilt {tot['DONE']}, running {tot['RUNNING']}, "
          f"queued {tot['QUEUED']}, withheld {tot['WITHHELD']}, "
          f"absent upstream {tot['ABSENT']}")
    print("\nWITHHELD months are one config edit away (EXCLUDED_MONTHS_BY_STATE).")
    print("ABSENT months need the data platform to re-run the load; no local fix.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
