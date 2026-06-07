"""SQL queries against MarketUnified — per-state, per-month.

fact_Rate has ZERO indexes, so every WHERE clause is a full-table scan of
198M rows on the server (~100 GB on disk). That scan is unavoidable. What
kills throughput is the VPN transfer of result rows — so the winning pattern
is to filter hard (one state AND one month), keeping each transfer small
(~30-60 MB). This matches the historical WSL dump workflow.

Validation: state is 2 uppercase letters, yyyymm is 6 digits. Safe for string
interpolation without parameter binding.
"""
from __future__ import annotations

import re

import pandas as pd
from dataloader import load

DB = "MarketUnified"

_STATE_RE = re.compile(r"^[A-Z]{2}$")
_YYYYMM_RE = re.compile(r"^\d{6}$")

# Columns actually used downstream — narrower SELECT = smaller VPN transfer.
RATE_COLS = [
    "PolicyLinkID", "RateId", "CompanyId", "RateIteration",
    "RatedDate", "TotalPremium", "DownPayment", "PercentDown",
    "NumOfPayments", "Purchased", "NonOwner", "AssumedCredit", "Term",
    "ThirdPartyId",  # ITC's quote-scenario ID; used for audit + Diamond join
]

CAR_COLS = [
    "RateLinkID", "RateVehicleId",
    "LiabLimits1", "LiabLimits2", "Year", "County",
    "LiabBIPremium", "LiabPDPremium", "CompPremium", "CollPremium",
    "MedPayPremium", "UIMBIPremium", "UIMPDPremium",
    "UninsBIPremium", "UninsPDPremium",
]

DRV_COLS = [
    "RateLinkID", "RateDriverId", "PriorInsurance",
    "Age", "Relation", "ResidencyStatus",
    "PriorMonthsCovg", "PriorDaysLapse",
]
VIOL_COLS = ["RateDriverLinkId", "AtFault"]


def _validate(state: str, yyyymm: str) -> None:
    if not _STATE_RE.match(state):
        raise ValueError(f"state must be 2 uppercase letters, got {state!r}")
    if not _YYYYMM_RE.match(yyyymm):
        raise ValueError(f"yyyymm must be 6 digits, got {yyyymm!r}")


def _select(cols: list[str]) -> str:
    return ", ".join(f"[{c}]" for c in cols)


# ─── Discovery (TABLESAMPLE — cheap, pages-random) ────────────────────────────

def discover_state_months(sample_percent: float = 1.0) -> pd.DataFrame:
    """Sample fact_Rate to enumerate (State_Name, Year_Month) combos that exist.

    TABLESAMPLE reads random pages instead of the whole table. 1% typically
    surfaces every combo with non-trivial volume in ~15 seconds.
    """
    if not (0 < sample_percent <= 100):
        raise ValueError("sample_percent must be in (0, 100]")
    return load(
        f"""
        SELECT  DISTINCT
                RTRIM(State_Name) AS State_Name,
                RTRIM(Year_Month) AS Year_Month
        FROM    dbo.fact_Rate TABLESAMPLE ({sample_percent} PERCENT)
        WHERE   State_Name IS NOT NULL AND Year_Month IS NOT NULL
        ORDER BY State_Name, Year_Month
        """,
        DB,
    )


# ─── Per-(state, month) pulls ─────────────────────────────────────────────────

def fetch_rate(state: str, yyyymm: str) -> pd.DataFrame:
    """fact_Rate rows for one state + one month."""
    _validate(state, yyyymm)
    return load(
        f"SELECT {_select(RATE_COLS)} FROM dbo.fact_Rate "
        f"WHERE State_Name = '{state}' AND Year_Month = '{yyyymm}'",
        DB,
    )


def fetch_car(state: str, yyyymm: str) -> pd.DataFrame:
    _validate(state, yyyymm)
    return load(
        f"SELECT {_select(CAR_COLS)} FROM dbo.fact_Rate_Car "
        f"WHERE State_Name = '{state}' AND Year_Month = '{yyyymm}'",
        DB,
    )


def fetch_driver(state: str, yyyymm: str) -> pd.DataFrame:
    _validate(state, yyyymm)
    return load(
        f"SELECT {_select(DRV_COLS)} FROM dbo.fact_Rate_Driver "
        f"WHERE State_Name = '{state}' AND Year_Month = '{yyyymm}'",
        DB,
    )


def fetch_violation(state: str, yyyymm: str) -> pd.DataFrame:
    _validate(state, yyyymm)
    return load(
        f"SELECT {_select(VIOL_COLS)} FROM dbo.fact_Rate_Violation "
        f"WHERE State_Name = '{state}' AND Year_Month = '{yyyymm}'",
        DB,
    )


# ─── Per-state pulls (all months in one query) ───────────────────────────────
#
# fact_Rate has zero indexes, so each WHERE-filtered query scans the full 198M-row
# table regardless of how narrow the filter is. Pulling 23 months in one query
# pays the scan once instead of 23 times — ~10-15x faster end-to-end.
#
# Year_Month comes back on the result so callers can split by month locally.

def _state_in_clause(state: str, months: list[str]) -> str:
    if not _STATE_RE.match(state):
        raise ValueError(f"state must be 2 uppercase letters, got {state!r}")
    for m in months:
        if not _YYYYMM_RE.match(m):
            raise ValueError(f"yyyymm must be 6 digits, got {m!r}")
    quoted = ", ".join(f"'{m}'" for m in months)
    return f"State_Name = '{state}' AND Year_Month IN ({quoted})"


def fetch_rate_state(state: str, months: list[str]) -> pd.DataFrame:
    """fact_Rate rows for one state across many months. Returns a single frame
    with a Year_Month column so callers can split it locally."""
    return load(
        f"SELECT {_select(RATE_COLS)}, Year_Month FROM dbo.fact_Rate "
        f"WHERE {_state_in_clause(state, months)}",
        DB,
    )


def fetch_car_state(state: str, months: list[str]) -> pd.DataFrame:
    return load(
        f"SELECT {_select(CAR_COLS)}, Year_Month FROM dbo.fact_Rate_Car "
        f"WHERE {_state_in_clause(state, months)}",
        DB,
    )


def fetch_driver_state(state: str, months: list[str]) -> pd.DataFrame:
    return load(
        f"SELECT {_select(DRV_COLS)}, Year_Month FROM dbo.fact_Rate_Driver "
        f"WHERE {_state_in_clause(state, months)}",
        DB,
    )


def fetch_violation_state(state: str, months: list[str]) -> pd.DataFrame:
    return load(
        f"SELECT {_select(VIOL_COLS)}, Year_Month FROM dbo.fact_Rate_Violation "
        f"WHERE {_state_in_clause(state, months)}",
        DB,
    )
