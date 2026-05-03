"""Merge + group-by aggregation.

Per-(state, month) pipeline:
  1. Four SQL pulls (rate / car / driver / violation) for this one (state, yyyymm).
  2. preprocess_rate / _car / _driver (ported verbatim from the WSL pipeline).
  3. Merge on RateId, derive bin columns, groupby on 10 dimensions.
  4. Return one-month partial aggregate.

Top-N + Other bucketing for unmapped companies is applied LATER, on the
concatenated multi-month DataFrame — doing it per-month produces diverging
"Other (N=X)" labels that don't merge.
"""
from __future__ import annotations

import time

import pandas as pd

from . import sql
from .config import (
    CREDIT_AGE_BANDS, CREDIT_BASE_SCORE, CREDIT_BI_LIMITS_PTS, CREDIT_BIN_SIZE,
    CREDIT_CARRIER_PTS, CREDIT_CARRIER_STATE_LAPSE,
    CREDIT_CARRIER_STATE_NO_LAPSE, CREDIT_CARRIER_STATE_NO_PC,
    CREDIT_FORMULA_STATES, CREDIT_HOMEOWNER_PTS, CREDIT_VEHCOUNT_PTS,
    GROUP_COLS, PAYPLAN_LABELS, PREM_BIN_CAP, PREM_BIN_SIZE, PREM_COLS,
    YEAR_BINS, YEAR_LABELS,
)
from .preprocess import (
    preprocess_car,
    preprocess_driver,
    preprocess_rate,
)


def _cap_drivers(n) -> str:
    if pd.isna(n):
        return "1"
    return "4+" if int(n) >= 4 else str(int(n))


def _cap_vehicles(n) -> str:
    if pd.isna(n):
        return "1"
    n = int(n)
    if n >= 5:
        return "5+"
    return str(n)


def _compute_predicted_credit(df: pd.DataFrame) -> pd.Series:
    """Row-level PredictedCredit per the IL/AZ formula. Vectorized.

    Inputs (must already be on df):
        PriorInsurance      int 0/1
        PriorMonthsCovg     int (months prior coverage)
        PriorDaysLapse      int
        Year                int (newest car year on policy)
        RatedDate           datetime
        NamedInsuredAge     int
        ResidencyStatus     str ('O' = own = homeowner)
        LiabLimits          str ('25/50' | '50/100' | '100/300')
        NumVehicles         str ('1'..'5+')

    Returns a Series of integer scores (NaN for rows missing inputs).
    """
    score = pd.Series(CREDIT_BASE_SCORE, index=df.index, dtype="float64")

    # ── Prior Carrier / Lapse state ──────────────────────────────────────────
    pi = df["PriorInsurance"].fillna(0).astype(int)
    lapse = df["PriorDaysLapse"].fillna(0)
    carrier_state = pd.Series(CREDIT_CARRIER_STATE_NO_PC, index=df.index)
    carrier_state.loc[(pi == 1) & (lapse == 0)] = CREDIT_CARRIER_STATE_NO_LAPSE
    carrier_state.loc[(pi == 1) & (lapse > 0)]  = CREDIT_CARRIER_STATE_LAPSE

    score += carrier_state.map(CREDIT_CARRIER_PTS).astype("float64")

    # ── Prior Duration pts ───────────────────────────────────────────────────
    # NO PC → treat as 0 months (falls in 0-5 bucket → -50). Else use PriorMonthsCovg.
    months = df["PriorMonthsCovg"].where(pi == 1, 0).fillna(0).clip(lower=0)
    dur_pts = pd.Series(0, index=df.index, dtype="float64")
    dur_pts.loc[months <= 5]                   = -50
    dur_pts.loc[(months >= 6) & (months <= 12)] = 0
    dur_pts.loc[months > 12]                    = 70
    score += dur_pts

    # ── Vehicle Min Age pts ──────────────────────────────────────────────────
    rated_year = df["RatedDate"].dt.year
    min_age = (rated_year - df["Year"]).clip(lower=0)
    vma_pts = pd.Series(0, index=df.index, dtype="float64")
    vma_pts.loc[min_age < 3]                  = 110
    vma_pts.loc[(min_age >= 3) & (min_age <= 9)] = 35
    vma_pts.loc[min_age >= 10]                = -40
    score += vma_pts

    # ── Named Insured Age × carrier-state matrix ─────────────────────────────
    age = df["NamedInsuredAge"].fillna(-1)
    age_pts = pd.Series(0, index=df.index, dtype="float64")
    for lo, hi, mapping in CREDIT_AGE_BANDS:
        in_band = (age >= lo) & (age <= hi)
        for cs_name, pts in mapping.items():
            age_pts.loc[in_band & (carrier_state == cs_name)] = pts
    score += age_pts

    # ── BI Limits pts ────────────────────────────────────────────────────────
    score += df["LiabLimits"].map(CREDIT_BI_LIMITS_PTS).fillna(0).astype("float64")

    # ── Vehicle Count pts ────────────────────────────────────────────────────
    score += df["NumVehicles"].map(CREDIT_VEHCOUNT_PTS).fillna(0).astype("float64")

    # ── Homeowner pts ────────────────────────────────────────────────────────
    homeowner_pts = (df["ResidencyStatus"] == "O").astype("float64") * CREDIT_HOMEOWNER_PTS
    score += homeowner_pts

    # Rows missing critical inputs → NaN (so they don't pollute CreditBin).
    valid = (
        df["NamedInsuredAge"].notna()
        & df["LiabLimits"].notna()
        & df["NumVehicles"].notna()
        & df["Year"].notna()
        & df["RatedDate"].notna()
    )
    return score.where(valid).round().astype("Int64")


def _credit_bin(score: pd.Series) -> pd.Series:
    """Bucket PredictedCredit into 50-pt floors. NaN preserved."""
    floored = (score // CREDIT_BIN_SIZE) * CREDIT_BIN_SIZE
    return floored.astype("Int64")


def _aggregate_one_state(
    state: str,
    yyyymm: str,
    rate: pd.DataFrame,
    car: pd.DataFrame,
    drv: pd.DataFrame,
    viol: pd.DataFrame,
) -> pd.DataFrame:
    """Preprocess + merge + groupby for one state's slice of one month."""
    if rate.empty:
        return pd.DataFrame()

    rate = preprocess_rate(rate, int(yyyymm), state)
    if rate.empty:
        return pd.DataFrame()

    if car.empty:
        return pd.DataFrame()
    car = preprocess_car(car)

    if not drv.empty:
        if viol.empty:
            viol = pd.DataFrame(columns=["RateDriverLinkId", "AtFault"])
        drv = preprocess_driver(drv, viol)
    else:
        drv = pd.DataFrame(columns=["RateId", "PriorInsurance", "AtFault", "NumDrivers"])

    df = (
        rate.merge(car, on="RateId", how="inner")
            .merge(drv, on="RateId", how="left")
    )
    if df.empty:
        return pd.DataFrame()

    df["PayPlan"] = df["PayPlan"].map(PAYPLAN_LABELS).fillna(df["PayPlan"])
    df["YearBin"] = pd.cut(df["Year"], bins=YEAR_BINS, labels=YEAR_LABELS).astype(str)
    df["NumDrivers"] = df["NumDrivers"].apply(_cap_drivers)
    df["NumVehicles"] = df["NumVehicles"].apply(_cap_vehicles)
    df["PriorInsurance"] = df["PriorInsurance"].fillna(0).astype(int)
    df["NonOwner"] = df["NonOwner"].fillna(0).astype(int)

    df["PremBin"] = (
        df["TotalPremium"].clip(upper=PREM_BIN_CAP) // PREM_BIN_SIZE * PREM_BIN_SIZE
    ).astype(int)
    df["_bridge_prem"] = (
        df["TotalPremium"].where(df["PurchasedFinal"] == 1, 0).fillna(0)
    )

    # PredictedCredit + CreditBin (only for states with a credit formula).
    if state in CREDIT_FORMULA_STATES:
        df["PredictedCredit"] = _compute_predicted_credit(df)
        df["CreditBin"] = _credit_bin(df["PredictedCredit"])
    else:
        df["CreditBin"] = pd.Series([pd.NA] * len(df), dtype="Int64")

    coverage_aggs = {f"Sum{col}": (col, "sum") for col in PREM_COLS}

    agg = (
        df.groupby(GROUP_COLS, dropna=False)
          .agg(
              Quotes=("TotalPremium", "count"),
              SumPremium=("TotalPremium", "sum"),
              BridgingCount=("PurchasedFinal", "sum"),
              SumBridgingPremium=("_bridge_prem", "sum"),
              **coverage_aggs,
          )
          .reset_index()
    )
    agg["YYYYMM"] = int(yyyymm)
    return agg


def fetch_and_aggregate(
    state: str, yyyymm: str, *, verbose: bool = True,
) -> pd.DataFrame:
    """Pull 4 tables from SQL for one (state, month) and return the partial aggregate."""
    if verbose:
        print(f"  {state} {yyyymm}:", flush=True)

    t0 = time.perf_counter()
    rate = sql.fetch_rate(state, yyyymm)
    if verbose:
        print(f"    fact_Rate           {len(rate):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)
    if rate.empty:
        return pd.DataFrame()

    t0 = time.perf_counter()
    car = sql.fetch_car(state, yyyymm)
    if verbose:
        print(f"    fact_Rate_Car       {len(car):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    drv = sql.fetch_driver(state, yyyymm)
    if verbose:
        print(f"    fact_Rate_Driver    {len(drv):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    viol = sql.fetch_violation(state, yyyymm)
    if verbose:
        print(f"    fact_Rate_Violation {len(viol):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    agg = _aggregate_one_state(state, yyyymm, rate, car, drv, viol)
    if verbose:
        print(f"    -> aggregated to {len(agg):,} groupby rows   {time.perf_counter()-t0:6.1f}s", flush=True)
    return agg
